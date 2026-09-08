// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The log store: a background actor batches appended entries into objects and
//! reads are served from the object catalog.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use common_wal::config::object_store::ObjectStoreWalConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::{ObjectStoreProvider, Provider};
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;
#[cfg(any(test, feature = "testing"))]
use tokio::sync::watch;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

use crate::error::{
    CorruptedWalObjectSnafu, Error, InvalidProviderSnafu, InvalidWalEntrySnafu,
    InvalidWalObjectSnafu, InvalidWalObjectStoreSnafu, MismatchedWalPrefixSnafu,
    ObjectStoreWalSnafu, ObjectStoreWalStoppedSnafu, Result, WalObjectSequenceExhaustedSnafu,
};
use crate::object_store_wal::batch::OpenBatch;
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{
    EncodedObject, FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN, Record,
    TRAILER_LEN, decode_footer, decode_header, decode_segment, decode_trailer, encode_object,
    footer_range, verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
/// Number of objects whose footers recovery fetches at a time.
const RECOVERY_CONCURRENCY: usize = 8;
/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare.
const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;

/// A [`LogStore`] that persists the entries of many regions as immutable
/// objects under one prefix.
///
/// Appends are admitted into an open batch and acknowledged once the object
/// holding them is durable. A background actor seals the batch when it reaches
/// the size limit or the flush interval elapses, creates the object under the
/// next sequence and indexes it in the catalog. Reads fetch and decode only the
/// segment of the requested region from every object the catalog lists for it.
pub struct ObjectStoreLogStore {
    prefix: String,
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    /// Largest obsolete entry id per region. Objects are not deleted yet.
    obsolete_entry_ids: Mutex<HashMap<RegionId, EntryId>>,
    /// Set once the store hit an error it cannot recover from, such as a
    /// conflicting object; every operation fails with it afterwards.
    terminal_error: TerminalError,
    /// Set by [`stop`](LogStore::stop) before the actor is told to exit.
    stopped: Arc<AtomicBool>,
    command_tx: mpsc::Sender<Command>,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Receiver<usize>,
}

type TerminalError = Arc<Mutex<Option<Arc<Error>>>>;

impl fmt::Debug for ObjectStoreLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreLogStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreLogStore {
    /// Builds the store over the objects under the prefix of `config`,
    /// recovering the catalog from the objects that already exist. Recovery
    /// fails on the first corrupted or conflicting object.
    pub async fn try_new(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
    ) -> Result<Arc<Self>> {
        let io = ObjectStoreIo::new(object_store, &config.prefix)?;
        Self::open(Arc::new(io), config).await
    }

    async fn open(io: Arc<dyn WalObjectIo>, config: &ObjectStoreWalConfig) -> Result<Arc<Self>> {
        ensure!(
            config.flush_interval >= MIN_FLUSH_INTERVAL,
            InvalidWalObjectStoreSnafu {
                reason: format!(
                    "flush interval {:?} is shorter than {MIN_FLUSH_INTERVAL:?}",
                    config.flush_interval
                ),
            }
        );
        let max_batch_bytes = usize::try_from(config.max_batch_bytes.as_bytes())
            .ok()
            .filter(|bytes| *bytes > 0)
            .with_context(|| InvalidWalObjectStoreSnafu {
                reason: format!(
                    "max batch bytes {} is zero or too large",
                    config.max_batch_bytes
                ),
            })?;

        let (catalog, next_object_seq, durable_entry_ids) = recover(io.as_ref()).await?;
        let catalog = Arc::new(RwLock::new(catalog));
        let terminal_error = TerminalError::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        #[cfg(any(test, feature = "testing"))]
        let (admitted_appends_tx, admitted_appends_rx) = watch::channel(0);

        let actor = Actor {
            io: io.clone(),
            catalog: catalog.clone(),
            terminal_error: terminal_error.clone(),
            stopped: stopped.clone(),
            command_rx,
            open_batch: OpenBatch::new(max_batch_bytes, durable_entry_ids),
            pending: Vec::new(),
            next_object_seq,
            writer_instance: uuid::Uuid::new_v4().into_bytes(),
            flush_interval: config.flush_interval,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_tx,
        };
        common_runtime::spawn_global(actor.run());

        Ok(Arc::new(Self {
            prefix: config.prefix.clone(),
            io,
            catalog,
            obsolete_entry_ids: Mutex::new(HashMap::new()),
            terminal_error,
            stopped,
            command_tx,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_rx,
        }))
    }

    /// Returns the region of `provider`, which must select this store's prefix.
    fn region_of(&self, provider: &Provider) -> Result<RegionId> {
        let provider =
            provider
                .as_object_store_provider()
                .with_context(|| InvalidProviderSnafu {
                    expected: ObjectStoreProvider::type_name(),
                    actual: provider.type_name(),
                })?;
        ensure!(
            provider.prefix == self.prefix,
            MismatchedWalPrefixSnafu {
                expected: self.prefix.clone(),
                actual: provider.prefix.clone(),
            }
        );
        Ok(provider.region_id)
    }

    fn check_terminal(&self) -> Result<()> {
        match terminal(&self.terminal_error) {
            Some(error) => Err(shared(&error)),
            None => Ok(()),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
impl ObjectStoreLogStore {
    /// Waits until the actor has admitted at least `expected` append calls
    /// since the store was built.
    pub async fn wait_for_admitted_appends(&self, expected: usize) -> Result<()> {
        self.admitted_appends
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Seals and persists the open batch regardless of its size and age.
    pub async fn seal_open_batch(&self) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            ObjectStoreWalStoppedSnafu
        );
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Seal {
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }
}

#[async_trait::async_trait]
impl LogStore for ObjectStoreLogStore {
    type Error = Error;

    async fn stop(&self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
        let (response_tx, response_rx) = oneshot::channel();
        let sent = self
            .command_tx
            .send(Command::Stop {
                response: response_tx,
            })
            .await;
        // A closed channel means the actor already exited.
        if sent.is_ok() {
            let _ = response_rx.await;
        }
        Ok(())
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            ObjectStoreWalStoppedSnafu
        );
        self.check_terminal()?;
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }
        for entry in &entries {
            let region_id = self.region_of(entry.provider())?;
            ensure!(
                region_id == entry.region_id(),
                InvalidWalEntrySnafu {
                    region_id: entry.region_id(),
                    reason: format!("provider belongs to region {region_id}"),
                }
            );
            ensure!(
                entry.is_complete(),
                InvalidWalEntrySnafu {
                    region_id,
                    reason: "multipart entry is incomplete",
                }
            );
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Append {
                entries,
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }

    /// Returns the entries of the provider's region with ids from `entry_id`
    /// on, skipping ids the region has obsoleted. Objects are located through
    /// the catalog, so `index` is not needed.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
        _index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Error>> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        let obsolete = self
            .obsolete_entry_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&region_id)
            .copied();
        let start_entry_id =
            entry_id.max(obsolete.map_or(0, |obsolete| obsolete.saturating_add(1)));
        let objects = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            match catalog.region_max_entry_id(region_id) {
                // A watermark at the maximum id hides everything.
                Some(max_entry_id)
                    if start_entry_id <= max_entry_id && obsolete != Some(EntryId::MAX) =>
                {
                    catalog
                        .objects_for_entry_range(region_id, start_entry_id, max_entry_id)?
                        .into_iter()
                        .map(|(object_seq, entry)| (object_seq, entry.clone()))
                        .collect::<Vec<_>>()
                }
                _ => Vec::new(),
            }
        };

        let io = self.io.clone();
        let provider = provider.clone();
        Ok(Box::pin(try_stream! {
            for (object_seq, footer_entry) in objects {
                let bytes = io
                    .get_range(object_seq, footer_entry.segment_offset, footer_entry.segment_len)
                    .await?;
                let records = decode_segment(&bytes, &footer_entry)
                    .with_context(|_| InvalidWalObjectSnafu {
                        path: io.object_path(object_seq),
                    })?;
                let entries = records
                    .into_iter()
                    .filter(|record| record.entry_id >= start_entry_id)
                    .map(|record| {
                        Entry::Naive(NaiveEntry {
                            provider: provider.clone(),
                            region_id,
                            entry_id: record.entry_id,
                            data: record.payload.to_vec(),
                        })
                    })
                    .collect::<Vec<_>>();
                if !entries.is_empty() {
                    yield entries;
                }
            }
        }))
    }

    async fn create_namespace(&self, ns: &Provider) -> Result<()> {
        self.check_terminal()?;
        self.region_of(ns).map(|_| ())
    }

    async fn delete_namespace(&self, ns: &Provider) -> Result<()> {
        self.check_terminal()?;
        self.region_of(ns).map(|_| ())
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        self.check_terminal()?;
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        let regions = catalog
            .objects_in_order()
            .flat_map(|(_, footer)| footer.iter().map(|entry| entry.region_id))
            .collect::<BTreeSet<_>>();
        Ok(regions
            .into_iter()
            .map(|region_id| Provider::object_store_provider(region_id, self.prefix.clone()))
            .collect())
    }

    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        self.check_terminal()?;
        let provider_region = self.region_of(provider)?;
        ensure!(
            provider_region == region_id,
            InvalidWalEntrySnafu {
                region_id,
                reason: format!("provider belongs to region {provider_region}"),
            }
        );
        self.obsolete_entry_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(region_id)
            .and_modify(|current| *current = (*current).max(entry_id))
            .or_insert(entry_id);
        Ok(())
    }

    async fn obsolete_all(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.obsolete(provider, region_id, EntryId::MAX).await
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        self.check_terminal()?;
        let provider_region = self.region_of(provider)?;
        ensure!(
            provider_region == region_id,
            InvalidWalEntrySnafu {
                region_id,
                reason: format!("provider belongs to region {provider_region}"),
            }
        );
        Ok(Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data,
        }))
    }

    /// Returns the largest durable entry id of the provider's region, or zero
    /// for a region without entries.
    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        Ok(catalog.region_max_entry_id(region_id).unwrap_or(0))
    }
}

enum Command {
    Append {
        entries: Vec<Entry>,
        response: oneshot::Sender<Result<AppendBatchResponse>>,
    },
    Stop {
        response: oneshot::Sender<()>,
    },
    #[cfg(any(test, feature = "testing"))]
    Seal {
        response: oneshot::Sender<Result<()>>,
    },
}

/// An append waiting for the object that holds its entries.
struct PendingAppend {
    last_entry_ids: HashMap<RegionId, EntryId>,
    response: oneshot::Sender<Result<AppendBatchResponse>>,
}

struct Actor {
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_rx: mpsc::Receiver<Command>,
    open_batch: OpenBatch,
    pending: Vec<PendingAppend>,
    next_object_seq: u64,
    writer_instance: [u8; 16],
    flush_interval: Duration,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Sender<usize>,
}

impl Actor {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes immediately.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.is_stopped() {
                        self.discard_open_batch();
                    } else {
                        self.flush_open_batch().await;
                    }
                }
                command = self.command_rx.recv() => match command {
                    Some(Command::Append { entries, response }) => {
                        self.handle_append(entries, response).await;
                    }
                    Some(Command::Stop { response }) => {
                        self.discard_open_batch();
                        let _ = response.send(());
                        return;
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Seal { response }) => {
                        let result = if self.is_stopped() {
                            self.discard_open_batch();
                            Err(ObjectStoreWalStoppedSnafu.build())
                        } else {
                            self.flush_open_batch().await;
                            terminal(&self.terminal_error)
                                .map_or(Ok(()), |error| Err(shared(&error)))
                        };
                        let _ = response.send(result);
                    }
                    // Every sender is gone: the store was dropped without `stop`.
                    None => return,
                },
            }
        }
    }

    async fn handle_append(
        &mut self,
        entries: Vec<Entry>,
        response: oneshot::Sender<Result<AppendBatchResponse>>,
    ) {
        // The append was queued before `stop` set the flag; nothing that is
        // not durable yet gets admitted once it is set.
        if self.is_stopped() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        let last_entry_ids = match self.open_batch.admit(entries) {
            Ok(last_entry_ids) => last_entry_ids,
            Err(error) => {
                let error = self.fail_permanently(error);
                let _ = response.send(Err(shared(&error)));
                return;
            }
        };
        self.pending.push(PendingAppend {
            last_entry_ids,
            response,
        });
        #[cfg(any(test, feature = "testing"))]
        self.admitted_appends.send_modify(|count| *count += 1);
        if self.open_batch.should_seal() {
            self.flush_open_batch().await;
        }
    }

    /// Persists the open batch as the object `next_object_seq`. The sequence
    /// advances only after the object is durable and indexed, so a failed
    /// attempt leaves it to the next batch.
    async fn flush_open_batch(&mut self) {
        if self.open_batch.is_empty() {
            return;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            self.fail_pending(|| shared(&error));
            return;
        }

        let object_seq = self.next_object_seq;
        let entries = self.open_batch.seal();
        let encoded = match encode_batch(object_seq, self.writer_instance, entries) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.fail_permanently(error);
                return;
            }
        };
        match self.io.put_if_absent(object_seq, encoded.bytes).await {
            Ok(_) => {}
            // The object store did not confirm the object. Its sequence stays
            // free and the entry ids are handed out again, so a retry of the
            // same entries writes the same object. Waiters of a store that was
            // stopped meanwhile learn that instead of the I/O error, like every
            // other entry that never became durable.
            Err(error @ Error::WalObjectStore { .. }) => {
                let durable_entry_ids = {
                    let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
                    durable_entry_ids(&catalog)
                };
                self.open_batch.reset(durable_entry_ids);
                if self.is_stopped() {
                    self.fail_pending(|| ObjectStoreWalStoppedSnafu.build());
                } else {
                    let error = Arc::new(error);
                    self.fail_pending(|| shared(&error));
                }
                return;
            }
            Err(error) => {
                self.fail_permanently(error);
                return;
            }
        }

        let indexed = self
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert_object(object_seq, encoded.footer);
        if let Err(error) = indexed {
            self.fail_permanently(error);
            return;
        }
        for pending in self.pending.drain(..) {
            let _ = pending.response.send(Ok(AppendBatchResponse {
                last_entry_ids: pending.last_entry_ids,
            }));
        }
        match object_seq.checked_add(1) {
            Some(next_object_seq) => self.next_object_seq = next_object_seq,
            None => {
                set_terminal(
                    &self.terminal_error,
                    WalObjectSequenceExhaustedSnafu {
                        last_object_seq: object_seq,
                    }
                    .build(),
                );
            }
        }
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Drops the entries of the open batch, which are not durable, and fails
    /// their waiters with the stopped error.
    fn discard_open_batch(&mut self) {
        let durable_entry_ids = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            durable_entry_ids(&catalog)
        };
        self.open_batch.reset(durable_entry_ids);
        self.fail_pending(|| ObjectStoreWalStoppedSnafu.build());
    }

    fn fail_pending(&mut self, error: impl Fn() -> Error) {
        for pending in self.pending.drain(..) {
            let _ = pending.response.send(Err(error()));
        }
    }

    /// Records `error` as terminal, drops the open batch and fails every
    /// waiter with the recorded error, unless the store was stopped meanwhile:
    /// their entries never became durable, so they learn that the store is
    /// stopped like every other such waiter. Returns the recorded error.
    fn fail_permanently(&mut self, error: Error) -> Arc<Error> {
        let error = set_terminal(&self.terminal_error, error);
        let durable_entry_ids = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            durable_entry_ids(&catalog)
        };
        self.open_batch.reset(durable_entry_ids);
        if self.is_stopped() {
            self.fail_pending(|| ObjectStoreWalStoppedSnafu.build());
        } else {
            self.fail_pending(|| shared(&error));
        }
        error
    }
}

fn terminal(terminal_error: &TerminalError) -> Option<Arc<Error>> {
    terminal_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// Records `error` as the terminal error unless one is already recorded, and
/// returns the recorded one.
fn set_terminal(terminal_error: &TerminalError, error: Error) -> Arc<Error> {
    terminal_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_or_insert_with(|| Arc::new(error))
        .clone()
}

/// Wraps an error that several callers receive.
fn shared(error: &Arc<Error>) -> Error {
    ObjectStoreWalSnafu.into_error(error.clone())
}

fn encode_batch(
    object_seq: u64,
    writer_instance: [u8; 16],
    entries: Vec<Entry>,
) -> Result<EncodedObject> {
    let records = entries
        .into_iter()
        .map(|entry| Record {
            region_id: entry.region_id(),
            entry_id: entry.entry_id(),
            payload: Bytes::from(entry.into_bytes()),
        })
        .collect::<Vec<_>>();
    encode_object(
        Header {
            object_seq,
            writer_instance,
        },
        &records,
    )
}

/// Rebuilds the catalog from the objects under the prefix and returns it with
/// the next object sequence and the largest durable entry id per region.
///
/// Only the header, trailer and footer of every object are read and verified,
/// so recovery costs a few small reads per object however large the objects
/// are. Segments are not read; a segment checksum is verified by the read that
/// decodes it. Footers are fetched for up to [`RECOVERY_CONCURRENCY`] objects
/// at a time and indexed in sequence order, so the catalog checks the entry
/// ranges of every object against its predecessors like a sequential replay.
async fn recover(io: &dyn WalObjectIo) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
    let objects = io.list().await?;
    let mut catalog = ObjectCatalog::default();
    for (object, footer) in fetch_footers(io, objects, RECOVERY_CONCURRENCY).await? {
        catalog
            .insert_object(object.object_seq, footer)
            .with_context(|_| InvalidWalObjectSnafu { path: object.path })?;
    }
    finish_recovery(catalog)
}

fn finish_recovery(
    catalog: ObjectCatalog,
) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
    let next_object_seq = catalog.next_object_seq()?;
    let durable_entry_ids = durable_entry_ids(&catalog);
    Ok((catalog, next_object_seq, durable_entry_ids))
}

/// Fetches and verifies the footers of `objects`, up to `concurrency` objects
/// at a time, and returns them ordered by object sequence whatever the order
/// the fetches complete in. The first failure abandons the remaining fetches.
async fn fetch_footers(
    io: &dyn WalObjectIo,
    objects: Vec<ListedObject>,
    concurrency: usize,
) -> Result<Vec<(ListedObject, Vec<FooterEntry>)>> {
    let mut footers = futures::stream::iter(objects)
        .map(|object| async move {
            let footer = fetch_footer(io, &object).await?;
            Ok((object, footer))
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    footers.sort_unstable_by_key(|(object, _)| object.object_seq);
    Ok(footers)
}

/// Reads the header, trailer and footer of `object` and verifies them: the
/// header must carry the sequence of the key, the trailer must be well formed,
/// the footer must match the checksum the trailer holds and its segments must
/// tile the object body.
///
/// A short object is read whole. Otherwise the header and a window at the end
/// of the object are read concurrently, and the footer is read separately only
/// when it starts before the window.
async fn fetch_footer(io: &dyn WalObjectIo, object: &ListedObject) -> Result<Vec<FooterEntry>> {
    let ListedObject {
        object_seq, size, ..
    } = *object;
    let invalid = |source: Error| {
        InvalidWalObjectSnafu {
            path: object.path.clone(),
        }
        .into_error(source)
    };
    let object_len = usize::try_from(size)
        .ok()
        .filter(|len| *len >= MIN_OBJECT_LEN)
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!(
                "truncated object, expected at least {MIN_OBJECT_LEN} bytes, actual {size}"
            ),
        })
        .map_err(invalid)?;

    let window = object_len.min(RECOVERY_TAIL_WINDOW);
    let tail_start = object_len - window;
    let (head, tail) = if tail_start == 0 {
        let bytes = io.get(object_seq).await?;
        (bytes.clone(), bytes)
    } else {
        futures::try_join!(
            io.get_range(object_seq, 0, HEADER_LEN as u64),
            io.get_range(object_seq, tail_start as u64, window as u64)
        )?
    };
    if head.len() < HEADER_LEN || tail.len() != window {
        return Err(invalid(
            CorruptedWalObjectSnafu {
                reason: format!(
                    "object holds fewer bytes than the listed {size}, head {} bytes, tail {} bytes",
                    head.len(),
                    tail.len()
                ),
            }
            .build(),
        ));
    }

    let (trailer, footer_range) =
        locate_footer(object_seq, object_len, &head, &tail).map_err(invalid)?;
    let footer = if footer_range.start >= tail_start {
        tail.slice(footer_range.start - tail_start..footer_range.end - tail_start)
    } else {
        io.get_range(
            object_seq,
            footer_range.start as u64,
            footer_range.len() as u64,
        )
        .await?
    };
    let footer = decode_footer(&footer, trailer).map_err(invalid)?;
    verify_segment_ranges(&footer, footer_range.start).map_err(invalid)?;
    Ok(footer)
}

/// Verifies the header and trailer of the object `object_seq` of `object_len`
/// bytes from its first bytes `head` and its last bytes `tail`, and returns
/// the trailer with the range the footer occupies in the object.
fn locate_footer(
    object_seq: u64,
    object_len: usize,
    head: &[u8],
    tail: &[u8],
) -> Result<(FixedTrailer, Range<usize>)> {
    let header = decode_header(head)?;
    ensure!(
        header.object_seq == object_seq,
        CorruptedWalObjectSnafu {
            reason: format!(
                "header sequence {} does not match key sequence {object_seq}",
                header.object_seq
            ),
        }
    );
    let trailer = decode_trailer(&tail[tail.len() - TRAILER_LEN..])?;
    let footer_range = footer_range(trailer, object_len)?;
    Ok((trailer, footer_range))
}

fn durable_entry_ids(catalog: &ObjectCatalog) -> HashMap<RegionId, EntryId> {
    let mut entry_ids = HashMap::new();
    for (_, footer) in catalog.objects_in_order() {
        for entry in footer {
            entry_ids
                .entry(entry.region_id)
                .and_modify(|current: &mut EntryId| *current = (*current).max(entry.max_entry_id))
                .or_insert(entry.max_entry_id);
        }
    }
    entry_ids
}

/// Object access of the store, so tests can inject failures.
#[async_trait::async_trait]
pub(crate) trait WalObjectIo: Send + Sync {
    async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult>;

    async fn get(&self, object_seq: u64) -> Result<Bytes>;

    async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes>;

    async fn list(&self) -> Result<Vec<ListedObject>>;

    fn object_path(&self, object_seq: u64) -> String;
}

#[async_trait::async_trait]
impl WalObjectIo for ObjectStoreIo {
    async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
        ObjectStoreIo::put_if_absent(self, object_seq, content).await
    }

    async fn get(&self, object_seq: u64) -> Result<Bytes> {
        ObjectStoreIo::get(self, object_seq).await
    }

    async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
        ObjectStoreIo::get_range(self, object_seq, offset, len).await
    }

    async fn list(&self) -> Result<Vec<ListedObject>> {
        ObjectStoreIo::list(self).await
    }

    fn object_path(&self, object_seq: u64) -> String {
        ObjectStoreIo::object_path(self, object_seq)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    use common_base::readable_size::ReadableSize;
    use common_error::ext::{ErrorExt, RetryHint};
    use object_store::ErrorKind;
    use object_store::services::Memory;
    use tokio::time::timeout;

    use super::*;
    use crate::error::WalObjectStoreSnafu;
    use crate::object_store_wal::format::{FOOTER_ENTRY_LEN, decode_object};

    const PREFIX: &str = "datanodes/1/epochs/2";
    const WAIT: Duration = Duration::from_secs(30);

    fn memory_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn config(flush_interval: Duration, max_batch_bytes: u64) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            storage_provider: String::new(),
            prefix: PREFIX.to_string(),
            flush_interval,
            max_batch_bytes: ReadableSize(max_batch_bytes),
        }
    }

    /// Every append reaches the size limit, so it is persisted on its own.
    fn eager() -> ObjectStoreWalConfig {
        config(Duration::from_secs(3600), 1)
    }

    /// Nothing is persisted until a test seals the open batch.
    fn manual() -> ObjectStoreWalConfig {
        config(Duration::from_secs(3600), u64::MAX)
    }

    async fn open(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
    ) -> Arc<ObjectStoreLogStore> {
        ObjectStoreLogStore::try_new(object_store, config)
            .await
            .unwrap()
    }

    fn region(number: u32) -> RegionId {
        RegionId::new(1, number)
    }

    fn provider(region_id: RegionId) -> Provider {
        Provider::object_store_provider(region_id, PREFIX.to_string())
    }

    fn entry(store: &ObjectStoreLogStore, region_id: RegionId, data: &str) -> Entry {
        store
            .entry(data.as_bytes().to_vec(), 0, region_id, &provider(region_id))
            .unwrap()
    }

    async fn append(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        data: &str,
    ) -> Result<AppendBatchResponse> {
        store
            .append_batch(vec![entry(store, region_id, data)])
            .await
    }

    async fn read(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        start_entry_id: EntryId,
    ) -> Vec<(EntryId, Vec<u8>)> {
        store
            .read(&provider(region_id), start_entry_id, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|entry| (entry.entry_id(), entry.into_bytes()))
            .collect()
    }

    fn entries(expected: &[(EntryId, &str)]) -> Vec<(EntryId, Vec<u8>)> {
        expected
            .iter()
            .map(|(entry_id, data)| (*entry_id, data.as_bytes().to_vec()))
            .collect()
    }

    fn latest(store: &ObjectStoreLogStore, region_id: RegionId) -> EntryId {
        store.latest_entry_id(&provider(region_id)).unwrap()
    }

    async fn object_seqs(io: &dyn WalObjectIo) -> Vec<u64> {
        io.list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .collect()
    }

    fn unwrap_shared(error: &Error) -> &Error {
        match error {
            Error::ObjectStoreWal { source, .. } => source,
            other => panic!("expected a shared error, actual {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_store_batches_regions_into_shared_objects() {
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);

        let first = {
            let store = store.clone();
            let entries = vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
            ];
            tokio::spawn(async move { store.append_batch(entries).await })
        };
        store.wait_for_admitted_appends(1).await.unwrap();
        let second = {
            let store = store.clone();
            let entries = vec![
                entry(&store, region_two, "b2"),
                entry(&store, region_one, "a2"),
            ];
            tokio::spawn(async move { store.append_batch(entries).await })
        };
        store.wait_for_admitted_appends(2).await.unwrap();
        assert!(!first.is_finished());
        assert_eq!(0, latest(&store, region_one));

        store.seal_open_batch().await.unwrap();
        let first = first.await.unwrap().unwrap().last_entry_ids;
        assert_eq!(HashMap::from([(region_one, 1), (region_two, 1)]), first);
        let second = second.await.unwrap().unwrap().last_entry_ids;
        assert_eq!(HashMap::from([(region_one, 2), (region_two, 2)]), second);

        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2")]),
            read(&store, region_one, 0).await
        );
        assert_eq!(
            entries(&[(1, "b1"), (2, "b2")]),
            read(&store, region_two, 0).await
        );
        assert_eq!(entries(&[(2, "b2")]), read(&store, region_two, 2).await);
        assert_eq!(2, latest(&store, region_one));
        assert_eq!(2, latest(&store, region_two));
        assert_eq!(0, latest(&store, region(3)));
        assert_eq!(
            vec![provider(region_one), provider(region_two)],
            store.list_namespaces().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_store_recovers_catalog_after_restart() {
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);

        let store = open(object_store.clone(), &eager()).await;
        append(&store, region_one, "a1").await.unwrap();
        store
            .append_batch(vec![
                entry(&store, region_two, "b1"),
                entry(&store, region_one, "a2"),
            ])
            .await
            .unwrap();
        append(&store, region_one, "a3").await.unwrap();
        let expected_one = read(&store, region_one, 1).await;
        let expected_two = read(&store, region_two, 1).await;
        assert_eq!(entries(&[(1, "a1"), (2, "a2"), (3, "a3")]), expected_one);
        assert_eq!(entries(&[(1, "b1")]), expected_two);
        store.stop().await.unwrap();
        drop(store);

        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(expected_one, read(&store, region_one, 1).await);
        assert_eq!(expected_two, read(&store, region_two, 1).await);
        assert_eq!(3, latest(&store, region_one));
        assert_eq!(1, latest(&store, region_two));

        let response = append(&store, region_two, "b2").await.unwrap();
        assert_eq!(Some(&2), response.last_entry_ids.get(&region_two));
        assert_eq!(vec![0, 1, 2, 3], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(1, "b1"), (2, "b2")]),
            read(&store, region_two, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_accepts_identical_retry_after_reported_failure() {
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);

        io.fail_after_next_put.store(true, Ordering::Relaxed);
        let error = append(&store, region_id, "a1").await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(0, latest(&store, region_id));

        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(Some(&1), response.last_entry_ids.get(&region_id));
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(1, latest(&store, region_id));

        append(&store, region_id, "a2").await.unwrap();
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_fails_closed_on_conflicting_object() {
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        io.put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();

        let retry = entry(&store, region_id, "a1");
        let error = append(&store, region_id, "a1").await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { path, .. } if path == &io.object_path(0)),
            "unexpected error: {error:?}"
        );
        let error = store.append_batch(vec![retry]).await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        assert!(store.latest_entry_id(&provider(region_id)).is_err());
        assert_eq!(vec![0], object_seqs(&io).await);

        let error = ObjectStoreLogStore::try_new(object_store, &eager())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidWalObject { path, source, .. }
                if path == &io.object_path(0) && matches!(**source, Error::CorruptedWalObject { .. })),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_poisoned_by_conflict_rejects_every_operation() {
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        let before = read(&store, region_id, 1).await;
        assert_eq!(entries(&[(1, "a1")]), before);

        let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        io.put_if_absent(1, Bytes::from_static(b"foreign"))
            .await
            .unwrap();
        append(&store, region_id, "a2").await.unwrap_err();

        let provider = provider(region_id);
        let errors = [
            store.create_namespace(&provider).await.unwrap_err(),
            store.delete_namespace(&provider).await.unwrap_err(),
            store.list_namespaces().await.unwrap_err(),
            store
                .entry(Vec::new(), 0, region_id, &provider)
                .unwrap_err(),
            store.obsolete(&provider, region_id, 1).await.unwrap_err(),
            store.obsolete_all(&provider, region_id).await.unwrap_err(),
            store.read(&provider, 1, None).await.err().unwrap(),
            store.latest_entry_id(&provider).unwrap_err(),
            store.append_batch(Vec::new()).await.unwrap_err(),
        ];
        for error in &errors {
            assert!(
                matches!(unwrap_shared(error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
        // The rejected obsoletes did not move the watermark.
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_id)
                .is_none()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_corrupted_object() {
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        append(&store, region(1), "a1").await.unwrap();
        store.stop().await.unwrap();
        drop(store);

        let path = ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .object_path(0);
        let mut bytes = object_store.read(&path).await.unwrap().to_vec();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        object_store.write(&path, bytes).await.unwrap();

        let error = ObjectStoreLogStore::try_new(object_store, &eager())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidWalObject { path: actual, source, .. }
                if actual == &path && matches!(**source, Error::CorruptedWalObject { .. })),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_keeps_sequence_after_transient_failure() {
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);

        io.fail_next_put.store(true, Ordering::Relaxed);
        let error = append(&store, region_id, "a1").await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert_eq!(0, latest(&store, region_id));

        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(Some(&1), response.last_entry_ids.get(&region_id));
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_id, 1).await);
    }

    #[tokio::test]
    async fn test_store_rejects_foreign_providers() {
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);
        let other_prefix = Provider::object_store_provider(region_id, "other/prefix".to_string());
        let raft_engine = Provider::raft_engine_provider(region_id.as_u64());

        for foreign in [&other_prefix, &raft_engine] {
            let entry = Entry::Naive(NaiveEntry {
                provider: foreign.clone(),
                region_id,
                entry_id: 0,
                data: b"a1".to_vec(),
            });
            let errors = [
                store.append_batch(vec![entry]).await.unwrap_err(),
                store.read(foreign, 0, None).await.err().unwrap(),
                store.latest_entry_id(foreign).unwrap_err(),
                store.entry(Vec::new(), 0, region_id, foreign).unwrap_err(),
                store.obsolete(foreign, region_id, 1).await.unwrap_err(),
            ];
            for error in errors {
                if foreign == &raft_engine {
                    assert!(
                        matches!(error, Error::InvalidProvider { .. }),
                        "unexpected error: {error:?}"
                    );
                } else {
                    assert!(
                        matches!(&error, Error::MismatchedWalPrefix { expected, actual, .. }
                            if expected == PREFIX && actual == "other/prefix"),
                        "unexpected error: {error:?}"
                    );
                }
            }
        }
        assert_eq!(0, latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_rejects_entries_of_another_region() {
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);

        let error = store
            .entry(Vec::new(), 0, region(2), &provider(region_id))
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntry { .. }),
            "unexpected error: {error:?}"
        );
        let entry = Entry::Naive(NaiveEntry {
            provider: provider(region_id),
            region_id: region(2),
            entry_id: 0,
            data: Vec::new(),
        });
        let error = store.append_batch(vec![entry]).await.unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntry { .. }),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .append_batch(Vec::new())
                .await
                .unwrap()
                .last_entry_ids
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_store_obsolete_hides_entries_from_read_only() {
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        for data in ["a1", "a2", "a3"] {
            append(&store, region_id, data).await.unwrap();
        }

        store
            .obsolete(&provider(region_id), region_id, 2)
            .await
            .unwrap();
        assert_eq!(entries(&[(3, "a3")]), read(&store, region_id, 1).await);
        assert_eq!(entries(&[(3, "a3")]), read(&store, region_id, 3).await);
        assert_eq!(3, latest(&store, region_id));

        // A lower watermark does not resurrect entries.
        store
            .obsolete(&provider(region_id), region_id, 1)
            .await
            .unwrap();
        assert_eq!(entries(&[(3, "a3")]), read(&store, region_id, 1).await);
        // The provider of another region cannot move this region's watermark.
        let error = store
            .obsolete(&provider(region(2)), region_id, 3)
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntry { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(entries(&[(3, "a3")]), read(&store, region_id, 1).await);

        store
            .obsolete_all(&provider(region_id), region_id)
            .await
            .unwrap();
        assert!(read(&store, region_id, 1).await.is_empty());
        assert_eq!(3, latest(&store, region_id));

        // Even the maximum entry id is hidden after obsoleting everything.
        let object_store = memory_store();
        put_object_with_max_entry_id(&object_store, region_id).await;
        let store = open(object_store, &manual()).await;
        assert_eq!(
            entries(&[(u64::MAX, "last")]),
            read(&store, region_id, 0).await
        );
        store
            .obsolete_all(&provider(region_id), region_id)
            .await
            .unwrap();
        assert!(read(&store, region_id, 0).await.is_empty());
        assert_eq!(u64::MAX, latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_stop_is_idempotent_and_fails_open_waiters() {
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a1").await })
        };
        store.wait_for_admitted_appends(1).await.unwrap();

        store.stop().await.unwrap();
        let error = pending.await.unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        store.stop().await.unwrap();

        let error = append(&store, region_id, "a2").await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        let error = store.append_batch(Vec::new()).await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        // The actor exited, so nothing admits further appends.
        let exited = timeout(WAIT, store.wait_for_admitted_appends(2))
            .await
            .unwrap();
        assert!(matches!(exited, Err(Error::ObjectStoreWalStopped { .. })));
        assert!(store.command_tx.is_closed());
        assert!(object_seqs(store.io.as_ref()).await.is_empty());
    }

    /// Appends one entry, lets its flush block inside the conditional create,
    /// stops the store while it is blocked, then lets the create proceed or
    /// fail. Returns the store and the outcome of the append.
    async fn stop_while_flush_is_blocked(
        proceed: bool,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<GatedIo>,
        Result<AppendBatchResponse>,
    ) {
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();

        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!stop.is_finished());

        gate.send(proceed).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        let result = timeout(WAIT, pending).await.unwrap().unwrap();
        (store, io, result)
    }

    #[tokio::test]
    async fn test_store_stop_during_failed_flush_reports_stopped() {
        let (store, io, result) = stop_while_flush_is_blocked(false).await;
        let error = result.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        assert!(object_seqs(io.as_ref()).await.is_empty());

        let error = append(&store, region(1), "a2").await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_stop_rejects_appends_queued_behind_a_flush() {
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let first = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();

        // The second append passes the public stopped check and queues behind
        // the blocked flush.
        let second = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a2").await })
        };
        while store.command_tx.capacity() == COMMAND_BUFFER {
            tokio::task::yield_now().await;
        }
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        gate.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, 1)]),
            timeout(WAIT, first)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .last_entry_ids
        );
        let error = timeout(WAIT, second).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        // No second object was created, so no second create was attempted.
        assert!(gates.try_recv().is_err());
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(1, latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_seal_after_stop_reports_stopped() {
        let store = open(memory_store(), &manual()).await;
        store.stop().await.unwrap();
        let error = store.seal_open_batch().await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        assert!(object_seqs(store.io.as_ref()).await.is_empty());

        // A seal queued behind a blocked flush before `stop` is refused too.
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let first = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region(1), "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        while store.command_tx.capacity() == COMMAND_BUFFER {
            tokio::task::yield_now().await;
        }
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        gate.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        timeout(WAIT, first).await.unwrap().unwrap().unwrap();
        let error = timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_stop_during_conflicting_flush_reports_stopped_and_poisons() {
        let object_store = memory_store();
        let (io, mut gates) = GatedIo::over(object_store.clone());
        let store = ObjectStoreLogStore::open(io, &eager()).await.unwrap();
        let region_id = region(1);
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        // The sequence is taken by different content before the create runs.
        ObjectStoreIo::new(object_store, PREFIX)
            .unwrap()
            .put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();

        gate.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        let error = timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        for error in [
            store.latest_entry_id(&provider(region_id)).unwrap_err(),
            store
                .read(&provider(region_id), 1, None)
                .await
                .err()
                .unwrap(),
        ] {
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
    }

    /// Writes object 0 holding one entry of `region_id` with the maximum id.
    async fn put_object_with_max_entry_id(object_store: &ObjectStore, region_id: RegionId) {
        let encoded = encode_object(
            Header {
                object_seq: 0,
                writer_instance: [0; 16],
            },
            &[Record {
                region_id,
                entry_id: u64::MAX,
                payload: Bytes::from_static(b"last"),
            }],
        )
        .unwrap();
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .put_if_absent(0, encoded.bytes)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_exhausted_entry_id_poisons_without_panicking() {
        let object_store = memory_store();
        let region_id = region(1);
        put_object_with_max_entry_id(&object_store, region_id).await;
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();

        let store = open(object_store, &manual()).await;
        assert_eq!(u64::MAX, latest(&store, region_id));
        // An append admitted earlier fails right away too, not at the next tick.
        let admitted = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region(2), "other").await })
        };
        store.wait_for_admitted_appends(1).await.unwrap();
        let error = timeout(WAIT, append(&store, region_id, "next"))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalEntryIdExhausted { .. }),
            "unexpected error: {error:?}"
        );
        let error = timeout(WAIT, admitted).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalEntryIdExhausted { .. }),
            "unexpected error: {error:?}"
        );
        assert!(store.latest_entry_id(&provider(region_id)).is_err());
        assert_eq!(vec![0], object_seqs(&io).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_stop_during_successful_flush_acknowledges_waiters() {
        let (store, io, result) = stop_while_flush_is_blocked(true).await;
        let region_id = region(1);
        assert_eq!(
            HashMap::from([(region_id, 1)]),
            result.unwrap().last_entry_ids
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(1, latest(&store, region_id));

        let error = append(&store, region_id, "a2").await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_actor_exits_when_the_store_is_dropped() {
        let store = open(memory_store(), &manual()).await;
        let mut admitted_appends = store.admitted_appends.clone();
        drop(store);

        // The sender side lives in the actor, so the receiver fails once it exits.
        timeout(WAIT, admitted_appends.changed())
            .await
            .unwrap()
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_store_seals_by_size_and_by_interval() {
        // Only the size limit can seal within the test: the interval is an hour.
        let store = open(memory_store(), &eager()).await;
        timeout(WAIT, append(&store, region(1), "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        // Only the interval can seal: the size limit is never reached.
        let store = open(memory_store(), &config(Duration::from_secs(1), u64::MAX)).await;
        timeout(WAIT, append(&store, region(1), "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_flushes_at_the_minimum_interval() {
        // An append is acknowledged once its object is durable, so a completed
        // append proves the tick after it sealed the batch. The appends are
        // spaced far apart, so every one of them lands in its own tick and
        // the ticks in between find an empty batch.
        let interval = MIN_FLUSH_INTERVAL;
        let store = open(memory_store(), &config(interval, u64::MAX)).await;
        let data = ["a1", "a2", "a3"];
        for (index, data) in data.iter().enumerate() {
            tokio::time::sleep(interval * 5).await;
            timeout(WAIT, append(&store, region(1), data))
                .await
                .unwrap()
                .unwrap();
            let expected_seqs = (0..=index as u64).collect::<Vec<_>>();
            assert_eq!(expected_seqs, object_seqs(store.io.as_ref()).await);
        }

        // Ticks with an empty open batch do not create objects.
        tokio::time::sleep(interval * 5).await;
        let seqs = object_seqs(store.io.as_ref()).await;
        assert_eq!(vec![0, 1, 2], seqs);
        for object_seq in seqs {
            let bytes = store.io.get(object_seq).await.unwrap();
            let decoded = decode_object(&bytes).unwrap();
            assert_eq!(1, decoded.records.len(), "object {object_seq} is empty");
        }
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2"), (3, "a3")]),
            read(&store, region(1), 1).await
        );
    }

    #[tokio::test]
    async fn test_store_rejects_invalid_config() {
        for config in [
            config(Duration::from_millis(9), 1),
            config(Duration::from_secs(1), 0),
            ObjectStoreWalConfig {
                prefix: "/absolute".to_string(),
                ..config(Duration::from_secs(1), 1)
            },
        ] {
            let error = ObjectStoreLogStore::try_new(memory_store(), &config)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, Error::InvalidWalObjectStore { .. }),
                "unexpected error for {config:?}: {error:?}"
            );
        }
    }

    /// Rebuilds the catalog the way recovery did before footers were fetched
    /// on their own: every object is read and decoded in full.
    async fn recover_by_decoding(
        io: &dyn WalObjectIo,
    ) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
        let mut catalog = ObjectCatalog::default();
        for ListedObject {
            object_seq, path, ..
        } in io.list().await?
        {
            let bytes = io.get(object_seq).await?;
            decode_object(&bytes)
                .and_then(|decoded| {
                    ensure!(
                        decoded.header.object_seq == object_seq,
                        CorruptedWalObjectSnafu {
                            reason: format!(
                                "header sequence {} does not match key sequence {object_seq}",
                                decoded.header.object_seq
                            ),
                        }
                    );
                    catalog.insert_object(object_seq, decoded.footer)
                })
                .with_context(|_| InvalidWalObjectSnafu { path })?;
        }
        finish_recovery(catalog)
    }

    fn catalog_contents(catalog: &ObjectCatalog) -> Vec<(u64, Vec<FooterEntry>)> {
        catalog
            .objects_in_order()
            .map(|(object_seq, footer)| (object_seq, footer.to_vec()))
            .collect()
    }

    /// Writes `objects` objects, each holding one entry of most of the
    /// regions `1..=regions`, with a different subset per object.
    async fn populate(object_store: &ObjectStore, objects: usize, regions: u32) {
        let store = open(object_store.clone(), &eager()).await;
        for object in 0..objects {
            let entries = (1..=regions)
                .filter(|number| !(object + *number as usize).is_multiple_of(3))
                .map(|number| entry(&store, region(number), &format!("o{object}-r{number}")))
                .collect::<Vec<_>>();
            store.append_batch(entries).await.unwrap();
        }
        store.stop().await.unwrap();
    }

    fn object_path(object_store: &ObjectStore, object_seq: u64) -> String {
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .object_path(object_seq)
    }

    async fn corrupt_object(
        object_store: &ObjectStore,
        path: &str,
        corrupt: impl FnOnce(&mut Vec<u8>),
    ) {
        let mut bytes = object_store.read(path).await.unwrap().to_vec();
        corrupt(&mut bytes);
        object_store.write(path, bytes).await.unwrap();
    }

    fn footer_of(bytes: &[u8]) -> (FixedTrailer, Vec<FooterEntry>) {
        let trailer = decode_trailer(&bytes[bytes.len() - TRAILER_LEN..]).unwrap();
        let footer =
            decode_footer(&bytes[footer_range(trailer, bytes.len()).unwrap()], trailer).unwrap();
        (trailer, footer)
    }

    fn assert_invalid_object(error: &Error, path: &str, reason: &str) {
        match error {
            Error::InvalidWalObject {
                path: actual,
                source,
                ..
            } => {
                assert_eq!(path, actual);
                match &**source {
                    Error::CorruptedWalObject { reason: actual, .. } => assert!(
                        actual.contains(reason),
                        "expected reason to contain {reason:?}, actual {actual:?}"
                    ),
                    other => panic!("expected a corrupted object error, actual {other:?}"),
                }
            }
            other => panic!("expected an invalid object error, actual {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_store_footer_recovery_matches_full_decode() {
        let object_store = memory_store();
        populate(&object_store, 40, 5).await;
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();

        let (catalog, next_object_seq, durable) = recover(&io).await.unwrap();
        let (expected_catalog, expected_next_object_seq, expected_durable) =
            recover_by_decoding(&io).await.unwrap();

        assert_eq!(40, catalog_contents(&catalog).len());
        assert_eq!(
            catalog_contents(&expected_catalog),
            catalog_contents(&catalog)
        );
        assert_eq!(expected_next_object_seq, next_object_seq);
        assert_eq!(40, next_object_seq);
        assert_eq!(expected_durable, durable);
        assert_eq!(5, durable.len());

        let store = open(object_store, &eager()).await;
        for number in 1..=5 {
            let region_id = region(number);
            assert_eq!(durable[&region_id], latest(&store, region_id));
            assert_eq!(
                durable[&region_id] as usize,
                read(&store, region_id, 1).await.len()
            );
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_corrupted_trailer_version_and_footer() {
        type Corrupt = fn(&mut Vec<u8>);
        let cases: [(&str, Corrupt); 3] = [
            ("invalid trailer magic", |bytes| {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }),
            ("unsupported format version 2", |bytes| {
                bytes[8..10].copy_from_slice(&2u16.to_be_bytes());
            }),
            ("footer checksum mismatch", |bytes| {
                let (trailer, _) = footer_of(bytes);
                bytes[trailer.footer_offset as usize] ^= 1;
            }),
        ];
        for (reason, corrupt) in cases {
            let object_store = memory_store();
            populate(&object_store, 3, 2).await;
            let path = object_path(&object_store, 1);
            corrupt_object(&object_store, &path, corrupt).await;

            let error = ObjectStoreLogStore::try_new(object_store, &eager())
                .await
                .unwrap_err();
            assert_invalid_object(&error, &path, reason);
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_header_sequence_mismatch() {
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let encoded = encode_object(
            Header {
                object_seq: 0,
                writer_instance: [0; 16],
            },
            &[Record {
                region_id: region(1),
                entry_id: 1,
                payload: Bytes::from_static(b"a1"),
            }],
        )
        .unwrap();
        io.put_if_absent(5, encoded.bytes).await.unwrap();

        let error = ObjectStoreLogStore::try_new(object_store, &eager())
            .await
            .unwrap_err();
        assert_invalid_object(
            &error,
            &io.object_path(5),
            "header sequence 0 does not match key sequence 5",
        );
    }

    /// Overwrites the byte range of footer entry `index` and refreshes the
    /// footer checksum, so the footer is intact but describes the wrong bytes.
    fn rewrite_segment_range(bytes: &mut [u8], index: usize, offset: u64, len: u64) {
        let trailer_start = bytes.len() - TRAILER_LEN;
        let (trailer, _) = footer_of(bytes);
        let footer = footer_range(trailer, bytes.len()).unwrap();
        let entry = footer.start + 4 + index * FOOTER_ENTRY_LEN;
        bytes[entry + 28..entry + 36].copy_from_slice(&offset.to_be_bytes());
        bytes[entry + 36..entry + 44].copy_from_slice(&len.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[footer]);
        bytes[trailer_start + 16..trailer_start + 20].copy_from_slice(&checksum.to_be_bytes());
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_segment_ranges_that_do_not_tile_the_object() {
        type Corrupt = fn(&mut Vec<u8>, &[FooterEntry]);
        let cases: [(&str, Corrupt); 5] = [
            ("overflows the object", |bytes, footer| {
                rewrite_segment_range(bytes, 0, u64::MAX, footer[0].segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset + 1, second.segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset - 1, second.segment_len);
            }),
            ("invalid segment range", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset, second.segment_len + 1);
            }),
            ("segments end at", |bytes, footer| {
                let second = &footer[1];
                rewrite_segment_range(bytes, 1, second.segment_offset, second.segment_len - 1);
            }),
        ];
        for (reason, corrupt) in cases {
            let object_store = memory_store();
            populate(&object_store, 2, 3).await;
            let path = object_path(&object_store, 1);
            corrupt_object(&object_store, &path, |bytes| {
                let (_, footer) = footer_of(bytes);
                assert_eq!(2, footer.len());
                corrupt(bytes, &footer);
                // The footer itself still verifies.
                footer_of(bytes);
            })
            .await;

            let error = ObjectStoreLogStore::try_new(object_store.clone(), &eager())
                .await
                .unwrap_err();
            assert_invalid_object(&error, &path, reason);
            let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
            let error = recover_by_decoding(&io).await.unwrap_err();
            assert_invalid_object(&error, &path, reason);
        }
    }

    #[tokio::test]
    async fn test_store_corrupted_segment_fails_the_read_that_decodes_it() {
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);
        let store = open(object_store.clone(), &eager()).await;
        store
            .append_batch(vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
            ])
            .await
            .unwrap();
        append(&store, region_two, "b2").await.unwrap();
        store.stop().await.unwrap();
        let path = object_path(&object_store, 0);
        corrupt_object(&object_store, &path, |bytes| {
            let (_, footer) = footer_of(bytes);
            let segment = &footer[1];
            assert_eq!(region_two, segment.region_id);
            bytes[(segment.segment_offset + segment.segment_len - 1) as usize] ^= 1;
        })
        .await;

        // Recovery indexes the object; only the corrupted segment is unreadable.
        let store = open(object_store, &eager()).await;
        assert_eq!(1, latest(&store, region_one));
        assert_eq!(2, latest(&store, region_two));
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_one, 1).await);
        assert_eq!(entries(&[(2, "b2")]), read(&store, region_two, 2).await);
        let error = store
            .read(&provider(region_two), 1, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_invalid_object(
            &error,
            &path,
            &format!("segment of region {region_two} checksum mismatch"),
        );
    }

    #[tokio::test]
    async fn test_store_recovery_bounds_concurrency_and_orders_by_sequence() {
        let object_store = memory_store();
        populate(&object_store, 10, 3).await;
        let (io, mut parked) = ParkedIo::over(object_store);
        let objects = io.list().await.unwrap();

        let mut fetch = {
            let io = io.clone();
            tokio::spawn(async move {
                fetch_footers(io.as_ref(), objects, 3)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(object, _)| object.object_seq)
                    .collect::<Vec<_>>()
            })
        };
        let mut wave = Vec::new();
        for _ in 0..3 {
            wave.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        assert_eq!(
            vec![0, 1, 2],
            wave.iter()
                .map(|(object_seq, _)| *object_seq)
                .collect::<Vec<_>>()
        );
        // A fourth fetch waits for one of the three to complete.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(parked.try_recv().is_err());

        // Completing out of order admits the next object right away.
        for (expected_next, (_, release)) in [3, 4, 5].into_iter().zip(wave.into_iter().rev()) {
            release.send(()).unwrap();
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            assert_eq!(expected_next, object_seq);
            assert!(!fetch.is_finished());
            release.send(()).unwrap();
        }
        loop {
            tokio::select! {
                order = &mut fetch => {
                    assert_eq!((0..10).collect::<Vec<u64>>(), order.unwrap());
                    break;
                }
                next = parked.recv() => {
                    let (_, release) = next.unwrap();
                    release.send(()).unwrap();
                }
            }
        }
        assert_eq!(3, io.max_in_flight.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_store_recovery_fails_closed_on_a_fetch_failure() {
        let object_store = memory_store();
        populate(&object_store, 4, 2).await;
        let io = Arc::new(FaultyIo::over(object_store));
        io.fail_reads_of.store(2, Ordering::Relaxed);

        let error = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                if path == &io.object_path(2)),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());

        // Nothing of the failed attempt survives: a retry recovers everything.
        io.fail_reads_of.store(u64::MAX, Ordering::Relaxed);
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        assert_eq!(3, read(&store, region(1), 1).await.len());
        assert_eq!(3, read(&store, region(2), 1).await.len());
        append(&store, region(1), "a4").await.unwrap();
        assert_eq!(vec![0, 1, 2, 3, 4], object_seqs(io.as_ref()).await);
        assert_eq!(4, read(&store, region(1), 1).await.len());
    }

    #[tokio::test]
    async fn test_store_recovery_fetches_a_footer_longer_than_the_tail_window() {
        let object_store = memory_store();
        let regions = (RECOVERY_TAIL_WINDOW / FOOTER_ENTRY_LEN + 100) as u32;
        let store = open(object_store.clone(), &eager()).await;
        let wide_entries = (1..=regions)
            .map(|number| entry(&store, region(number), "wide"))
            .collect::<Vec<_>>();
        store.append_batch(wide_entries).await.unwrap();
        append(&store, region(1), "narrow").await.unwrap();
        store.stop().await.unwrap();

        let (io, reads) = RecordingIo::over(object_store.clone());
        let objects = io.list().await.unwrap();
        let wide = &objects[0];
        let (trailer, _) = footer_of(&object_store.read(&wide.path).await.unwrap().to_vec());
        assert!(trailer.footer_len > RECOVERY_TAIL_WINDOW as u64);

        let (catalog, next_object_seq, durable) = recover(io.as_ref()).await.unwrap();
        let (expected_catalog, expected_next_object_seq, expected_durable) =
            recover_by_decoding(io.as_ref()).await.unwrap();
        assert_eq!(
            catalog_contents(&expected_catalog),
            catalog_contents(&catalog)
        );
        assert_eq!(expected_next_object_seq, next_object_seq);
        assert_eq!(expected_durable, durable);
        assert_eq!(regions as usize, durable.len());

        // The wide object took the header, the tail window and the footer;
        // the narrow one was read whole.
        let mut wide_reads = reads
            .lock()
            .unwrap()
            .iter()
            .filter(|(object_seq, _, _)| *object_seq == wide.object_seq)
            .map(|(_, offset, len)| (*offset, *len))
            .collect::<Vec<_>>();
        wide_reads.sort_unstable();
        assert_eq!(
            vec![
                (0, HEADER_LEN as u64),
                (trailer.footer_offset, trailer.footer_len),
                (
                    wide.size - RECOVERY_TAIL_WINDOW as u64,
                    RECOVERY_TAIL_WINDOW as u64
                ),
            ],
            wide_reads
        );
        assert!(
            reads
                .lock()
                .unwrap()
                .iter()
                .all(|(object_seq, _, _)| *object_seq == wide.object_seq)
        );

        let store = open(object_store, &eager()).await;
        assert_eq!(
            entries(&[(1, "wide"), (2, "narrow")]),
            read(&store, region(1), 1).await
        );
        assert_eq!(
            entries(&[(1, "wide")]),
            read(&store, region(regions), 1).await
        );
    }

    /// Object access that fails a conditional create on request, either before
    /// or after the object was actually written, or every read of one object.
    struct FaultyIo {
        inner: ObjectStoreIo,
        fail_next_put: AtomicBool,
        fail_after_next_put: AtomicBool,
        /// Sequence of the object whose reads fail; `u64::MAX` fails none.
        fail_reads_of: AtomicU64,
    }

    impl FaultyIo {
        fn new() -> Self {
            Self::over(memory_store())
        }

        fn over(object_store: ObjectStore) -> Self {
            Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                fail_next_put: AtomicBool::new(false),
                fail_after_next_put: AtomicBool::new(false),
                fail_reads_of: AtomicU64::new(u64::MAX),
            }
        }

        fn check_read(&self, object_seq: u64) -> Result<()> {
            if self.fail_reads_of.load(Ordering::Relaxed) == object_seq {
                injected_failure("read", self.inner.object_path(object_seq))
            } else {
                Ok(())
            }
        }
    }

    fn injected_failure<T>(operation: &'static str, path: String) -> Result<T> {
        Err(object_store::Error::new(ErrorKind::Unexpected, "injected failure").set_temporary())
            .context(WalObjectStoreSnafu { operation, path })
    }

    #[async_trait::async_trait]
    impl WalObjectIo for FaultyIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            if self.fail_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure("write", self.inner.object_path(object_seq));
            }
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if self.fail_after_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure("write", self.inner.object_path(object_seq));
            }
            Ok(result)
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.check_read(object_seq)?;
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.check_read(object_seq)?;
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    /// Object access whose conditional creates block until the test decides
    /// whether they proceed or fail with a transient error.
    struct GatedIo {
        inner: ObjectStoreIo,
        gates: mpsc::UnboundedSender<oneshot::Sender<bool>>,
    }

    impl GatedIo {
        fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<oneshot::Sender<bool>>) {
            Self::over(memory_store())
        }

        fn over(
            object_store: ObjectStore,
        ) -> (Arc<Self>, mpsc::UnboundedReceiver<oneshot::Sender<bool>>) {
            let (gates, gate_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                gates,
            };
            (Arc::new(io), gate_rx)
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for GatedIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            let (gate, opened) = oneshot::channel();
            self.gates.send(gate).unwrap();
            if opened.await.unwrap() {
                self.inner.put_if_absent(object_seq, content).await
            } else {
                injected_failure("write", self.inner.object_path(object_seq))
            }
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    /// Object access whose reads park until the test releases them, counting
    /// how many are in flight. Objects must be short enough to be read whole,
    /// so every object costs exactly one read.
    struct ParkedIo {
        inner: ObjectStoreIo,
        parked: mpsc::UnboundedSender<(u64, oneshot::Sender<()>)>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl ParkedIo {
        fn over(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<()>)>,
        ) {
            let (parked, parked_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                parked,
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
            };
            (Arc::new(io), parked_rx)
        }

        async fn park(&self, object_seq: u64) {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            let (release, released) = oneshot::channel();
            self.parked.send((object_seq, release)).unwrap();
            released.await.unwrap();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for ParkedIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.park(object_seq).await;
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.park(object_seq).await;
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    type RangeReads = Arc<Mutex<Vec<(u64, u64, u64)>>>;

    /// Object access that records every range read as (sequence, offset, length).
    struct RecordingIo {
        inner: ObjectStoreIo,
        reads: RangeReads,
    }

    impl RecordingIo {
        fn over(object_store: ObjectStore) -> (Arc<Self>, RangeReads) {
            let reads = RangeReads::default();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                reads: reads.clone(),
            };
            (Arc::new(io), reads)
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RecordingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.reads.lock().unwrap().push((object_seq, offset, len));
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }
}

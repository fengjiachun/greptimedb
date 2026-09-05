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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use async_stream::try_stream;
use bytes::Bytes;
use common_wal::config::object_store::ObjectStoreWalConfig;
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
    EncodedObject, FooterEntry, Header, Record, decode_object, decode_segment, encode_object,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// A [`LogStore`] that persists the entries of many regions as immutable
/// objects under one prefix.
///
/// Appends are admitted into an open batch and acknowledged once the object
/// holding them is durable. A background actor seals the batch when it reaches
/// the size limit or the flush interval elapses, creates the object under the
/// next sequence and indexes it in the catalog. Reads decode only the segment
/// of the requested region from every object the catalog lists for it.
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
        let start_entry_id = self
            .obsolete_entry_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&region_id)
            .map_or(entry_id, |obsolete| {
                entry_id.max(obsolete.saturating_add(1))
            });
        let objects = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            match catalog.region_max_entry_id(region_id) {
                Some(max_entry_id) if start_entry_id <= max_entry_id => catalog
                    .objects_for_entry_range(region_id, start_entry_id, max_entry_id)?
                    .into_iter()
                    .map(|(object_seq, entry)| (object_seq, entry.clone()))
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            }
        };

        let io = self.io.clone();
        let provider = provider.clone();
        Ok(Box::pin(try_stream! {
            for (object_seq, footer_entry) in objects {
                let bytes = io.get(object_seq).await?;
                let records = decode_region_segment(&bytes, &footer_entry)
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
        self.region_of(provider)?;
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
                _ = interval.tick() => self.flush_open_batch().await,
                command = self.command_rx.recv() => match command {
                    Some(Command::Append { entries, response }) => {
                        self.handle_append(entries, response).await;
                    }
                    Some(Command::Stop { response }) => {
                        self.fail_pending(|| ObjectStoreWalStoppedSnafu.build());
                        let _ = response.send(());
                        return;
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Seal { response }) => {
                        self.flush_open_batch().await;
                        let result = terminal(&self.terminal_error)
                            .map_or(Ok(()), |error| Err(shared(&error)));
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
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        let last_entry_ids = self.open_batch.admit(entries);
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
            Err(error) => return self.fail_permanently(error),
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
                if self.stopped.load(Ordering::Acquire) {
                    self.fail_pending(|| ObjectStoreWalStoppedSnafu.build());
                } else {
                    let error = Arc::new(error);
                    self.fail_pending(|| shared(&error));
                }
                return;
            }
            Err(error) => return self.fail_permanently(error),
        }

        let indexed = self
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert_object(object_seq, encoded.footer);
        if let Err(error) = indexed {
            return self.fail_permanently(error);
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

    fn fail_pending(&mut self, error: impl Fn() -> Error) {
        for pending in self.pending.drain(..) {
            let _ = pending.response.send(Err(error()));
        }
    }

    fn fail_permanently(&mut self, error: Error) {
        let error = set_terminal(&self.terminal_error, error);
        self.fail_pending(|| shared(&error));
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

/// Decodes the segment that `entry` locates inside the object `bytes`.
fn decode_region_segment(bytes: &[u8], entry: &FooterEntry) -> Result<Vec<Record>> {
    let segment = usize::try_from(entry.segment_offset)
        .ok()
        .zip(usize::try_from(entry.segment_len).ok())
        .and_then(|(start, len)| start.checked_add(len).map(|end| start..end))
        .and_then(|range| bytes.get(range))
        .with_context(|| CorruptedWalObjectSnafu {
            reason: format!(
                "segment of region {} at offset {} with length {} is outside the object of {} bytes",
                entry.region_id,
                entry.segment_offset,
                entry.segment_len,
                bytes.len()
            ),
        })?;
    decode_segment(segment, entry)
}

/// Rebuilds the catalog from the objects under the prefix and returns it with
/// the next object sequence and the largest durable entry id per region.
async fn recover(io: &dyn WalObjectIo) -> Result<(ObjectCatalog, u64, HashMap<RegionId, EntryId>)> {
    let mut catalog = ObjectCatalog::default();
    for ListedObject { object_seq, path } in io.list().await? {
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
    let next_object_seq = catalog.next_object_seq()?;
    let durable_entry_ids = durable_entry_ids(&catalog);
    Ok((catalog, next_object_seq, durable_entry_ids))
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

    async fn list(&self) -> Result<Vec<ListedObject>> {
        ObjectStoreIo::list(self).await
    }

    fn object_path(&self, object_seq: u64) -> String {
        ObjectStoreIo::object_path(self, object_seq)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use common_base::readable_size::ReadableSize;
    use common_error::ext::{ErrorExt, RetryHint};
    use futures::TryStreamExt;
    use object_store::ErrorKind;
    use object_store::services::Memory;
    use tokio::time::timeout;

    use super::*;
    use crate::error::WalObjectStoreSnafu;

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

        store
            .obsolete_all(&provider(region_id), region_id)
            .await
            .unwrap();
        assert!(read(&store, region_id, 1).await.is_empty());
        assert_eq!(3, latest(&store, region_id));
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
    async fn test_store_rejects_invalid_config() {
        for config in [
            config(Duration::from_millis(999), 1),
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

    /// Object access that fails a conditional create on request, either before
    /// or after the object was actually written.
    struct FaultyIo {
        inner: ObjectStoreIo,
        fail_next_put: AtomicBool,
        fail_after_next_put: AtomicBool,
    }

    impl FaultyIo {
        fn new() -> Self {
            Self {
                inner: ObjectStoreIo::new(memory_store(), PREFIX).unwrap(),
                fail_next_put: AtomicBool::new(false),
                fail_after_next_put: AtomicBool::new(false),
            }
        }
    }

    fn injected_failure<T>(path: String) -> Result<T> {
        Err(object_store::Error::new(ErrorKind::Unexpected, "injected failure").set_temporary())
            .context(WalObjectStoreSnafu {
                operation: "write",
                path,
            })
    }

    #[async_trait::async_trait]
    impl WalObjectIo for FaultyIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            if self.fail_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure(self.inner.object_path(object_seq));
            }
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if self.fail_after_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure(self.inner.object_path(object_seq));
            }
            Ok(result)
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
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
            let (gates, gate_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(memory_store(), PREFIX).unwrap(),
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
                injected_failure(self.inner.object_path(object_seq))
            }
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }
}

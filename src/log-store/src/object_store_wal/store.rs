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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use bytes::Bytes;
use common_telemetry::{debug, warn};
use common_wal::config::object_store::{AckMode, CorruptedSegmentAction, ObjectStoreWalConfig};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::{ObjectStoreProvider, Provider};
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

use crate::error::{
    CorruptedWalObjectSnafu, Error, InvalidProviderSnafu, InvalidWalEntrySnafu,
    InvalidWalObjectSnafu, InvalidWalObjectStoreSnafu, MismatchedWalPrefixSnafu,
    ObjectStoreWalSnafu, ObjectStoreWalStoppedSnafu, Result, WalObjectHistoryGapSnafu,
    WalObjectSequenceExhaustedSnafu, WalObjectSequenceUnsettledSnafu,
};
use crate::metrics::{
    METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS, METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL,
    METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL, METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL,
    METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL, METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL,
    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES, METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS,
    METRIC_OBJECT_STORE_WAL_OBJECT_BYTES, METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES,
    METRIC_OBJECT_STORE_WAL_POISONED_TOTAL, METRIC_OBJECT_STORE_WAL_READ_SECONDS,
    METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL, METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS,
    METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS,
    METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL, METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
    METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL,
};
use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, OpenBatch, sequence_floor};
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{
    EncodedObject, FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN, Record,
    TRAILER_LEN, decode_footer, decode_header, decode_segment, decode_trailer, encode_object,
    footer_range, object_len, verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
/// Number of conditional creates that run at a time.
const MAX_IN_FLIGHT_CREATES: usize = 4;
/// Number of object deletes the collections run at a time. A collection takes
/// no more candidates than this leaves free and leaves the rest to the next
/// one, so the first collection on a prefix that accumulated a large WAL
/// costs a bounded number of requests.
const MAX_IN_FLIGHT_DELETES: usize = 4;
/// Number of objects one collection inspects in the catalog before it stops
/// and leaves a cursor for the next one. It bounds the work a collection does
/// on the actor whatever the prefix holds: a pass costs a range over at most
/// this many footers, and nothing it reads besides them grows with the
/// prefix. It is wide enough to cross a stretch of objects that are all
/// retained in few passes, which matters because such a pass accepts no
/// candidate and so schedules nothing of its own: it is advanced only by the
/// passes an `obsolete` or a delete already in flight that succeeds starts.
const DELETE_SCAN_LIMIT: usize = 1024;
/// Delay before a create that failed transiently is attempted again in the
/// `enqueued` acknowledgement mode, where no caller is left to retry it.
const CREATE_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Number of objects whose footers recovery fetches at a time.
const RECOVERY_CONCURRENCY: usize = 8;
/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare.
const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;

/// A [`LogStore`] that persists the entries of many regions as immutable
/// objects under one prefix.
///
/// Appends are admitted into an open batch. A background actor seals the batch
/// when it reaches the size limit or the flush interval elapses and creates
/// the object under the next sequence while it keeps admitting entries into
/// the next batch; up to [`MAX_IN_FLIGHT_CREATES`] creates run at a time.
/// Objects are indexed in the catalog and their batches acknowledged in
/// sequence order, so an acknowledged entry never has a missing predecessor.
/// In the `durable` acknowledgement mode an append returns once its object is
/// indexed; in the `enqueued` mode it returns on admission and the object is
/// created in the background. Reads fetch and decode only the segment of the
/// requested region from every object the catalog lists for it; a segment
/// that does not decode is handled as `on_corrupted_segment` says. After an
/// obsolete watermark moves, the actor deletes the objects whose every
/// segment is at or below the watermark of its region, except the object with
/// the highest sequence, see [`ObjectCatalog::deletable_objects`].
pub struct ObjectStoreLogStore {
    prefix: String,
    ack_mode: AckMode,
    on_corrupted_segment: CorruptedSegmentAction,
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    /// Largest obsolete entry id per region, which reads hide and garbage
    /// collection deletes objects below.
    obsolete_entry_ids: ObsoleteEntryIds,
    /// Segments that reads skipped, per region and by object sequence.
    wal_holes: WalHoles,
    /// Set once the store hit an error it cannot recover from, such as a
    /// conflicting object; every operation fails with it afterwards.
    terminal_error: TerminalError,
    /// Objects a collection is deleting, which a read consults, see
    /// [`is_collected`].
    deleting: Arc<DeletingObjects>,
    /// Set by [`stop`](LogStore::stop) before the actor is told to exit.
    stopped: Arc<AtomicBool>,
    command_tx: mpsc::Sender<Command>,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Sender<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
}

type TerminalError = Arc<Mutex<Option<Arc<Error>>>>;

/// The delete attempts a collection has in flight, one per object.
///
/// The actor registers an attempt before its delete starts, unindexes the
/// object if the delete succeeded, and only then settles the attempt and takes
/// it out, so an object without an attempt was either never collected or is
/// already unindexed. [`is_collected`] depends on that order.
///
/// A read holds the handle of the attempt it observed rather than the sequence
/// of the object, so an attempt that settles releases the reads that were
/// waiting for it even when the same object is already being retried: the
/// retry is a different attempt with a handle of its own.
#[derive(Debug, Default)]
struct DeletingObjects {
    /// The settlement flag of the attempt in flight per object sequence.
    attempts: Mutex<BTreeMap<u64, watch::Sender<bool>>>,
    /// Parks a read between the two observations of [`is_collected`], so a
    /// test can decide what happens in that window.
    #[cfg(any(test, feature = "testing"))]
    read_gap: Mutex<Option<mpsc::UnboundedSender<oneshot::Sender<()>>>>,
}

/// The completion of one delete attempt, taken together with the lookup that
/// found it so that it names that attempt alone.
#[derive(Debug)]
struct AttemptHandle(watch::Receiver<bool>);

impl AttemptHandle {
    /// Returns once the attempt has settled: its delete succeeded or failed,
    /// or the actor exited and abandoned it. A sender dropped without a
    /// settlement ends the wait as well, so no read is left behind.
    async fn settled(mut self) {
        let _ = self.0.wait_for(|settled| *settled).await;
    }
}

impl DeletingObjects {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, watch::Sender<bool>>> {
        self.attempts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a delete attempt for `object_seq` and returns whether it was
    /// registered, which it is not while an earlier attempt is in flight.
    fn start(&self, object_seq: u64) -> bool {
        let mut attempts = self.lock();
        if attempts.contains_key(&object_seq) {
            return false;
        }
        let (settled, _) = watch::channel(false);
        attempts.insert(object_seq, settled);
        true
    }

    /// Settles the attempt of `object_seq` and takes it out, releasing the
    /// reads that hold its handle. The actor calls this after a delete that
    /// succeeded unindexed the object and after one that failed left it
    /// indexed, so a read that wakes up reads the outcome from the catalog.
    fn finish(&self, object_seq: u64) {
        if let Some(settled) = self.lock().remove(&object_seq) {
            settled.send_replace(true);
        }
    }

    /// Settles and takes out every attempt in flight, which the actor does
    /// when it exits with deletes it will never complete: their objects stay
    /// present and indexed, and the reads waiting for them report the error
    /// they met instead of waiting for a collection that will not happen.
    fn abandon_all(&self) {
        for (_, settled) in std::mem::take(&mut *self.lock()) {
            settled.send_replace(true);
        }
    }

    /// Returns the handle of the attempt in flight for `object_seq`, taken
    /// together with the lookup, or `None` when no delete of it is in flight.
    fn attempt(&self, object_seq: u64) -> Option<AttemptHandle> {
        self.lock()
            .get(&object_seq)
            .map(|settled| AttemptHandle(settled.subscribe()))
    }

    /// Returns how many deletes are in flight.
    fn len(&self) -> usize {
        self.lock().len()
    }

    /// Waits until no delete is in flight. New attempts can only come from a
    /// collection, which an `obsolete` starts, so a caller that made none is
    /// answered once the attempts it waited for have settled.
    #[cfg(any(test, feature = "testing"))]
    async fn wait_until_empty(&self) {
        loop {
            let in_flight = self
                .lock()
                .values()
                .map(|settled| AttemptHandle(settled.subscribe()))
                .collect::<Vec<_>>();
            if in_flight.is_empty() {
                return;
            }
            for attempt in in_flight {
                attempt.settled().await;
            }
        }
    }

    /// Installs the gap a read passes between its two observations and
    /// returns the parked reads. Each is released by answering its sender.
    #[cfg(any(test, feature = "testing"))]
    fn hold_read_gap(&self) -> mpsc::UnboundedReceiver<oneshot::Sender<()>> {
        let (gap, parked) = mpsc::unbounded_channel();
        *self.read_gap.lock().unwrap_or_else(PoisonError::into_inner) = Some(gap);
        parked
    }

    /// Parks the caller if a test installed the gap.
    #[cfg(any(test, feature = "testing"))]
    async fn pass_read_gap(&self) {
        let gap = self
            .read_gap
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(gap) = gap {
            let (release, released) = oneshot::channel();
            if gap.send(release).is_ok() {
                let _ = released.await;
            }
        }
    }
}
type ObsoleteEntryIds = Arc<Mutex<HashMap<RegionId, EntryId>>>;
type WalHoles = Arc<Mutex<HashMap<RegionId, BTreeMap<u64, WalHole>>>>;

/// A segment a read skipped because it did not decode: the entries of one
/// region in one object are missing from every read of that region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHole {
    /// Key of the object that holds the segment.
    pub path: String,
    pub object_seq: u64,
    /// Entry id range the footer records for the segment.
    pub min_entry_id: EntryId,
    pub max_entry_id: EntryId,
}

impl fmt::Debug for ObjectStoreLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreLogStore")
            .field("prefix", &self.prefix)
            .field("ack_mode", &self.ack_mode)
            .field("on_corrupted_segment", &self.on_corrupted_segment)
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
        let max_batch_bytes = positive_bytes(config.max_batch_bytes.as_bytes(), "max batch bytes")?;
        let max_unpersisted_bytes = positive_bytes(
            config.max_unpersisted_bytes.as_bytes(),
            "max unpersisted bytes",
        )?;
        ensure!(
            config.max_unpersisted_age > Duration::ZERO,
            InvalidWalObjectStoreSnafu {
                reason: "max unpersisted age is zero",
            }
        );

        let (catalog, next_object_seq, durable_entry_ids) = recover(io.as_ref()).await?;
        let catalog = Arc::new(RwLock::new(catalog));
        let obsolete_entry_ids = ObsoleteEntryIds::default();
        let deleting = DeletingObjects::new();
        let wal_holes = WalHoles::default();
        let terminal_error = TerminalError::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        #[cfg(any(test, feature = "testing"))]
        let (admitted_appends_tx, admitted_appends_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (creates_held_tx, creates_held_rx) = watch::channel(false);
        #[cfg(any(test, feature = "testing"))]
        let creates_fail = Arc::new(AtomicBool::new(false));

        let actor = Actor {
            io: io.clone(),
            catalog: catalog.clone(),
            obsolete_entry_ids: obsolete_entry_ids.clone(),
            terminal_error: terminal_error.clone(),
            stopped: stopped.clone(),
            command_rx,
            ack_mode: config.ack_mode,
            max_unpersisted_bytes,
            max_unpersisted_age: config.max_unpersisted_age,
            open_batch: OpenBatch::new(max_batch_bytes),
            issued_entry_ids: durable_entry_ids,
            pending: Vec::new(),
            sealed: VecDeque::new(),
            creates: FuturesUnordered::new(),
            draining: false,
            stalled: VecDeque::new(),
            durable_waiters: Vec::new(),
            stop: Vec::new(),
            stop_error: None,
            next_object_seq: Some(next_object_seq),
            unresolved_object_seq: None,
            deleting: deleting.clone(),
            collect_cursor: 0,
            deletes: FuturesUnordered::new(),
            writer_instance: uuid::Uuid::new_v4().into_bytes(),
            flush_interval: config.flush_interval,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail: creates_fail.clone(),
        };
        common_runtime::spawn_global(actor.run());

        Ok(Arc::new(Self {
            prefix: config.prefix.clone(),
            ack_mode: config.ack_mode,
            on_corrupted_segment: config.on_corrupted_segment,
            io,
            catalog,
            obsolete_entry_ids,
            deleting,
            wal_holes,
            terminal_error,
            stopped,
            command_tx,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail,
        }))
    }

    /// Returns the largest entry id of the provider's region whose object is
    /// durable and indexed, or zero for a region without such entries. In the
    /// `enqueued` acknowledgement mode an entry id that an append returned
    /// stays above this value until the object holding it is created.
    pub fn durable_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        Ok(self.durable_entry_id_of(region_id))
    }

    fn durable_entry_id_of(&self, region_id: RegionId) -> EntryId {
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        catalog.region_max_entry_id(region_id).unwrap_or(0)
    }

    /// Returns the segments of the provider's region that reads of this store
    /// skipped because they did not decode, ordered by object sequence. Holes
    /// are kept in memory only: a store opened later on the same prefix
    /// learns of them again from the reads that meet them.
    pub fn wal_holes(&self, provider: &Provider) -> Result<Vec<WalHole>> {
        let region_id = self.region_of(provider)?;
        Ok(self
            .wal_holes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&region_id)
            .map(|holes| holes.values().cloned().collect())
            .unwrap_or_default())
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

    /// Checks that the store is healthy and that `provider` selects this
    /// store's prefix and `region_id`.
    fn check_region(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_terminal()?;
        let provider_region = self.region_of(provider)?;
        ensure!(
            provider_region == region_id,
            InvalidWalEntrySnafu {
                region_id,
                reason: format!("provider belongs to region {provider_region}"),
            }
        );
        Ok(())
    }
}

/// Records the obsolete watermark of `region_id`, which never moves down.
fn record_obsolete(obsolete_entry_ids: &ObsoleteEntryIds, region_id: RegionId, entry_id: EntryId) {
    obsolete_entry_ids
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(region_id)
        .and_modify(|current| *current = (*current).max(entry_id))
        .or_insert(entry_id);
}

fn positive_bytes(bytes: u64, name: &str) -> Result<usize> {
    usize::try_from(bytes)
        .ok()
        .filter(|bytes| *bytes > 0)
        .with_context(|| InvalidWalObjectStoreSnafu {
            reason: format!("{name} {bytes} is zero or too large"),
        })
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

    /// Seals the open batch regardless of its size and age and returns once
    /// its object is durable and indexed, or with the error that failed it.
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

    /// Parks every conditional create that starts from now on until
    /// [`release_creates`](Self::release_creates), so a test can observe
    /// entries that are admitted but not durable. A create that is parked when
    /// the store is dropped never runs.
    pub fn hold_creates(&self) {
        self.creates_held.send_replace(true);
    }

    /// Lets the creates parked by [`hold_creates`](Self::hold_creates) run.
    pub fn release_creates(&self) {
        self.creates_held.send_replace(false);
    }

    /// Makes every create that runs from now on fail with a transient object
    /// store error instead of writing, so a test can lose a backlog.
    pub fn fail_creates(&self) {
        self.creates_fail.store(true, Ordering::Release);
    }

    /// Sets the stopped flag without sending the stop command, which is the
    /// state a store is in between the two steps of [`stop`](LogStore::stop).
    pub fn begin_stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// Waits until no object delete is in flight, so a test can observe the
    /// objects a collection left. A collection runs when `obsolete` succeeds
    /// and its deletes are in flight by the time that call returns.
    pub async fn wait_for_garbage_collection(&self) {
        self.deleting.wait_until_empty().await;
    }

    /// Parks every read that reaches the gap between the two observations of
    /// a collected object, so a test can decide what happens in that window,
    /// and returns the parked reads. Each is released by answering its sender.
    pub fn hold_read_gap(&self) -> mpsc::UnboundedReceiver<oneshot::Sender<()>> {
        self.deleting.hold_read_gap()
    }

    /// Returns the key of the object `object_seq` and the byte range the
    /// segment of the provider's region occupies in it, so a test can damage
    /// that segment alone, or `None` when the object holds no segment of the
    /// region.
    pub fn segment_location(
        &self,
        provider: &Provider,
        object_seq: u64,
    ) -> Result<Option<(String, Range<u64>)>> {
        let region_id = self.region_of(provider)?;
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        Ok(catalog
            .objects_for_entry_range(region_id, 0, EntryId::MAX)?
            .into_iter()
            .find(|(seq, _)| *seq == object_seq)
            .map(|(_, entry)| {
                let start = entry.segment_offset;
                (
                    self.io.object_path(object_seq),
                    start..start + entry.segment_len,
                )
            }))
    }
}

#[async_trait::async_trait]
impl LogStore for ObjectStoreLogStore {
    type Error = Error;

    /// Stops the store. Creates in flight run to completion and acknowledge
    /// their entries if they succeed; in the `enqueued` mode the remaining
    /// backlog is uploaded first, and a failure of that upload is returned.
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
            return response_rx.await.unwrap_or(Ok(()));
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

        let requested_at = Instant::now();
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Append {
                entries,
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        let response = response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?;
        // Only an acknowledgement is timed; an append that failed never got one.
        if self.ack_mode == AckMode::Durable && response.is_ok() {
            METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS
                .observe(requested_at.elapsed().as_secs_f64());
        }
        response
    }

    /// Returns the entries of the provider's region with ids from `entry_id`
    /// on, skipping ids the region has obsoleted. Objects are located through
    /// the catalog, so `index` is not needed. A segment that does not decode
    /// fails the read or is skipped and recorded as a hole, as
    /// `on_corrupted_segment` says; an I/O failure fails the read unless the
    /// object was collected after the read listed it, in which case it is
    /// skipped: every entry it held is at or below a watermark.
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
        let catalog = self.catalog.clone();
        let deleting = self.deleting.clone();
        let provider = provider.clone();
        let on_corrupted_segment = self.on_corrupted_segment;
        let wal_holes = self.wal_holes.clone();
        Ok(Box::pin(try_stream! {
            let started_at = Instant::now();
            for (object_seq, footer_entry) in objects {
                let records = match fetch_segment(io.as_ref(), object_seq, &footer_entry).await {
                    Ok(records) => records,
                    Err(error) if is_collected(&catalog, &deleting, object_seq).await => {
                        debug!(
                            "Skipped WAL object {} (sequence {}), collected after the read listed it: {error}",
                            io.object_path(object_seq),
                            object_seq
                        );
                        continue;
                    }
                    Err(error @ Error::InvalidWalObject { .. })
                        if on_corrupted_segment == CorruptedSegmentAction::Skip =>
                    {
                        skip_segment(&wal_holes, &io, object_seq, &footer_entry, &error);
                        continue;
                    }
                    Err(error) => Err(error)?,
                };
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
            METRIC_OBJECT_STORE_WAL_READ_SECONDS.observe(started_at.elapsed().as_secs_f64());
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

    /// Moves the obsolete watermark of the region up to `entry_id`. In the
    /// `enqueued` acknowledgement mode the watermark never passes the durable
    /// entry id: an entry that is not durable yet is replayed after a crash,
    /// and hiding it would skip it.
    ///
    /// Every id the region is assigned from now on is greater than
    /// `entry_id`, whatever watermark is recorded: a watermark that was
    /// assigned under another prefix, or one whose object this prefix no
    /// longer holds, raises the sequence of the next object above the object
    /// it names. The watermark and the sequence are applied together by the
    /// actor: a call cancelled before its command is queued publishes
    /// neither, while a command already queued still applies both. The call
    /// fails with [`Error::WalObjectSequenceUnsettled`] while the sequence
    /// cannot be raised without skipping a sequence whose create outcome is
    /// unknown. In the `enqueued` mode an `entry_id` this store handed out
    /// needs no floor: it is never handed out again while the store runs.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        self.check_region(provider, region_id)?;
        let watermark = match self.ack_mode {
            AckMode::Durable => entry_id,
            AckMode::Enqueued => entry_id.min(self.durable_entry_id_of(region_id)),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let command = Command::Obsolete {
            region_id,
            entry_id,
            watermark,
            response: response_tx,
        };
        let answered = match self.command_tx.send(command).await {
            Ok(()) => response_rx.await.ok(),
            Err(_) => None,
        };
        match answered {
            Some(result) => result,
            // The actor exited, before the command was queued or with it
            // still queued behind the stop: nothing is assigned an id any
            // more, so the watermark alone is consistent.
            None => {
                record_obsolete(&self.obsolete_entry_ids, region_id, watermark);
                Ok(())
            }
        }
    }

    async fn obsolete_all(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_region(provider, region_id)?;
        record_obsolete(&self.obsolete_entry_ids, region_id, EntryId::MAX);
        Ok(())
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
        self.durable_entry_id(provider)
    }

    /// Waits until every entry of the provider's region with an id at or
    /// below `entry_id` is durable and indexed. Returns at once in the
    /// `durable` acknowledgement mode, where a caller only holds durable ids.
    async fn wait_durable(&self, provider: &Provider, entry_id: EntryId) -> Result<()> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        if entry_id <= self.durable_entry_id_of(region_id) {
            return Ok(());
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::WaitDurable {
                region_id,
                entry_id,
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }
}

type AppendResponse = oneshot::Sender<Result<AppendBatchResponse>>;

enum Command {
    Append {
        entries: Vec<Entry>,
        response: AppendResponse,
    },
    /// Answered once the region is durable through `entry_id`.
    WaitDurable {
        region_id: RegionId,
        entry_id: EntryId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Answered once the watermark is recorded and no id of the region at
    /// or below `entry_id` can be assigned, or with the reason neither was
    /// done.
    Obsolete {
        region_id: RegionId,
        entry_id: EntryId,
        watermark: EntryId,
        response: oneshot::Sender<Result<()>>,
    },
    Stop {
        response: oneshot::Sender<Result<()>>,
    },
    #[cfg(any(test, feature = "testing"))]
    Seal {
        response: oneshot::Sender<Result<()>>,
    },
}

/// An append waiting for the object that holds its entries.
struct PendingAppend {
    last_entry_ids: HashMap<RegionId, EntryId>,
    response: AppendResponse,
}

/// A caller waiting until a region is durable through an entry id.
struct DurableWaiter {
    region_id: RegionId,
    entry_id: EntryId,
    response: oneshot::Sender<Result<()>>,
}

/// Where the conditional create of a sealed batch stands.
enum CreateState {
    /// The create has not started: no slot was free, or in the `enqueued`
    /// mode an earlier attempt failed transiently and is repeated.
    Pending,
    InFlight,
    Created,
    /// The create failed transiently and is not repeated.
    Failed(Arc<Error>),
}

/// A sealed and encoded batch that holds its object sequence and waits to be
/// created, indexed and acknowledged.
struct SealedBatch {
    object_seq: u64,
    bytes: Bytes,
    footer: Vec<FooterEntry>,
    first_admitted_at: Instant,
    sealed_at: Instant,
    /// Number of creates that were attempted for the batch.
    attempts: u32,
    waiters: Vec<PendingAppend>,
    #[cfg(any(test, feature = "testing"))]
    seal_waiters: Vec<oneshot::Sender<Result<()>>>,
    state: CreateState,
}

impl SealedBatch {
    fn is_in_flight(&self) -> bool {
        matches!(self.state, CreateState::InFlight)
    }

    fn fail(self, error: impl Fn() -> Error) {
        for waiter in self.waiters {
            let _ = waiter.response.send(Err(error()));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in self.seal_waiters {
            let _ = waiter.send(Err(error()));
        }
    }
}

type CreateOutcome = (u64, Instant, Result<PutResult>);
type DeleteOutcome = (u64, Result<()>);

/// The actor that owns the open batch and the sealed batches until they are
/// durable.
///
/// Sealed batches form a pipeline in sequence order. A batch is created under
/// its sequence as soon as one of [`MAX_IN_FLIGHT_CREATES`] slots is free, but
/// it is indexed and acknowledged only once every earlier batch is, so the
/// acknowledged history of a region never has a missing predecessor. The
/// outcomes of a create are:
///
/// | Situation | Sequence | Waiters | Store |
/// | --- | --- | --- | --- |
/// | created, or identical retry | advances | acknowledged in order | healthy |
/// | transient error, `durable` mode, no later object created | rolls back to the failed batch | this and every later batch fail; entry ids roll back to the durable watermark | healthy |
/// | transient error, `durable` mode, a later object is durable | unchanged | this and every later batch fail | poisoned: the later object cannot be rolled back, and its entries were never acknowledged |
/// | transient error, `enqueued` mode | unchanged | already acknowledged | healthy: the create is repeated after [`CREATE_RETRY_DELAY`] |
/// | transient error, `enqueued` mode after `stop` began | unchanged | already acknowledged | the backlog is dropped and `stop` reports the error |
/// | conflicting object, encoding or catalog error | unchanged | every batch that is not durable fails | poisoned |
/// | created at the last representable sequence | cannot advance | acknowledged | poisoned: no later batch can be allocated a sequence |
///
/// A transient error stops new creates from starting until every create in
/// flight has completed, because only then is it known whether a later object
/// exists. After `stop` began nothing is admitted and no create starts, except
/// that the `enqueued` mode uploads its backlog; creates in flight run to
/// completion and acknowledge if they succeed.
///
/// The actor also collects garbage: after an obsolete watermark is recorded
/// it deletes, in the background, the objects the catalog and the watermarks
/// allow, and unindexes each once its delete succeeded. A failed delete is
/// counted and repeated at the next collection; deletes in flight when stop
/// begins run to completion, and no collection starts afterwards.
struct Actor {
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    command_rx: mpsc::Receiver<Command>,
    ack_mode: AckMode,
    max_unpersisted_bytes: usize,
    max_unpersisted_age: Duration,
    open_batch: OpenBatch,
    /// Largest entry id ever handed out per region, whether it became
    /// durable, was rolled back or was lost with a dropped backlog. Unlike
    /// the accepted ids of the open batch it never moves down.
    issued_entry_ids: HashMap<RegionId, EntryId>,
    /// Waiters of the open batch in the `durable` mode.
    pending: Vec<PendingAppend>,
    /// Batches that are not durable yet, in sequence order.
    sealed: VecDeque<SealedBatch>,
    creates: FuturesUnordered<BoxFuture<'static, CreateOutcome>>,
    /// Set by a transient failure that is not repeated: no create starts until
    /// every create in flight has completed.
    draining: bool,
    /// Appends held back in the `enqueued` mode while the unpersisted backlog
    /// is at a threshold, with the instant they were held back, in arrival
    /// order.
    stalled: VecDeque<(Vec<Entry>, AppendResponse, Instant)>,
    durable_waiters: Vec<DurableWaiter>,
    /// Callers of `stop`, answered once nothing is in flight.
    stop: Vec<oneshot::Sender<Result<()>>>,
    /// The failure `stop` reports in the `enqueued` mode once an
    /// acknowledged backlog was dropped, recorded when it happens.
    stop_error: Option<Arc<Error>>,
    /// Sequence of the next sealed batch, `None` once the sequence is
    /// exhausted. The open batch assigns its entry ids from it.
    next_object_seq: Option<u64>,
    /// Largest sequence whose batch failed transiently and was rolled back:
    /// its object may exist. The sequences from the next one up to it are
    /// reused in order, so the conditional create at each reconciles it; no
    /// sequence is skipped until an object at or above it is indexed.
    unresolved_object_seq: Option<u64>,
    /// Objects whose delete is in flight, so a collection does not delete
    /// one twice and a read can wait for the outcome of one it met, see
    /// [`is_collected`].
    deleting: Arc<DeletingObjects>,
    /// Sequence the next collection starts its scan at, see
    /// [`collect_garbage`](Self::collect_garbage).
    collect_cursor: u64,
    deletes: FuturesUnordered<BoxFuture<'static, DeleteOutcome>>,
    writer_instance: [u8; 16],
    flush_interval: Duration,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Receiver<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
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
                    // Nothing starts after stop began; the `enqueued` mode
                    // sealed its backlog when stop was requested.
                    if !self.is_stopped() {
                        self.flush_open_batch();
                    }
                }
                Some((object_seq, sealed_at, result)) = self.creates.next(), if !self.creates.is_empty() => {
                    self.on_create_completed(object_seq, sealed_at, result);
                }
                Some((object_seq, result)) = self.deletes.next(), if !self.deletes.is_empty() => {
                    self.on_delete_completed(object_seq, result);
                }
                command = self.command_rx.recv() => match command {
                    Some(Command::Append { entries, response }) => {
                        self.handle_append(entries, response);
                    }
                    Some(Command::WaitDurable { region_id, entry_id, response }) => {
                        self.handle_wait_durable(region_id, entry_id, response);
                    }
                    Some(Command::Obsolete { region_id, entry_id, watermark, response }) => {
                        self.handle_obsolete(region_id, entry_id, watermark, response);
                    }
                    Some(Command::Stop { response }) => {
                        self.handle_stop(response);
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Seal { response }) => {
                        self.handle_seal(response);
                    }
                    // Every sender is gone: the store was dropped without
                    // `stop`. The creates in flight are dropped with the
                    // actor, and so are the deletes, whose attempts the
                    // teardown settles.
                    None => return,
                },
            }
            if self.finish_stop() {
                return;
            }
        }
    }

    fn handle_append(&mut self, entries: Vec<Entry>, response: AppendResponse) {
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
        if self.ack_mode == AckMode::Enqueued && self.backlog_at_threshold() {
            METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.inc();
            self.stalled.push_back((entries, response, Instant::now()));
            self.ensure_create_in_flight();
            return;
        }
        self.admit(entries, response);
    }

    /// Admits `entries` into the open batch, which assigns their ids under
    /// the next object sequence. In the `durable` mode the caller waits for
    /// the object, in the `enqueued` mode it is answered now.
    fn admit(&mut self, entries: Vec<Entry>, response: AppendResponse) {
        // A region would run past the position range of the open batch: the
        // batch is sealed and the entries open the next one. The size limit
        // seals a batch long before a million entries of one region, so this
        // is a theoretical bound.
        if !self.open_batch.is_empty() && self.open_batch.would_exhaust_positions(&entries) {
            self.flush_open_batch();
            if let Some(error) = terminal(&self.terminal_error) {
                let _ = response.send(Err(shared(&error)));
                return;
            }
        }
        let Some(object_seq) = self.next_object_seq else {
            let error = self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            let _ = response.send(Err(shared(&error)));
            return;
        };
        let last_entry_ids = match self.open_batch.admit(object_seq, entries) {
            Ok(last_entry_ids) => last_entry_ids,
            // Nothing was admitted: the append alone runs past the position
            // range, which no object can hold.
            Err(error) => {
                let _ = response.send(Err(error));
                return;
            }
        };
        for (region_id, entry_id) in &last_entry_ids {
            self.issued_entry_ids
                .entry(*region_id)
                .and_modify(|issued| *issued = (*issued).max(*entry_id))
                .or_insert(*entry_id);
        }
        match self.ack_mode {
            AckMode::Durable => self.pending.push(PendingAppend {
                last_entry_ids,
                response,
            }),
            AckMode::Enqueued => {
                let _ = response.send(Ok(AppendBatchResponse { last_entry_ids }));
            }
        }
        #[cfg(any(test, feature = "testing"))]
        self.admitted_appends.send_modify(|count| *count += 1);
        if self.open_batch.should_seal() {
            self.flush_open_batch();
        }
    }

    /// Returns true once the unpersisted backlog, the open batch and every
    /// sealed batch that is not durable, reaches the size or the age threshold.
    fn backlog_at_threshold(&self) -> bool {
        let bytes = self.open_batch.estimated_bytes()
            + self
                .sealed
                .iter()
                .map(|batch| batch.bytes.len())
                .sum::<usize>();
        if bytes >= self.max_unpersisted_bytes {
            return true;
        }
        let oldest = self
            .sealed
            .front()
            .map(|batch| batch.first_admitted_at)
            .or_else(|| self.open_batch.first_admitted_at());
        oldest.is_some_and(|admitted_at| admitted_at.elapsed() >= self.max_unpersisted_age)
    }

    /// Seals the open batch when no create is in flight, so that a stalled
    /// append has an upload to wait for.
    fn ensure_create_in_flight(&mut self) {
        if !self.sealed.iter().any(SealedBatch::is_in_flight) {
            self.flush_open_batch();
        }
    }

    /// Admits the stalled appends in arrival order while the backlog is
    /// below the thresholds.
    fn release_stalled(&mut self) {
        while !self.stalled.is_empty() {
            if self.is_stopped() {
                for (_, response, stalled_at) in self.stalled.drain(..) {
                    observe_stalled(stalled_at);
                    let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
                }
                return;
            }
            if self.backlog_at_threshold() {
                // The remaining waiters need an upload to complete.
                self.ensure_create_in_flight();
                return;
            }
            let Some((entries, response, stalled_at)) = self.stalled.pop_front() else {
                return;
            };
            observe_stalled(stalled_at);
            self.admit(entries, response);
        }
    }

    /// Seals the open batch as the object `next_object_seq` and starts its
    /// create when a slot is free. Returns whether a batch was sealed.
    fn flush_open_batch(&mut self) -> bool {
        if self.open_batch.is_empty() {
            return false;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            self.fail_unacknowledged(|| shared(&error));
            return false;
        }
        let Some(object_seq) = self.next_object_seq else {
            self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            return false;
        };

        let (entries, first_admitted_at) = self.open_batch.seal();
        let entry_count = entries.len();
        let encoded = match encode_batch(object_seq, self.writer_instance, entries) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.poison(error);
                return false;
            }
        };
        METRIC_OBJECT_STORE_WAL_OBJECT_BYTES.observe(encoded.bytes.len() as f64);
        METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES.observe(entry_count as f64);
        self.next_object_seq = object_seq
            .checked_add(1)
            .filter(|next_object_seq| *next_object_seq < OBJECT_SEQ_LIMIT);
        if self.next_object_seq.is_none() {
            // The batch takes the last representable sequence: it is created
            // and acknowledged, but no later batch can be allocated one.
            set_terminal(
                &self.terminal_error,
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: object_seq,
                }
                .build(),
            );
        }
        self.sealed.push_back(SealedBatch {
            object_seq,
            bytes: encoded.bytes,
            footer: encoded.footer,
            first_admitted_at,
            sealed_at: Instant::now(),
            attempts: 0,
            waiters: std::mem::take(&mut self.pending),
            #[cfg(any(test, feature = "testing"))]
            seal_waiters: Vec::new(),
            state: CreateState::Pending,
        });
        self.start_creates();
        true
    }

    /// Starts the creates of pending batches in sequence order while fewer
    /// than [`MAX_IN_FLIGHT_CREATES`] are in flight. Nothing starts once
    /// stop began, except the backlog of the `enqueued` mode.
    fn start_creates(&mut self) {
        if self.draining || (self.is_stopped() && self.ack_mode == AckMode::Durable) {
            return;
        }
        let mut in_flight = self
            .sealed
            .iter()
            .filter(|batch| batch.is_in_flight())
            .count();
        for batch in self.sealed.iter_mut() {
            if in_flight >= MAX_IN_FLIGHT_CREATES {
                break;
            }
            if !matches!(batch.state, CreateState::Pending) {
                continue;
            }
            batch.state = CreateState::InFlight;
            in_flight += 1;
            let delay = if batch.attempts == 0 {
                Duration::ZERO
            } else {
                CREATE_RETRY_DELAY
            };
            batch.attempts += 1;
            let io = self.io.clone();
            let object_seq = batch.object_seq;
            let sealed_at = batch.sealed_at;
            let bytes = batch.bytes.clone();
            #[cfg(any(test, feature = "testing"))]
            let mut creates_held = self.creates_held.clone();
            #[cfg(any(test, feature = "testing"))]
            let creates_fail = self.creates_fail.clone();
            self.creates.push(Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                #[cfg(any(test, feature = "testing"))]
                let _ = creates_held.wait_for(|held| !*held).await;
                #[cfg(any(test, feature = "testing"))]
                if creates_fail.load(Ordering::Acquire) {
                    let error = object_store::Error::new(
                        object_store::ErrorKind::Unexpected,
                        "injected create failure",
                    )
                    .set_temporary();
                    let result = Err(error).context(crate::error::WalObjectStoreSnafu {
                        operation: "write",
                        path: io.object_path(object_seq),
                    });
                    return (object_seq, sealed_at, result);
                }
                (
                    object_seq,
                    sealed_at,
                    io.put_if_absent(object_seq, bytes).await,
                )
            }));
        }
    }

    fn on_create_completed(
        &mut self,
        object_seq: u64,
        sealed_at: Instant,
        result: Result<PutResult>,
    ) {
        // Every create that completes is counted, whether or not its batch is
        // still there to be indexed: a batch the store gave up on when it
        // poisoned itself leaves its create running, and an object it creates
        // is as durable as any other. The sealing instant comes with the
        // outcome for the same reason.
        match &result {
            Ok(_) => {
                METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS
                    .observe(sealed_at.elapsed().as_secs_f64());
                METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL.inc();
            }
            Err(Error::WalObjectStore { .. }) => {
                METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL.inc()
            }
            Err(Error::WalObjectConflict { .. }) => {
                METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL.inc()
            }
            Err(_) => {}
        }
        // A batch the store gave up on when it poisoned itself: the object
        // may exist, but nothing was acknowledged for it.
        let Some(index) = self
            .sealed
            .iter()
            .position(|batch| batch.object_seq == object_seq)
        else {
            return;
        };
        match result {
            Ok(_) => self.sealed[index].state = CreateState::Created,
            // The object store did not confirm the object. A caller of the
            // `durable` mode retries the append itself, so the batch fails
            // once it is known that no later object exists. Nobody is left to
            // retry in the `enqueued` mode, so the store repeats the create.
            Err(error @ Error::WalObjectStore { .. }) => {
                if self.ack_mode == AckMode::Enqueued && !self.is_stopped() {
                    self.sealed[index].state = CreateState::Pending;
                } else {
                    self.sealed[index].state = CreateState::Failed(Arc::new(error));
                    self.draining = true;
                }
            }
            Err(error) => {
                self.poison(error);
                return;
            }
        }
        self.settle();
    }

    /// Indexes and acknowledges the sealed batches from the front as far as
    /// they are created, resolves a failed batch at the front once nothing is
    /// in flight, and starts the creates that a free slot allows.
    fn settle(&mut self) {
        enum Next {
            Wait,
            Index,
            Gap {
                object_seq: u64,
                later_object_seq: u64,
            },
            RollBack {
                object_seq: u64,
                error: Arc<Error>,
            },
        }
        while let Some(front) = self.sealed.front() {
            let next = match &front.state {
                CreateState::Pending | CreateState::InFlight => Next::Wait,
                CreateState::Created => Next::Index,
                CreateState::Failed(error) => {
                    if self.sealed.iter().any(SealedBatch::is_in_flight) {
                        Next::Wait
                    } else if let Some(later) = self
                        .sealed
                        .iter()
                        .skip(1)
                        .find(|batch| matches!(batch.state, CreateState::Created))
                    {
                        Next::Gap {
                            object_seq: front.object_seq,
                            later_object_seq: later.object_seq,
                        }
                    } else {
                        Next::RollBack {
                            object_seq: front.object_seq,
                            error: error.clone(),
                        }
                    }
                }
            };
            match next {
                Next::Wait => break,
                Next::Index => {
                    if !self.index_front() {
                        break;
                    }
                }
                Next::Gap {
                    object_seq,
                    later_object_seq,
                } => {
                    self.poison(
                        WalObjectHistoryGapSnafu {
                            object_seq,
                            later_object_seq,
                        }
                        .build(),
                    );
                    break;
                }
                Next::RollBack { object_seq, error } => {
                    self.roll_back(object_seq, error);
                    break;
                }
            }
        }
        self.start_creates();
        self.release_stalled();
    }

    /// Indexes the created object at the front and acknowledges its waiters.
    /// Returns false when the catalog rejected it, which poisons the store.
    fn index_front(&mut self) -> bool {
        let Some(front) = self.sealed.front() else {
            return false;
        };
        let indexed = self
            .catalog
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert_object(front.object_seq, front.footer.clone());
        if let Err(error) = indexed {
            self.poison(error);
            return false;
        }
        let Some(batch) = self.sealed.pop_front() else {
            return false;
        };
        METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.inc();
        METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.add(batch.bytes.len() as i64);
        for waiter in batch.waiters {
            let _ = waiter.response.send(Ok(AppendBatchResponse {
                last_entry_ids: waiter.last_entry_ids,
            }));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in batch.seal_waiters {
            let _ = waiter.send(Ok(()));
        }
        // Objects are indexed in sequence order, so every rolled back
        // sequence up to this one was written again and reconciled.
        if self
            .unresolved_object_seq
            .is_some_and(|unresolved| unresolved <= batch.object_seq)
        {
            self.unresolved_object_seq = None;
        }
        self.resolve_durable_waiters();
        true
    }

    fn resolve_durable_waiters(&mut self) {
        let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
        let waiters = std::mem::take(&mut self.durable_waiters);
        for waiter in waiters {
            if waiter.entry_id <= catalog.region_max_entry_id(waiter.region_id).unwrap_or(0) {
                let _ = waiter.response.send(Ok(()));
            } else {
                self.durable_waiters.push(waiter);
            }
        }
    }

    /// Drops every batch that is not durable after the batch `object_seq`
    /// failed to be created while no later object was created. Its sequence
    /// stays free and the entry ids are handed out again, so a retry of the
    /// same entries writes the same object. Waiters of a store that was
    /// stopped meanwhile learn that instead of the I/O error, like every
    /// other entry that never became durable.
    fn roll_back(&mut self, object_seq: u64, error: Arc<Error>) {
        self.next_object_seq = Some(object_seq);
        // Every batch that failed may have left an object behind.
        let failed = self
            .sealed
            .iter()
            .filter(|batch| matches!(batch.state, CreateState::Failed(_)))
            .map(|batch| batch.object_seq)
            .max();
        self.unresolved_object_seq = self.unresolved_object_seq.max(failed);
        self.draining = false;
        let stopped = self.is_stopped();
        let failure = || {
            if stopped {
                ObjectStoreWalStoppedSnafu.build()
            } else {
                shared(&error)
            }
        };
        for batch in self.sealed.drain(..) {
            batch.fail(failure);
        }
        self.reset_open_batch();
        self.fail_unacknowledged(failure);
        // An acknowledged backlog was dropped: `stop` reports it, whether
        // its caller has arrived yet or not.
        if self.ack_mode == AckMode::Enqueued {
            self.stop_error.get_or_insert(error);
        }
    }

    /// Records `error` as terminal and fails every waiter that is not
    /// acknowledged with it, or with the stopped error if the store was
    /// stopped meanwhile. Creates in flight run to completion, but their
    /// outcome is ignored: the entries of an object they create were never
    /// acknowledged, like those of a crash between creation and
    /// acknowledgement. Returns the recorded error.
    fn poison(&mut self, error: Error) -> Arc<Error> {
        let error = set_terminal(&self.terminal_error, error);
        self.draining = false;
        let stopped = self.is_stopped();
        let failure = || {
            if stopped {
                ObjectStoreWalStoppedSnafu.build()
            } else {
                shared(&error)
            }
        };
        for batch in self.sealed.drain(..) {
            batch.fail(failure);
        }
        self.reset_open_batch();
        self.fail_unacknowledged(failure);
        if self.ack_mode == AckMode::Enqueued {
            self.stop_error.get_or_insert(error.clone());
        }
        error
    }

    /// Drops the entries of the open batch; the next admission hands out the
    /// same ids again under the sequence the batch is at.
    fn reset_open_batch(&mut self) {
        self.open_batch.reset();
    }

    /// Fails the waiters of the open batch, the stalled appends and the
    /// durability waiters.
    fn fail_unacknowledged(&mut self, error: impl Fn() -> Error) {
        for pending in self.pending.drain(..) {
            let _ = pending.response.send(Err(error()));
        }
        for (_, response, stalled_at) in self.stalled.drain(..) {
            observe_stalled(stalled_at);
            let _ = response.send(Err(error()));
        }
        for waiter in self.durable_waiters.drain(..) {
            let _ = waiter.response.send(Err(error()));
        }
    }

    fn handle_wait_durable(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        response: oneshot::Sender<Result<()>>,
    ) {
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        let durable = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            catalog.region_max_entry_id(region_id).unwrap_or(0)
        };
        if entry_id <= durable {
            let _ = response.send(Ok(()));
            return;
        }
        // An acknowledged backlog was dropped: no entry that is not durable
        // can be certified any more, whether it was in that backlog or not.
        if let Some(error) = &self.stop_error {
            let _ = response.send(Err(shared(error)));
            return;
        }
        // An id this store never handed out is not in its backlog, so there
        // is nothing to wait for. Ids that were handed out and rolled back
        // are waited for like any other, since they are handed out again.
        let issued = self.issued_entry_ids.get(&region_id).copied().unwrap_or(0);
        if entry_id > issued {
            let _ = response.send(Ok(()));
            return;
        }
        self.durable_waiters.push(DurableWaiter {
            region_id,
            entry_id,
            response,
        });
    }

    /// Makes sure no id of the region at or below `entry_id` is assigned
    /// from now on, then records the obsolete watermark of the region and
    /// collects the objects the watermarks release; nothing is done when the
    /// sequence cannot be raised.
    fn handle_obsolete(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        watermark: EntryId,
        response: oneshot::Sender<Result<()>>,
    ) {
        let result = self.raise_sequence_floor(region_id, entry_id).map(|_| {
            record_obsolete(&self.obsolete_entry_ids, region_id, watermark);
        });
        if result.is_ok() {
            self.collect_garbage();
        }
        let _ = response.send(result);
    }

    /// Starts deleting the objects the catalog and the obsolete watermarks
    /// allow, see [`ObjectCatalog::deletable_objects`]. The deletes run in
    /// the background so they never hold up admission, sealing, uploads or
    /// acknowledgements; an object is unindexed once its delete succeeded.
    /// An object has an attempt registered from before its delete starts
    /// until after it was unindexed, so a read that listed it never meets it
    /// as an indexed object the object store has already removed.
    ///
    /// A collection is one bounded pass: it inspects at most
    /// [`DELETE_SCAN_LIMIT`] objects of the catalog and schedules at most
    /// [`MAX_IN_FLIGHT_DELETES`] deletes, the bound creates have. It resumes
    /// at the cursor the last one left and wraps at the object that is
    /// always kept, so a prefix that accumulated a large WAL is swept in
    /// bounded passes, a stretch of objects that are all retained is not
    /// walked again by every pass, and objects whose delete failed are
    /// retried on a later sweep instead of holding up the objects behind
    /// them.
    ///
    /// A pass runs on an `obsolete` and on every delete that succeeded, and
    /// takes no more candidates than it has free slots, so the first pass of
    /// a stretch that yields candidates fills all four and every pass a
    /// completed delete starts refills the one slot it freed, which keeps
    /// four deletes in flight for as long as the stretch lasts. A pass that
    /// accepts nothing schedules nothing of its own and leaves the next one
    /// to the following `obsolete` or to a delete already in flight that
    /// succeeds, and so does a delete that failed. Nothing is collected once stop began or the store is
    /// poisoned.
    fn collect_garbage(&mut self) {
        if self.is_stopped() || terminal(&self.terminal_error).is_some() {
            return;
        }
        let free = MAX_IN_FLIGHT_DELETES.saturating_sub(self.deleting.len());
        if free == 0 {
            return;
        }
        let scan = {
            // The watermarks are read where they are for the length of the
            // pass rather than copied, which a pass must not do: the map
            // holds an entry per region and the pass is bounded. Wherever
            // both are held, the watermarks are taken before the catalog.
            let obsolete_entry_ids = self
                .obsolete_entry_ids
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            catalog.deletable_objects(
                &obsolete_entry_ids,
                self.collect_cursor,
                free,
                DELETE_SCAN_LIMIT,
            )
        };
        self.collect_cursor = scan.resume_from;
        for object_seq in scan.objects {
            // Still in flight from an earlier pass that the cursor wrapped
            // past; the free slots it holds are already accounted for.
            if !self.deleting.start(object_seq) {
                continue;
            }
            let io = self.io.clone();
            self.deletes.push(Box::pin(async move {
                (object_seq, io.delete(object_seq).await)
            }));
        }
    }

    /// Unindexes the object `object_seq` once its delete succeeded, then
    /// settles its attempt; a read that listed the object sees it as
    /// collected throughout, since the object store may have removed it as
    /// soon as the delete started. A delete that succeeded starts the next
    /// pass from the cursor, so a stretch of collectable objects drains at
    /// the rate the deletes complete rather than at the rate watermarks
    /// move. A failed delete leaves the object indexed and starts nothing:
    /// it is deleted again by a later sweep, which a watermark starts, so a
    /// delete that keeps failing is not repeated in a loop.
    fn on_delete_completed(&mut self, object_seq: u64, result: Result<()>) {
        let deleted = result.is_ok();
        match result {
            Ok(()) => {
                let unindexed = self
                    .catalog
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove_object(object_seq);
                if let Some(footer) = unindexed {
                    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.dec();
                    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.sub(object_len(&footer) as i64);
                }
                METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.inc();
            }
            Err(error) => {
                warn!(
                    error;
                    "Failed to delete WAL object {} (sequence {}), it is deleted again at the next collection",
                    self.io.object_path(object_seq),
                    object_seq
                );
                METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.inc();
            }
        }
        // Settled last, so a read that the attempt releases reads the outcome
        // of this delete from the catalog, see [`is_collected`].
        self.deleting.finish(object_seq);
        if deleted {
            self.collect_garbage();
        }
    }

    /// Moves the sequence of the next object above the object that holds
    /// `entry_id` unless it is there already. The move fails while the next
    /// sequence is not settled: the open batch handed out ids under it, a
    /// create is in flight, or a rolled back batch may have left an object
    /// at it. Skipping such a sequence would leave an object that recovery
    /// indexes but this store never did, so the next create at that sequence
    /// has to reconcile it first. A floor that does not fit an entry id
    /// poisons the store like an exhausted sequence.
    fn raise_sequence_floor(&mut self, region_id: RegionId, entry_id: EntryId) -> Result<()> {
        // Nothing is assigned an id after stop began.
        if self.is_stopped() {
            return Ok(());
        }
        if let Some(error) = terminal(&self.terminal_error) {
            return Err(shared(&error));
        }
        // In the `enqueued` mode an id the store handed out is never handed
        // out again while it runs: a create that fails transiently is
        // repeated under its sequence and a permanent failure poisons the
        // store. Every later id of the region is greater, so the id needs no
        // floor even before it is durable.
        if self.ack_mode == AckMode::Enqueued
            && entry_id <= self.issued_entry_ids.get(&region_id).copied().unwrap_or(0)
        {
            return Ok(());
        }
        let sequence_floor = sequence_floor(entry_id);
        let Some(next_object_seq) = self.next_object_seq else {
            return Ok(());
        };
        // A failed create rolls the sequence back to the first batch that is
        // not durable, so ids below the floor are out of reach only when
        // that sequence is at or above it, not the speculative next one.
        let lowest_object_seq = self
            .sealed
            .front()
            .map_or(next_object_seq, |batch| batch.object_seq);
        if lowest_object_seq >= sequence_floor {
            return Ok(());
        }
        if let Some(batch) = self.sealed.front() {
            return WalObjectSequenceUnsettledSnafu {
                object_seq: batch.object_seq,
            }
            .fail();
        }
        ensure!(
            self.open_batch.is_empty()
                && self
                    .unresolved_object_seq
                    .is_none_or(|unresolved| unresolved < next_object_seq),
            WalObjectSequenceUnsettledSnafu {
                object_seq: next_object_seq,
            }
        );
        if sequence_floor < OBJECT_SEQ_LIMIT {
            self.next_object_seq = Some(sequence_floor);
            Ok(())
        } else {
            self.next_object_seq = None;
            let error = self.poison(
                WalObjectSequenceExhaustedSnafu {
                    last_object_seq: OBJECT_SEQ_LIMIT - 1,
                }
                .build(),
            );
            Err(shared(&error))
        }
    }

    /// Begins stopping. Nothing is admitted from now on; the `durable` mode
    /// drops the open batch and the batches whose create has not started,
    /// the `enqueued` mode seals its backlog so it is uploaded. Stop is
    /// answered by [`finish_stop`](Self::finish_stop) once nothing is in flight.
    fn handle_stop(&mut self, response: oneshot::Sender<Result<()>>) {
        self.stop.push(response);
        match self.ack_mode {
            AckMode::Durable => {
                // The entries are dropped; the ids stay handed out, so a
                // durability wait for a batch in flight is still answered
                // by its create.
                let _ = self.open_batch.seal();
                for pending in self.pending.drain(..) {
                    let _ = pending
                        .response
                        .send(Err(ObjectStoreWalStoppedSnafu.build()));
                }
                if let Some(index) = self
                    .sealed
                    .iter()
                    .position(|batch| matches!(batch.state, CreateState::Pending))
                {
                    self.next_object_seq = Some(self.sealed[index].object_seq);
                    for batch in self.sealed.drain(index..) {
                        batch.fail(|| ObjectStoreWalStoppedSnafu.build());
                    }
                }
            }
            AckMode::Enqueued => {
                for (_, response, stalled_at) in self.stalled.drain(..) {
                    observe_stalled(stalled_at);
                    let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
                }
                self.flush_open_batch();
            }
        }
    }

    /// Answers the callers of `stop` once every sealed batch is settled and
    /// every create and delete has completed. Returns true when the actor is
    /// done.
    fn finish_stop(&mut self) -> bool {
        if self.stop.is_empty()
            || !self.sealed.is_empty()
            || !self.creates.is_empty()
            || !self.deletes.is_empty()
        {
            return false;
        }
        for waiter in self.durable_waiters.drain(..) {
            let _ = waiter
                .response
                .send(Err(ObjectStoreWalStoppedSnafu.build()));
        }
        let error = self.stop_error.take();
        for response in self.stop.drain(..) {
            let _ = response.send(error.as_ref().map_or(Ok(()), |error| Err(shared(error))));
        }
        true
    }

    #[cfg(any(test, feature = "testing"))]
    fn handle_seal(&mut self, response: oneshot::Sender<Result<()>>) {
        if self.is_stopped() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if self.flush_open_batch()
            && let Some(batch) = self.sealed.back_mut()
        {
            batch.seal_waiters.push(response);
            return;
        }
        let result = terminal(&self.terminal_error).map_or(Ok(()), |error| Err(shared(&error)));
        let _ = response.send(result);
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Drop for Actor {
    /// Settles every delete the actor still had in flight. The read streams
    /// of the store outlive it, and one of them may be waiting for an attempt
    /// this actor will never complete, either because the store was dropped
    /// without `stop` or because the runtime dropped the task. Their objects
    /// stay present and indexed, so the reads report the error they met.
    /// `stop` settles its deletes by completing them, and leaves none here.
    ///
    /// An append still held back when the actor is torn down leaves the queue
    /// here, with its caller gone or about to be, so the wait it spent there
    /// is recorded like the wait of one released any other way.
    fn drop(&mut self) {
        self.deleting.abandon_all();
        for (_, _, stalled_at) in self.stalled.drain(..) {
            observe_stalled(stalled_at);
        }
    }
}

/// Records how long an append was held back by the backlog thresholds, which
/// every path out of the stalled queue does once, the teardown of the actor
/// included: an append released by a stop or by a terminal error waited
/// longest of all.
fn observe_stalled(stalled_at: Instant) {
    METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.observe(stalled_at.elapsed().as_secs_f64());
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
    let mut terminal_error = terminal_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if terminal_error.is_none() {
        METRIC_OBJECT_STORE_WAL_POISONED_TOTAL.inc();
    }
    terminal_error
        .get_or_insert_with(|| Arc::new(error))
        .clone()
}

/// Wraps an error that several callers receive.
fn shared(error: &Arc<Error>) -> Error {
    ObjectStoreWalSnafu.into_error(error.clone())
}

fn is_indexed(catalog: &RwLock<ObjectCatalog>, object_seq: u64) -> bool {
    catalog
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_object(object_seq)
}

/// Returns whether garbage collection deleted the object `object_seq` that a
/// read listed earlier, so that the read skips it instead of failing on it.
///
/// The two observations are made in the reverse of the order the actor writes
/// them. The actor registers the object as deleting, deletes it, unindexes it
/// if the delete succeeded, and only then takes it out of the set. Reading the
/// set first and the catalog second is therefore sound: an object absent from
/// the set was either never collected, in which case the catalog still holds
/// it and the error is genuine, or its collection has completed, in which case
/// the unindex happened before that observation and the catalog read after it
/// reports the object as gone. Reading the catalog first would let a
/// collection complete between the two and leave the read with an object that
/// still looked indexed and no longer looked deleting.
///
/// An object that is being deleted is not yet decided, so the caller waits for
/// that one delete to settle rather than assume it succeeds: a delete that
/// fails leaves the object present and indexed, and the error the read met is
/// then reported as any other, corruption included. The wait is for the
/// attempt the lookup found, taken with it, so a retry of the same object is a
/// different attempt that never holds the read up, and the actor settles every
/// attempt it abandons when it exits. The wait is therefore bounded by one
/// object store request. Every object a collection picked holds only entries
/// at or below the watermark of their region, which a read never returns, so
/// skipping one never loses an entry.
async fn is_collected(
    catalog: &RwLock<ObjectCatalog>,
    deleting: &DeletingObjects,
    object_seq: u64,
) -> bool {
    if let Some(attempt) = deleting.attempt(object_seq) {
        attempt.settled().await;
    }
    #[cfg(any(test, feature = "testing"))]
    deleting.pass_read_gap().await;
    !is_indexed(catalog, object_seq)
}

/// Fetches and decodes the segment `entry` describes in the object
/// `object_seq`. A segment that does not decode is fetched once more, since
/// the download rather than the object may be damaged; when it still does not
/// decode the error is returned as [`Error::InvalidWalObject`]. A failed fetch
/// is returned as is.
async fn fetch_segment(
    io: &dyn WalObjectIo,
    object_seq: u64,
    entry: &FooterEntry,
) -> Result<Vec<Record>> {
    let fetch = || io.get_range(object_seq, entry.segment_offset, entry.segment_len);
    if let Ok(records) = decode_segment(&fetch().await?, entry) {
        return Ok(records);
    }
    decode_segment(&fetch().await?, entry).with_context(|_| InvalidWalObjectSnafu {
        path: io.object_path(object_seq),
    })
}

/// Records the segment `entry` describes in the object `object_seq` as a hole
/// of its region, once per object, counts the skip and warns about it.
fn skip_segment(
    wal_holes: &WalHoles,
    io: &Arc<dyn WalObjectIo>,
    object_seq: u64,
    entry: &FooterEntry,
    error: &Error,
) {
    let hole = WalHole {
        path: io.object_path(object_seq),
        object_seq,
        min_entry_id: entry.min_entry_id,
        max_entry_id: entry.max_entry_id,
    };
    warn!(
        error;
        "Skipped a corrupted WAL segment of region {}, object {} (sequence {}), entry ids {}..={}",
        entry.region_id,
        hole.path,
        object_seq,
        hole.min_entry_id,
        hole.max_entry_id
    );
    METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.inc();
    wal_holes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(entry.region_id)
        .or_default()
        .entry(object_seq)
        .or_insert(hole);
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
    let started_at = Instant::now();
    let objects = io.list().await?;
    let footers = fetch_footers(io, objects, RECOVERY_CONCURRENCY).await?;
    let recovered_objects = footers.len() as u64;
    let indexed_objects = footers.len() as i64;
    let indexed_bytes = footers.iter().map(|(object, _)| object.size).sum::<u64>() as i64;
    let mut catalog = ObjectCatalog::default();
    for (object, footer) in footers {
        catalog
            .insert_object(object.object_seq, footer)
            .with_context(|_| InvalidWalObjectSnafu { path: object.path })?;
    }
    let recovered = finish_recovery(catalog)?;
    // Only a recovery that succeeded is reported, so that the objects counted
    // and the time measured are those of the same recoveries.
    METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL.inc_by(recovered_objects);
    // The catalog of the store that is about to open replaces any earlier one.
    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.set(indexed_objects);
    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.set(indexed_bytes);
    METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS.observe(started_at.elapsed().as_secs_f64());
    Ok(recovered)
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

    async fn delete(&self, object_seq: u64) -> Result<()>;

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

    async fn delete(&self, object_seq: u64) -> Result<()> {
        ObjectStoreIo::delete(self, object_seq).await
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
    use crate::object_store_wal::batch::{POSITION_LIMIT, entry_id};
    use crate::object_store_wal::format::{FOOTER_ENTRY_LEN, decode_object};

    const PREFIX: &str = "datanodes/1/epochs/2";
    const WAIT: Duration = Duration::from_secs(30);
    /// Held for the whole of every test of this module. The metrics of the
    /// object store WAL are process wide, and `cargo test` runs the tests of
    /// one binary on several threads of one process, so a test that asserts a
    /// counter or a gauge would otherwise see the objects another test
    /// creates, indexes, deletes or recovers. Serializing the module is what
    /// makes those assertions mean anything under either test runner.
    static SERIALIZED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn memory_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn config(flush_interval: Duration, max_batch_bytes: u64) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            storage_provider: String::new(),
            prefix: PREFIX.to_string(),
            flush_interval,
            max_batch_bytes: ReadableSize(max_batch_bytes),
            ..Default::default()
        }
    }

    /// The same batching as `config`, acknowledging appends on admission.
    fn enqueued(config: ObjectStoreWalConfig) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            ack_mode: AckMode::Enqueued,
            ..config
        }
    }

    /// The same batching as `config`, handling a corrupted segment as
    /// `on_corrupted_segment` says.
    fn on_corruption(
        on_corrupted_segment: CorruptedSegmentAction,
        config: ObjectStoreWalConfig,
    ) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            on_corrupted_segment,
            ..config
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

    /// The id of the entry at `position` of its region in object `object_seq`.
    fn id(object_seq: u64, position: u64) -> EntryId {
        entry_id(object_seq, position)
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

    /// Moves the watermark of `region_id` to `entry_id` and waits for the
    /// collection it triggers.
    async fn obsolete_and_collect(store: &ObjectStoreLogStore, region_id: RegionId, entry_id: u64) {
        store
            .obsolete(&provider(region_id), region_id, entry_id)
            .await
            .unwrap();
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
    }

    fn is_indexed(store: &ObjectStoreLogStore, object_seq: u64) -> bool {
        super::is_indexed(&store.catalog, object_seq)
    }

    /// Polls until the listed objects are `expected`, so a test can observe an
    /// object an in-flight create wrote before the store indexed it.
    async fn wait_for_objects(io: &dyn WalObjectIo, expected: &[u64]) {
        let deadline = Instant::now() + WAIT;
        while object_seqs(io).await != expected {
            assert!(
                Instant::now() < deadline,
                "objects never became {expected:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn test_store_batches_regions_into_shared_objects() {
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        assert_eq!(
            entries(&[(id(0, 1), "a1"), (id(1, 1), "a2"), (id(2, 1), "a3")]),
            expected_one
        );
        assert_eq!(entries(&[(id(1, 1), "b1")]), expected_two);
        store.stop().await.unwrap();
        drop(store);

        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(expected_one, read(&store, region_one, 1).await);
        assert_eq!(expected_two, read(&store, region_two, 1).await);
        assert_eq!(id(2, 1), latest(&store, region_one));
        assert_eq!(id(1, 1), latest(&store, region_two));

        let response = append(&store, region_two, "b2").await.unwrap();
        assert_eq!(Some(&id(3, 1)), response.last_entry_ids.get(&region_two));
        assert_eq!(vec![0, 1, 2, 3], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(id(1, 1), "b1"), (id(3, 1), "b2")]),
            read(&store, region_two, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_accepts_identical_retry_after_reported_failure() {
        let _serialized = SERIALIZED.lock().await;
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
            entries(&[(1, "a1"), (id(1, 1), "a2")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_fails_closed_on_conflicting_object() {
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        for data in ["a1", "a2", "a3"] {
            append(&store, region_id, data).await.unwrap();
        }

        // The objects below the watermark are collected, the highest is kept.
        let third = entries(&[(id(2, 1), "a3")]);
        obsolete_and_collect(&store, region_id, id(1, 1)).await;
        assert_eq!(vec![2], object_seqs(store.io.as_ref()).await);
        assert_eq!(third, read(&store, region_id, 1).await);
        assert_eq!(third, read(&store, region_id, id(2, 1)).await);
        assert_eq!(id(2, 1), latest(&store, region_id));

        // A lower watermark does not resurrect entries.
        store
            .obsolete(&provider(region_id), region_id, 1)
            .await
            .unwrap();
        assert_eq!(third, read(&store, region_id, 1).await);
        // The provider of another region cannot move this region's watermark.
        let error = store
            .obsolete(&provider(region(2)), region_id, id(2, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntry { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(third, read(&store, region_id, 1).await);

        store
            .obsolete_all(&provider(region_id), region_id)
            .await
            .unwrap();
        assert!(read(&store, region_id, 1).await.is_empty());
        assert_eq!(id(2, 1), latest(&store, region_id));
        // Obsoleting everything neither moves the sequence nor collects.
        append(&store, region_id, "a4").await.unwrap();
        assert_eq!(vec![2, 3], object_seqs(store.io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_stop_is_idempotent_and_fails_open_waiters() {
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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

    /// Appends `count` single-entry batches with the eager config, waiting
    /// for each to be admitted, so every append seals its own batch.
    async fn spawn_appends(
        store: &Arc<ObjectStoreLogStore>,
        region_id: RegionId,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>> {
        let mut handles = Vec::with_capacity(count);
        for index in 1..=count {
            let spawned = store.clone();
            let data = format!("a{index}");
            handles.push(tokio::spawn(async move {
                append(&spawned, region_id, &data).await
            }));
            store.wait_for_admitted_appends(index).await.unwrap();
        }
        handles
    }

    #[tokio::test]
    async fn test_store_stop_with_several_creates_in_flight() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        // Four creates are in flight, the fifth batch waits for a slot.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut open = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_CREATES {
            open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
        }
        assert!(gates.try_recv().is_err());

        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!stop.is_finished());

        // The creates in flight run to completion in any order and are
        // acknowledged; the batch that never started learns of the stop.
        for index in [2, 0, 3, 1] {
            open.remove(index.min(open.len() - 1)).send(true).unwrap();
        }
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        let mut appends = appends.into_iter();
        for object_seq in 0..MAX_IN_FLIGHT_CREATES as u64 {
            let response = timeout(WAIT, appends.next().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        let error = timeout(WAIT, appends.next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        // No create was started after stop began.
        assert!(gates.try_recv().is_err());
        assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);
        assert_eq!(id(3, 1), latest(&store, region_id));
        assert!(store.command_tx.is_closed());
    }

    #[tokio::test]
    async fn test_store_seal_after_stop_reports_stopped() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        store.stop().await.unwrap();
        let error = store.seal_open_batch().await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        assert!(object_seqs(store.io.as_ref()).await.is_empty());

        // A seal whose create is in flight when stop begins is acknowledged
        // once the object is durable.
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &manual())
            .await
            .unwrap();
        let first = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region(1), "a1").await })
        };
        store.wait_for_admitted_appends(1).await.unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!seal.is_finished());
        gate.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        timeout(WAIT, first).await.unwrap().unwrap().unwrap();
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap();
        let error = store.seal_open_batch().await.unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_stop_during_conflicting_flush_reports_stopped_and_poisons() {
        let _serialized = SERIALIZED.lock().await;
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
    async fn test_store_rejects_a_prefix_whose_entry_ids_leave_no_sequence() {
        let _serialized = SERIALIZED.lock().await;
        // The maximum id names the last sequence, so no new id of the region
        // could be greater: the store cannot be constructed.
        let object_store = memory_store();
        put_object_with_max_entry_id(&object_store, region(1)).await;
        let error = ObjectStoreLogStore::try_new(object_store, &manual())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceExhausted {
                    last_object_seq, ..
                } if last_object_seq == OBJECT_SEQ_LIMIT - 1
            ),
            "unexpected error: {error:?}"
        );
    }

    /// Writes the object `object_seq` holding the entries `entry_ids` of
    /// `region_id`, in the order given, without a store.
    async fn put_object(
        object_store: &ObjectStore,
        object_seq: u64,
        region_id: RegionId,
        entry_ids: &[EntryId],
    ) {
        let records = entry_ids
            .iter()
            .map(|entry_id| Record {
                region_id,
                entry_id: *entry_id,
                payload: Bytes::from(format!("e{entry_id}")),
            })
            .collect::<Vec<_>>();
        let encoded = encode_object(
            Header {
                object_seq,
                writer_instance: [0; 16],
            },
            &records,
        )
        .unwrap();
        ObjectStoreIo::new(object_store.clone(), PREFIX)
            .unwrap()
            .put_if_absent(object_seq, encoded.bytes)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_resumes_above_contiguous_entry_ids() {
        let _serialized = SERIALIZED.lock().await;
        // Objects written under the contiguous scheme: their ids carry no
        // object information, and the largest lies far above the ids the
        // sequence after them would assign.
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);
        put_object(&object_store, 0, region_one, &[1, 2]).await;
        put_object(&object_store, 1, region_one, &[4_999_999, 5_000_000]).await;
        put_object(&object_store, 2, region_two, &[1]).await;
        assert!(5_000_000 > id(3, 1));

        // The sequence resumes above the object the largest id names, so
        // the first new id of every region is greater than every old one.
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(5_000_000, latest(&store, region_one));
        assert_eq!(1, latest(&store, region_two));
        let response = store
            .append_batch(vec![
                entry(&store, region_one, "a"),
                entry(&store, region_two, "b"),
            ])
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(5, 1)), (region_two, id(5, 1))]),
            response.last_entry_ids
        );
        assert!(id(5, 1) > 5_000_000);
        assert_eq!(vec![0, 1, 2, 5], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[
                (1, "e1"),
                (2, "e2"),
                (4_999_999, "e4999999"),
                (5_000_000, "e5000000"),
                (id(5, 1), "a")
            ]),
            read(&store, region_one, 1).await
        );
        assert_eq!(
            entries(&[(1, "e1"), (id(5, 1), "b")]),
            read(&store, region_two, 1).await
        );
        store.stop().await.unwrap();

        // The old objects stay readable after a restart and the sequence
        // continues after the new object.
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(id(5, 1), latest(&store, region_one));
        append(&store, region_two, "b2").await.unwrap();
        assert_eq!(vec![0, 1, 2, 5, 6], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(1, "e1"), (id(5, 1), "b"), (id(6, 1), "b2")]),
            read(&store, region_two, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_assigns_ids_from_the_object_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_one = region(1);
        let region_two = region(2);

        // Regions of one object share its sequence and take their own
        // positions, in admission order.
        let response = store
            .append_batch(vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
                entry(&store, region_one, "a2"),
            ])
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(0, 2)), (region_two, id(0, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        let response = store
            .append_batch(vec![
                entry(&store, region_two, "b2"),
                entry(&store, region_one, "a3"),
            ])
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1)), (region_two, id(1, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);

        // Across consecutive objects the ids of a region step by the position
        // width, and the high bits name the object.
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2"), (id(1, 1), "a3")]),
            read(&store, region_one, 1).await
        );
        assert_eq!(
            entries(&[(1, "b1"), (id(1, 1), "b2")]),
            read(&store, region_two, 1).await
        );
        assert_eq!(POSITION_LIMIT, id(1, 1) - id(0, 1));
        assert_eq!(1, id(1, 1) >> 20);
        assert_eq!(id(1, 1), latest(&store, region_one));
        assert_eq!(id(1, 1), latest(&store, region_two));
    }

    #[tokio::test]
    async fn test_store_seals_when_a_region_exhausts_its_positions() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        let entries_of = |count: u64| {
            (0..count)
                .map(|_| entry(&store, region_id, ""))
                .collect::<Vec<_>>()
        };

        // The open batch holds every position of the region.
        let response = store
            .append_batch(entries_of(POSITION_LIMIT - 1))
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(0, POSITION_LIMIT - 1))]),
            response.last_entry_ids
        );
        assert!(object_seqs(store.io.as_ref()).await.is_empty());

        // The next entry of the region seals the batch and opens the next
        // object; another region would still have fit.
        let response = append(&store, region_id, "next").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        timeout(
            WAIT,
            store.wait_durable(&provider(region_id), id(0, POSITION_LIMIT - 1)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        // An append that alone runs past the range fits no object: it seals
        // the open batch like any append the batch cannot take, then it is
        // refused without poisoning the store.
        let error = store
            .append_batch(entries_of(POSITION_LIMIT))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::WalEntryPositionExhausted { .. }),
            "unexpected error: {error:?}"
        );
        let response = append(&store, region_id, "after").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![0, 1, 2], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(id(1, 1), "next"), (id(2, 1), "after")]),
            read(&store, region_id, id(1, 1)).await
        );
    }

    #[tokio::test]
    async fn test_store_obsolete_raises_the_sequence_floor() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);
        let mut admitted = 0;
        let mut spawn_append = |region_id, data: &'static str| {
            let store = store.clone();
            admitted += 1;
            (
                tokio::spawn(async move { append(&store, region_id, data).await }),
                admitted,
            )
        };
        let (first, count) = spawn_append(region_one, "a1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, first).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        let (second, count) = spawn_append(region_one, "a2");
        store.wait_for_admitted_appends(count).await.unwrap();

        // A durable watermark names an object below the next sequence and
        // changes nothing: the open batch goes on under sequence 1.
        store
            .obsolete(&provider(region_one), region_one, 1)
            .await
            .unwrap();
        let (third, count) = spawn_append(region_one, "a3");
        store.wait_for_admitted_appends(count).await.unwrap();

        // A watermark that names a later object, as one inherited from
        // another prefix does, cannot move the sequence while the open batch
        // handed out ids under it: neither the sequence nor the watermark
        // moves.
        let error = store
            .obsolete(&provider(region_two), region_two, id(3, 7))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 1, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );

        // Once the batch is durable the sequence moves above that object, so
        // the region's next id is greater than the watermark.
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, second).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1))]),
            response.last_entry_ids
        );
        let response = timeout(WAIT, third).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 2))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        obsolete_and_collect(&store, region_two, id(3, 7)).await;
        assert_eq!(
            Some(&id(3, 7)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_two)
        );
        // Object 0 held only entry 1 of region one, at its watermark, and
        // object 1 was the highest: only object 0 was collected.
        assert_eq!(vec![1], object_seqs(store.io.as_ref()).await);
        let (fourth, count) = spawn_append(region_two, "b1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, fourth).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(4, 1))]),
            response.last_entry_ids
        );
        assert!(id(4, 1) > id(3, 7));
        assert_eq!(vec![1, 4], object_seqs(store.io.as_ref()).await);
        // The watermark hides nothing of this prefix; the region has no
        // entry at or below it.
        assert_eq!(
            entries(&[(id(4, 1), "b1")]),
            read(&store, region_two, 1).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_floor_waits_for_a_create_in_flight_and_its_rollback() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_one = region(1);
        let region_two = region(2);
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_one, "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        let assert_unsettled = |error: Error| {
            assert!(
                matches!(
                    error,
                    Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
                ),
                "unexpected error: {error:?}"
            );
        };

        // Object 0 is in flight: the floor cannot move past it.
        let error = store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap_err();
        assert_unsettled(error);

        // The create fails before anything was written; whether an object
        // exists at sequence 0 is not known to the store, so the floor still
        // cannot skip it.
        gate.send(false).unwrap();
        timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err();
        assert!(object_seqs(io.as_ref()).await.is_empty());
        let error = store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap_err();
        assert_unsettled(error);

        // The retry reconciles sequence 0, after which the floor applies.
        let retry = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_one, "a1").await })
        };
        timeout(WAIT, gates.recv())
            .await
            .unwrap()
            .unwrap()
            .send(true)
            .unwrap();
        let response = timeout(WAIT, retry).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap();
        let next = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_one, "a2").await })
        };
        timeout(WAIT, gates.recv())
            .await
            .unwrap()
            .unwrap()
            .send(true)
            .unwrap();
        let response = timeout(WAIT, next).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(6, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 6], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_floor_does_not_skip_an_object_left_by_a_failed_create() {
        let _serialized = SERIALIZED.lock().await;
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_one = region(1);
        let region_two = region(2);

        // Object 0 is written, but its create is reported as failed: the
        // object exists and is not indexed.
        io.fail_after_next_put.store(true, Ordering::Relaxed);
        append(&store, region_one, "a1").await.unwrap_err();
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(0, latest(&store, region_one));

        // A floor from another region may not skip sequence 0: a later
        // object would carry the retry, and recovery would index both.
        let error = store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );

        // The identical retry reconciles sequence 0; now the floor applies.
        let response = append(&store, region_one, "a1").await.unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap();
        let response = append(&store, region_two, "b1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(6, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 6], object_seqs(io.as_ref()).await);
        store.stop().await.unwrap();

        // Recovery indexes the same objects the store did: one copy of "a1".
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_one, 0).await);
        assert_eq!(
            entries(&[(id(6, 1), "b1")]),
            read(&store, region_two, 0).await
        );
        assert_eq!(1, latest(&store, region_one));
    }

    #[tokio::test]
    async fn test_store_obsolete_cancelled_before_queueing_publishes_nothing() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);

        // The command channel is full, so the call suspends before the
        // actor learns of it, and is dropped there.
        let permits = store.command_tx.reserve_many(COMMAND_BUFFER).await.unwrap();
        {
            let provider = provider(region_id);
            let obsolete = store.obsolete(&provider, region_id, id(5, 1));
            tokio::pin!(obsolete);
            assert!(futures::poll!(obsolete.as_mut()).is_pending());
        }
        drop(permits);

        // Neither the watermark nor the floor was applied: the next entry
        // takes sequence 0 and is readable.
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_id)
                .is_none()
        );
        let response = append(&store, region_id, "new").await.unwrap();
        assert_eq!(HashMap::from([(region_id, 1)]), response.last_entry_ids);
        assert_eq!(entries(&[(1, "new")]), read(&store, region_id, 0).await);

        // A call that completes applies both.
        store
            .obsolete(&provider(region_id), region_id, id(5, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(5, 1)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        let response = append(&store, region_id, "later").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(6, 1))]),
            response.last_entry_ids
        );
        assert_eq!(
            entries(&[(id(6, 1), "later")]),
            read(&store, region_id, 0).await
        );
    }

    #[tokio::test]
    async fn test_store_floor_is_measured_against_a_rollback_not_the_next_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_one = region(1);
        let region_two = region(2);
        let pending = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_one, "a1").await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();

        // Object 0 is in flight and the next sequence is 1, but a failure
        // rolls it back to 0: a watermark naming object 0 is not accepted.
        let error = store
            .obsolete(&provider(region_two), region_two, 1)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );
        gate.send(false).unwrap();
        timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err();

        // The other region takes id 1 under sequence 0 and can read it back.
        let write = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_two, "b1").await })
        };
        timeout(WAIT, gates.recv())
            .await
            .unwrap()
            .unwrap()
            .send(true)
            .unwrap();
        let response = timeout(WAIT, write).await.unwrap().unwrap().unwrap();
        assert_eq!(HashMap::from([(region_two, 1)]), response.last_entry_ids);
        assert_eq!(1, latest(&store, region_two));
        assert_eq!(entries(&[(1, "b1")]), read(&store, region_two, 0).await);
    }

    #[tokio::test]
    async fn test_store_enqueued_issued_id_needs_no_floor_while_in_flight() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        store.hold_creates();
        append(&store, region_id, "a1").await.unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!seal.is_finished());

        // Id 1 was handed out and is in flight: a region that is dropped
        // hands it in, and it is accepted without waiting for the upload,
        // with the watermark capped to what is durable. An id the store
        // never handed out still has to wait.
        store
            .obsolete(&provider(region_id), region_id, 1)
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        let error = store
            .obsolete(&provider(region_id), region_id, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 0, .. }
            ),
            "unexpected error: {error:?}"
        );

        store.release_creates();
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap();
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_id, 0).await);
        store
            .obsolete(&provider(region_id), region_id, 1)
            .await
            .unwrap();
        assert!(read(&store, region_id, 0).await.is_empty());
        let response = append(&store, region_id, "a2").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_inherited_watermark_raises_the_floor() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &enqueued(manual())).await;
        let region_id = region(1);

        // The region has nothing durable here, so the recorded watermark
        // stays at zero, but its ids must still start above the watermark.
        store
            .obsolete(&provider(region_id), region_id, id(5, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        let response = append(&store, region_id, "r1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(6, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        timeout(WAIT, store.wait_durable(&provider(region_id), id(6, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id(6, 1), latest(&store, region_id));
        store.stop().await.unwrap();

        // Replay from the watermark sees the entry.
        let store = open(object_store, &enqueued(manual())).await;
        assert_eq!(
            entries(&[(id(6, 1), "r1")]),
            read(&store, region_id, id(5, 1) + 1).await
        );
    }

    #[tokio::test]
    async fn test_store_obsolete_queued_behind_stop_records_the_watermark() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        let region_two = region(2);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_two, "b1").await.unwrap();

        // Both commands are queued before the actor runs; it handles the
        // stop, exits and drops the obsolete without answering it.
        let stop = store.stop();
        tokio::pin!(stop);
        assert!(futures::poll!(stop.as_mut()).is_pending());
        let provider_one = provider(region_id);
        let obsolete = store.obsolete(&provider_one, region_id, 1);
        tokio::pin!(obsolete);
        assert!(futures::poll!(obsolete.as_mut()).is_pending());
        timeout(WAIT, stop).await.unwrap().unwrap();
        // The actor drops the command channel as it exits, which it does on
        // its own runtime after it answered the stop, so the close is waited
        // for rather than expected the instant `stop` returns.
        let deadline = Instant::now() + WAIT;
        while !store.command_tx.is_closed() {
            assert!(
                Instant::now() < deadline,
                "the actor never dropped the command channel"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        timeout(WAIT, obsolete).await.unwrap().unwrap();

        // Nothing is assigned an id any more, so the watermark alone holds.
        assert_eq!(
            Some(&1),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        assert!(read(&store, region_id, 0).await.is_empty());
        assert_eq!(1, latest(&store, region_id));

        // The same holds for a call that finds the actor gone.
        store
            .obsolete(&provider(region_two), region_two, id(1, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(1, 1)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_two)
        );
        assert!(read(&store, region_two, 0).await.is_empty());
    }

    #[tokio::test]
    async fn test_store_stop_during_successful_flush_acknowledges_waiters() {
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
            entries(&[(id(0, 1), "a1"), (id(1, 1), "a2"), (id(2, 1), "a3")]),
            read(&store, region(1), 1).await
        );
    }

    #[tokio::test]
    async fn test_store_rejects_invalid_config() {
        let _serialized = SERIALIZED.lock().await;
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

    /// Asserts that recovery over `object_store` rejects the object at `path`
    /// for `reason`, whatever a read does with a corrupted segment.
    async fn assert_recovery_rejects(object_store: &ObjectStore, path: &str, reason: &str) {
        for on_corrupted_segment in [CorruptedSegmentAction::Skip, CorruptedSegmentAction::Fail] {
            let config = on_corruption(on_corrupted_segment, eager());
            let error = ObjectStoreLogStore::try_new(object_store.clone(), &config)
                .await
                .unwrap_err();
            assert_invalid_object(&error, path, reason);
        }
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
        let _serialized = SERIALIZED.lock().await;
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
            // The objects `populate` gave the region one entry each.
            let objects = (0..40u64)
                .filter(|object| !(object + number as u64).is_multiple_of(3))
                .collect::<Vec<_>>();
            assert_eq!(id(*objects.last().unwrap(), 1), durable[&region_id]);
            assert_eq!(durable[&region_id], latest(&store, region_id));
            assert_eq!(objects.len(), read(&store, region_id, 1).await.len());
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_corrupted_trailer_version_and_footer() {
        let _serialized = SERIALIZED.lock().await;
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

            assert_recovery_rejects(&object_store, &path, reason).await;
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_header_sequence_mismatch() {
        let _serialized = SERIALIZED.lock().await;
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

        assert_recovery_rejects(
            &object_store,
            &io.object_path(5),
            "header sequence 0 does not match key sequence 5",
        )
        .await;
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
        let _serialized = SERIALIZED.lock().await;
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

            assert_recovery_rejects(&object_store, &path, reason).await;
            let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
            let error = recover_by_decoding(&io).await.unwrap_err();
            assert_invalid_object(&error, &path, reason);
        }
    }

    /// Writes object 0 with one entry of region 1 and one of region 2 and
    /// object 1 with a second entry of region 2, then flips a byte in the
    /// segment of region 2 in object 0. Returns the key of object 0.
    async fn write_corrupted_segment(object_store: &ObjectStore) -> String {
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
        let path = object_path(object_store, 0);
        corrupt_object(object_store, &path, |bytes| {
            let (_, footer) = footer_of(bytes);
            let segment = &footer[1];
            assert_eq!(region_two, segment.region_id);
            bytes[(segment.segment_offset + segment.segment_len - 1) as usize] ^= 1;
        })
        .await;
        path
    }

    #[tokio::test]
    async fn test_store_skips_a_corrupted_segment_and_records_a_hole() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);
        let path = write_corrupted_segment(&object_store).await;
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

        // Recovery indexes the object; a read of the other region and of the
        // later object of the damaged region are unaffected.
        let store = open(object_store, &eager()).await;
        assert_eq!(CorruptedSegmentAction::Skip, store.on_corrupted_segment);
        assert_eq!(1, latest(&store, region_one));
        assert_eq!(id(1, 1), latest(&store, region_two));
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_one, 1).await);
        assert_eq!(
            entries(&[(id(1, 1), "b2")]),
            read(&store, region_two, id(1, 1)).await
        );
        assert!(store.wal_holes(&provider(region_two)).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );

        // A read across the damaged segment continues with the next object
        // and records the hole.
        assert_eq!(
            entries(&[(id(1, 1), "b2")]),
            read(&store, region_two, 1).await
        );
        let hole = WalHole {
            path: path.clone(),
            object_seq: 0,
            min_entry_id: 1,
            max_entry_id: 1,
        };
        assert_eq!(
            vec![hole.clone()],
            store.wal_holes(&provider(region_two)).unwrap()
        );
        assert!(store.wal_holes(&provider(region_one)).unwrap().is_empty());
        assert_eq!(
            skipped + 1,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );

        // Every read across the segment skips it again; the hole is one.
        assert_eq!(
            entries(&[(id(1, 1), "b2")]),
            read(&store, region_two, 1).await
        );
        assert_eq!(vec![hole], store.wal_holes(&provider(region_two)).unwrap());
        assert_eq!(
            skipped + 2,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
    }

    #[tokio::test]
    async fn test_store_corrupted_segment_fails_the_read_that_decodes_it() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);
        let path = write_corrupted_segment(&object_store).await;
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

        // Recovery indexes the object; only the corrupted segment is unreadable.
        let config = on_corruption(CorruptedSegmentAction::Fail, eager());
        let store = open(object_store, &config).await;
        assert_eq!(1, latest(&store, region_one));
        assert_eq!(id(1, 1), latest(&store, region_two));
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_one, 1).await);
        assert_eq!(
            entries(&[(id(1, 1), "b2")]),
            read(&store, region_two, id(1, 1)).await
        );
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
        assert!(store.wal_holes(&provider(region_two)).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
    }

    #[tokio::test]
    async fn test_store_reads_a_segment_whose_first_download_was_damaged() {
        let _serialized = SERIALIZED.lock().await;
        for on_corrupted_segment in [CorruptedSegmentAction::Skip, CorruptedSegmentAction::Fail] {
            let io = Arc::new(FaultyIo::new());
            let config = on_corruption(on_corrupted_segment, eager());
            let store = ObjectStoreLogStore::open(io.clone(), &config)
                .await
                .unwrap();
            let region_one = region(1);
            let region_two = region(2);
            store
                .append_batch(vec![
                    entry(&store, region_one, "a1"),
                    entry(&store, region_two, "b1"),
                ])
                .await
                .unwrap();
            append(&store, region_two, "b2").await.unwrap();
            let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

            // The first download of the segment is damaged, the second is
            // not: the read sees the entries and records no hole.
            io.damage_next_range_read.store(true, Ordering::Relaxed);
            assert_eq!(
                entries(&[(1, "b1"), (id(1, 1), "b2")]),
                read(&store, region_two, 1).await
            );
            assert!(!io.damage_next_range_read.load(Ordering::Relaxed));
            assert!(store.wal_holes(&provider(region_two)).unwrap().is_empty());
            assert_eq!(
                skipped,
                METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
            );
            store.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_recovery_bounds_concurrency_and_orders_by_sequence() {
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
        let _serialized = SERIALIZED.lock().await;
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
            entries(&[(1, "wide"), (id(1, 1), "narrow")]),
            read(&store, region(1), 1).await
        );
        assert_eq!(
            entries(&[(1, "wide")]),
            read(&store, region(regions), 1).await
        );
    }

    #[tokio::test]
    async fn test_store_pipelines_creates_and_acknowledges_in_sequence_order() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = ParkedIo::parking_creates();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 2).await;

        // At most the limit of creates run at a time; the rest wait for a slot.
        let mut releases = HashMap::new();
        for _ in 0..MAX_IN_FLIGHT_CREATES {
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            releases.insert(object_seq, release);
        }
        assert_eq!(
            (0..MAX_IN_FLIGHT_CREATES as u64).collect::<BTreeSet<_>>(),
            releases.keys().copied().collect::<BTreeSet<_>>()
        );
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(parked.try_recv().is_err());
        assert!(appends.iter().all(|append| !append.is_finished()));

        // Objects 2 and 1 become durable before object 0 and free a slot
        // each, but nothing is acknowledged ahead of object 0.
        for (released, expected_next) in [(2, 4), (1, 5)] {
            releases.remove(&released).unwrap().send(()).unwrap();
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            assert_eq!(expected_next, object_seq);
            releases.insert(object_seq, release);
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(vec![1, 2], object_seqs(io.as_ref()).await);
        assert!(appends.iter().all(|append| !append.is_finished()));
        assert_eq!(0, latest(&store, region_id));

        // Object 0 releases the acknowledgements of objects 0 to 2.
        releases.remove(&0).unwrap().send(()).unwrap();
        let mut appends = appends.into_iter();
        for object_seq in 0..3 {
            let response = timeout(WAIT, appends.next().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(id(2, 1), latest(&store, region_id));

        // Object 5 before object 4: the append of object 5 waits for it.
        releases.remove(&3).unwrap().send(()).unwrap();
        releases.remove(&5).unwrap().send(()).unwrap();
        let fourth = timeout(WAIT, appends.next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(3, 1))]),
            fourth.last_entry_ids
        );
        let mut appends = appends.collect::<Vec<_>>();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(appends.iter().all(|append| !append.is_finished()));
        releases.remove(&4).unwrap().send(()).unwrap();
        for (append, object_seq) in appends.drain(..).zip(4..) {
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(
            MAX_IN_FLIGHT_CREATES,
            io.max_in_flight.load(Ordering::SeqCst)
        );
        assert_eq!(vec![0, 1, 2, 3, 4, 5], object_seqs(io.as_ref()).await);
        assert_eq!(
            entries(&[
                (id(0, 1), "a1"),
                (id(1, 1), "a2"),
                (id(2, 1), "a3"),
                (id(3, 1), "a4"),
                (id(4, 1), "a5"),
                (id(5, 1), "a6")
            ]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_seals_at_the_threshold_while_creates_are_in_flight() {
        let _serialized = SERIALIZED.lock().await;
        // The threshold holds exactly two entries, so every second admission
        // must seal, whether or not a create is in flight.
        let region_id = region(1);
        let sample = Entry::Naive(NaiveEntry {
            provider: provider(region_id),
            region_id,
            entry_id: 0,
            data: b"a1".to_vec(),
        });
        let threshold = 2 * sample.estimated_size();
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(
            io.clone(),
            &config(Duration::from_secs(3600), threshold as u64),
        )
        .await
        .unwrap();

        // Nine appends are admitted while the first create is gated: the
        // batches seal at admissions 2, 4, 6 and 8 and the ninth entry stays
        // in the open batch.
        let appends = spawn_appends(&store, region_id, 9).await;
        let mut open = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_CREATES {
            open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
        }
        assert!(gates.try_recv().is_err());
        for gate in open {
            gate.send(true).unwrap();
        }
        let mut appends = appends.into_iter();
        for index in 0..8 {
            let response = timeout(WAIT, appends.next().unwrap())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(index / 2, index % 2 + 1))]),
                response.last_entry_ids
            );
        }
        assert!(!appends.next().unwrap().is_finished());

        // No object holds more than the threshold plus the append that
        // reached it.
        assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);
        for object_seq in 0..4 {
            let decoded = decode_object(&io.get(object_seq).await.unwrap()).unwrap();
            assert_eq!(2, decoded.records.len(), "object {object_seq}");
            let payload = decoded
                .records
                .iter()
                .map(|record| record.payload.len())
                .sum::<usize>();
            assert!(payload <= threshold + sample.estimated_size());
        }
    }

    #[tokio::test]
    async fn test_store_transient_failure_rolls_back_batches_that_were_not_created() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        // Every slot is taken; the last batch waits for one.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut open = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_CREATES {
            open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(gates.try_recv().is_err());

        // Object 0 fails while the others are in flight: nothing is decided
        // until they complete, and the freed slots start no create. None of
        // the later objects is created either: every batch fails and the
        // sequence rolls back to object 0.
        open.remove(0).send(false).unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(appends.iter().all(|append| !append.is_finished()));
        for gate in open {
            gate.send(false).unwrap();
        }
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
        }
        assert!(gates.try_recv().is_err());
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert_eq!(0, latest(&store, region_id));

        // The retried entries take the same sequences and ids.
        let retries = spawn_appends(&store, region_id, 2).await;
        for _ in 0..2 {
            timeout(WAIT, gates.recv())
                .await
                .unwrap()
                .unwrap()
                .send(true)
                .unwrap();
        }
        for (retry, object_seq) in retries.into_iter().zip(0..) {
            let response = timeout(WAIT, retry).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        assert_eq!(
            entries(&[(id(0, 1), "a1"), (id(1, 1), "a2")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_transient_failure_before_a_durable_object_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (io, mut gates) = GatedIo::over(object_store.clone());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let third = entry(&store, region_id, "a3");
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut open = Vec::new();
        for _ in 0..2 {
            open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
        }

        // Object 1 is durable while object 0 failed: object 1 cannot be
        // rolled back, so the store poisons itself and acknowledges neither.
        open.remove(0).send(false).unwrap();
        open.remove(0).send(true).unwrap();
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(
                    unwrap_shared(&error),
                    Error::WalObjectHistoryGap {
                        object_seq: 0,
                        later_object_seq: 1,
                        ..
                    }
                ),
                "unexpected error: {error:?}"
            );
        }
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);
        let error = store.latest_entry_id(&provider(region_id)).unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectHistoryGap { .. }),
            "unexpected error: {error:?}"
        );
        let error = store.append_batch(vec![third]).await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectHistoryGap { .. }),
            "unexpected error: {error:?}"
        );
        store.stop().await.unwrap();

        // Recovery indexes the durable object: its entries were never
        // acknowledged, but they replay like those of a crash between the
        // creation of an object and its acknowledgement.
        let store = ObjectStoreLogStore::try_new(object_store, &eager())
            .await
            .unwrap();
        assert_eq!(id(1, 1), latest(&store, region_id));
        assert_eq!(
            entries(&[(id(1, 1), "a2")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_permanent_failure_before_a_durable_object_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (io, mut gates) = GatedIo::over(object_store.clone());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let foreign = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        foreign
            .put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut open = Vec::new();
        for _ in 0..2 {
            open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
        }

        // Object 1 is created; object 0 conflicts with the foreign object.
        open.remove(1).send(true).unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(vec![0, 1], object_seqs(&foreign).await);
        assert!(appends.iter().all(|append| !append.is_finished()));
        open.remove(0).send(true).unwrap();
        for append in appends {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
        let error = store.latest_entry_id(&provider(region_id)).unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_enqueued_append_returns_before_the_object_exists() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);

        let response = timeout(WAIT, append(&store, region_id, "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(HashMap::from([(region_id, 1)]), response.last_entry_ids);
        assert!(object_seqs(store.io.as_ref()).await.is_empty());
        assert_eq!(0, latest(&store, region_id));
        assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
        assert!(read(&store, region_id, 1).await.is_empty());
        let response = append(&store, region_id, "a2").await.unwrap();
        assert_eq!(HashMap::from([(region_id, 2)]), response.last_entry_ids);

        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        assert_eq!(2, latest(&store, region_id));
        assert_eq!(2, store.durable_entry_id(&provider(region_id)).unwrap());
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_durable_id_advances_after_create_and_indexing() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &enqueued(eager()))
            .await
            .unwrap();
        let region_one = region(1);
        let region_two = region(2);

        let response = timeout(WAIT, append(&store, region_one, "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(HashMap::from([(region_one, 1)]), response.last_entry_ids);
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        assert_eq!(0, store.durable_entry_id(&provider(region_one)).unwrap());
        let wait_one = {
            let store = store.clone();
            tokio::spawn(async move { store.wait_durable(&provider(region_one), 1).await })
        };
        // The other region has nothing pending, and neither has an id the
        // store never handed out.
        timeout(WAIT, store.wait_durable(&provider(region_two), 0))
            .await
            .unwrap()
            .unwrap();
        timeout(WAIT, store.wait_durable(&provider(region_one), 7))
            .await
            .unwrap()
            .unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!wait_one.is_finished());

        gate.send(true).unwrap();
        timeout(WAIT, wait_one).await.unwrap().unwrap().unwrap();
        assert_eq!(1, store.durable_entry_id(&provider(region_one)).unwrap());
        assert_eq!(0, store.durable_entry_id(&provider(region_two)).unwrap());
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_enqueued_obsolete_never_passes_the_durable_id() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        store
            .obsolete(&provider(region_id), region_id, 2)
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2")]),
            read(&store, region_id, 1).await
        );
        store
            .obsolete(&provider(region_id), region_id, 2)
            .await
            .unwrap();
        assert_eq!(
            Some(&2),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        assert!(read(&store, region_id, 1).await.is_empty());
    }

    /// Appends one entry in the enqueued mode with `config`, whose create
    /// blocks on a gate, then appends a second one that the backlog threshold
    /// must stall. Returns the store, the gates, the stalled append and the
    /// gate of the first create.
    async fn stall_second_append(
        config: ObjectStoreWalConfig,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<GatedIo>,
        mpsc::UnboundedReceiver<oneshot::Sender<bool>>,
        tokio::task::JoinHandle<Result<AppendBatchResponse>>,
        oneshot::Sender<bool>,
    ) {
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &config)
            .await
            .unwrap();
        let region_id = region(1);
        let response = timeout(WAIT, append(&store, region_id, "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(HashMap::from([(region_id, 1)]), response.last_entry_ids);
        if config.max_unpersisted_age < Duration::from_secs(1) {
            tokio::time::sleep(config.max_unpersisted_age * 2).await;
        }

        let stalled = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a2").await })
        };
        // The stall seals the open batch so that an upload is in flight.
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!stalled.is_finished());
        assert!(gates.try_recv().is_err());
        (store, io, gates, stalled, gate)
    }

    #[tokio::test]
    async fn test_store_enqueued_backlog_bytes_stall_admission_until_an_upload_completes() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            max_unpersisted_bytes: ReadableSize(1),
            ..enqueued(manual())
        };
        let (store, io, mut gates, stalled, gate) = stall_second_append(config).await;
        let region_id = region(1);
        // Two more appends queue behind the stalled one.
        let third = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a3").await })
        };
        let fourth = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a4").await })
        };
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!third.is_finished() && !fourth.is_finished());
        assert!(gates.try_recv().is_err());

        // The upload releases the second append, whose entry reaches the
        // threshold again; the stall seals it so that the next upload can
        // release the third, and so on.
        gate.send(true).unwrap();
        let response = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(1, latest(&store, region_id));
        for (append, expected) in [(third, id(2, 1)), (fourth, id(3, 1))] {
            timeout(WAIT, gates.recv())
                .await
                .unwrap()
                .unwrap()
                .send(true)
                .unwrap();
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, expected)]),
                response.last_entry_ids
            );
        }
        assert_eq!(vec![0, 1, 2], object_seqs(io.as_ref()).await);
        assert_eq!(
            entries(&[(1, "a1"), (id(1, 1), "a2"), (id(2, 1), "a3")]),
            read(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_backlog_age_stalls_admission_until_an_upload_completes() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            max_unpersisted_age: Duration::from_millis(50),
            ..enqueued(manual())
        };
        let (store, io, _gates, stalled, gate) = stall_second_append(config).await;
        let region_id = region(1);

        gate.send(true).unwrap();
        let response = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_id, 1).await);
        // A young open batch does not stall.
        timeout(WAIT, append(&store, region_id, "a3"))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_enqueued_repeats_a_create_that_failed_transiently() {
        let _serialized = SERIALIZED.lock().await;
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &enqueued(eager()))
            .await
            .unwrap();
        let region_id = region(1);

        io.fail_next_put.store(true, Ordering::Relaxed);
        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(HashMap::from([(region_id, 1)]), response.last_entry_ids);
        timeout(WAIT, store.wait_durable(&provider(region_id), 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_eq!(entries(&[(1, "a1")]), read(&store, region_id, 1).await);
    }

    #[tokio::test]
    async fn test_store_enqueued_permanent_failure_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &enqueued(eager())).await;
        let region_id = region(1);
        ObjectStoreIo::new(object_store, PREFIX)
            .unwrap()
            .put_if_absent(0, Bytes::from_static(b"foreign"))
            .await
            .unwrap();

        // The append was acknowledged; the conflict surfaces afterwards.
        let second = entry(&store, region_id, "a2");
        append(&store, region_id, "a1").await.unwrap();
        let error = timeout(WAIT, store.wait_durable(&provider(region_id), 1))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        let error = store.append_batch(vec![second]).await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        assert!(store.latest_entry_id(&provider(region_id)).is_err());
        // The acknowledged entry was dropped, which stop reports.
        let error = store.stop().await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_reports_a_backlog_lost_before_the_stop_command() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates) = GatedIo::new();
        let store = ObjectStoreLogStore::open(io.clone(), &enqueued(manual()))
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();

        // Stop began, but the actor has not received the stop command when
        // the create fails: the backlog is dropped and the failure is kept
        // for the stop that follows.
        store.begin_stop();
        gate.send(false).unwrap();
        let error = timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        // The lost entry cannot be certified as durable before the stop
        // command is handled; a durable id still is.
        let error = timeout(WAIT, store.wait_durable(&provider(region_id), 1))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        timeout(WAIT, store.wait_durable(&provider(region_id), 0))
            .await
            .unwrap()
            .unwrap();
        let error = store.stop().await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert!(gates.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_uploads_the_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();
        assert!(object_seqs(store.io.as_ref()).await.is_empty());

        store.stop().await.unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        assert_eq!(2, latest(&store, region_id));
        assert_eq!(
            entries(&[(1, "a1"), (2, "a2")]),
            read(&store, region_id, 1).await
        );

        // A backlog that cannot be uploaded is reported by stop.
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &enqueued(manual()))
            .await
            .unwrap();
        append(&store, region_id, "a1").await.unwrap();
        io.fail_next_put.store(true, Ordering::Relaxed);
        let error = store.stop().await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert!(object_seqs(io.as_ref()).await.is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_collects_objects_below_the_watermarks() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_one = region(1);
        let region_two = region(2);
        // Object 0 holds one entry of each region, object 1 two entries of
        // region one, object 2 one of region two and object 3 one of region
        // one.
        store
            .append_batch(vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
            ])
            .await
            .unwrap();
        store.seal_open_batch().await.unwrap();
        store
            .append_batch(vec![
                entry(&store, region_one, "a2"),
                entry(&store, region_one, "a3"),
            ])
            .await
            .unwrap();
        store.seal_open_batch().await.unwrap();
        append(&store, region_two, "b2").await.unwrap();
        store.seal_open_batch().await.unwrap();
        append(&store, region_one, "a4").await.unwrap();
        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![0, 1, 2, 3], object_seqs(store.io.as_ref()).await);
        let deleted = METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get();
        let failed = METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get();

        // A watermark inside object 1 keeps it, and object 0 holds a segment
        // of a region without a watermark.
        obsolete_and_collect(&store, region_one, id(1, 1)).await;
        assert_eq!(vec![0, 1, 2, 3], object_seqs(store.io.as_ref()).await);
        // A watermark at the last id of its segment releases object 1.
        obsolete_and_collect(&store, region_one, id(1, 2)).await;
        assert_eq!(vec![0, 2, 3], object_seqs(store.io.as_ref()).await);
        // Object 0 goes once both regions are at or above their segments;
        // object 2 lies above the watermark of region two.
        obsolete_and_collect(&store, region_two, id(0, 1)).await;
        assert_eq!(vec![2, 3], object_seqs(store.io.as_ref()).await);
        obsolete_and_collect(&store, region_two, id(2, 1)).await;
        assert_eq!(vec![3], object_seqs(store.io.as_ref()).await);
        // The highest-sequence object is kept whatever the watermark.
        obsolete_and_collect(&store, region_one, id(3, 1)).await;
        assert_eq!(vec![3], object_seqs(store.io.as_ref()).await);
        assert!(is_indexed(&store, 3) && !is_indexed(&store, 2));
        assert_eq!(
            deleted + 3,
            METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get()
        );
        assert_eq!(failed, METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get());

        // Reads never needed the collected objects; the largest id of a
        // region outlives its last object, so a durability wait for it
        // returns and the watermark of the `enqueued` mode is not clamped
        // below it.
        assert!(read(&store, region_one, 0).await.is_empty());
        assert!(read(&store, region_two, 0).await.is_empty());
        assert_eq!(id(3, 1), latest(&store, region_one));
        assert_eq!(id(2, 1), latest(&store, region_two));
        timeout(WAIT, store.wait_durable(&provider(region_two), id(2, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![provider(region_one)],
            store.list_namespaces().await.unwrap()
        );
        let response = append(&store, region_two, "b3").await.unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(4, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![3, 4], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(id(4, 1), "b3")]),
            read(&store, region_two, 0).await
        );
    }

    #[tokio::test]
    async fn test_store_keeps_an_object_that_is_created_but_not_indexed() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = ParkedIo::parking_creates();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        let first = {
            let store = store.clone();
            tokio::spawn(async move { append(&store, region_id, "a1").await })
        };
        let (_, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        release.send(()).unwrap();
        timeout(WAIT, first).await.unwrap().unwrap().unwrap();

        // Object 2 is created while object 1 is still in flight, so it is
        // not indexed yet.
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut releases = HashMap::new();
        for _ in 0..2 {
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            releases.insert(object_seq, release);
        }
        releases.remove(&2).unwrap().send(()).unwrap();
        wait_for_objects(io.as_ref(), &[0, 2]).await;
        assert_eq!(id(0, 1), latest(&store, region_id));

        // Object 0 is the highest indexed object: it is kept although an
        // object above it exists.
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert_eq!(vec![0, 2], object_seqs(io.as_ref()).await);

        // Once objects 1 and 2 are indexed the next collection releases it.
        releases.remove(&1).unwrap().send(()).unwrap();
        for append in appends {
            timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        }
        assert_eq!(id(2, 1), latest(&store, region_id));
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert_eq!(vec![1, 2], object_seqs(io.as_ref()).await);
        obsolete_and_collect(&store, region_id, id(2, 1)).await;
        assert_eq!(vec![2], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_holds_collection_on_a_prefix_with_contiguous_ids() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let region_one = region(1);
        let region_two = region(2);
        put_object(&object_store, 0, region_one, &[1, 2]).await;
        put_object(&object_store, 1, region_one, &[4_999_999, 5_000_000]).await;
        put_object(&object_store, 2, region_two, &[1]).await;
        let store = open(object_store.clone(), &eager()).await;

        // Every object is below its watermark, but id 5_000_000 names object
        // 4, beyond the highest object: object 2 alone could not resume the
        // sequence above every id ever written, so nothing is collected.
        obsolete_and_collect(&store, region_one, 5_000_000).await;
        obsolete_and_collect(&store, region_two, 1).await;
        assert_eq!(vec![0, 1, 2], object_seqs(store.io.as_ref()).await);

        // The first object written at the raised sequence is durable: from
        // then on it is the retained anchor and the old objects go.
        let response = append(&store, region_two, "b").await.unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(5, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0, 1, 2, 5], object_seqs(store.io.as_ref()).await);
        obsolete_and_collect(&store, region_two, id(5, 1)).await;
        assert_eq!(vec![5], object_seqs(store.io.as_ref()).await);
        assert_eq!(5_000_000, latest(&store, region_one));
        assert!(read(&store, region_one, 0).await.is_empty());
        store.stop().await.unwrap();

        // Recovery resumes after the retained object, above every old id.
        let store = open(object_store, &eager()).await;
        assert_eq!(0, latest(&store, region_one));
        assert_eq!(id(5, 1), latest(&store, region_two));
        let response = append(&store, region_one, "a").await.unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(6, 1))]),
            response.last_entry_ids
        );
        assert!(id(6, 1) > 5_000_000);
        assert_eq!(vec![5, 6], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            entries(&[(id(6, 1), "a")]),
            read(&store, region_one, 0).await
        );
    }

    #[tokio::test]
    async fn test_store_read_started_before_collection_skips_a_deleted_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        let region_id = region(1);
        for data in ["a1", "a2", "a3"] {
            append(&store, region_id, data).await.unwrap();
        }

        // The stream lists objects 0 to 2 before objects 0 and 1 are
        // collected, and skips them when it gets there.
        let stream = store.read(&provider(region_id), 1, None).await.unwrap();
        obsolete_and_collect(&store, region_id, id(1, 1)).await;
        assert_eq!(vec![2], object_seqs(store.io.as_ref()).await);
        let read_entries = stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|entry| (entry.entry_id(), entry.into_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(entries(&[(id(2, 1), "a3")]), read_entries);

        // An object that is missing while still indexed fails the read.
        append(&store, region_id, "a4").await.unwrap();
        assert_eq!(vec![2, 3], object_seqs(store.io.as_ref()).await);
        object_store
            .delete(&object_path(&object_store, 2))
            .await
            .unwrap();
        assert!(is_indexed(&store, 2));
        let error = store
            .read(&provider(region_id), id(1, 1) + 1, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                if path == &object_path(&object_store, 2)),
            "unexpected error: {error:?}"
        );
    }

    /// Collects the entries of a stream a test took earlier.
    async fn collect_stream(
        stream: SendableEntryStream<'static, Entry, Error>,
    ) -> Result<Vec<(EntryId, Vec<u8>)>> {
        Ok(stream
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .map(|entry| (entry.entry_id(), entry.into_bytes()))
            .collect())
    }

    #[tokio::test]
    async fn test_store_read_waits_for_a_pending_delete_and_skips_the_collected_object() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = FaultyIo::holding_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // The stream lists objects 0 and 1 before the collection starts.
        let stream = store.read(&provider(region_id), 1, None).await.unwrap();
        store
            .obsolete(&provider(region_id), region_id, id(0, 1))
            .await
            .unwrap();
        let (deleted_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        assert_eq!(0, deleted_seq);
        assert!(is_indexed(&store, 0));

        // The fetch of object 0 fails while its delete is still undecided,
        // so the read waits for that one delete instead of guessing.
        io.fail_reads_of.store(0, Ordering::Relaxed);
        let read = tokio::spawn(collect_stream(stream));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!read.is_finished());

        // The delete succeeds: the object is unindexed before it leaves the
        // in-flight set, so the read that wakes up finds it collected.
        release.send(true).unwrap();
        let read_entries = timeout(WAIT, read).await.unwrap().unwrap().unwrap();
        assert_eq!(entries(&[(id(1, 1), "a2")]), read_entries);
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert!(!is_indexed(&store, 0));
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_read_observes_the_in_flight_set_before_the_catalog() {
        let _serialized = SERIALIZED.lock().await;
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // The fetch of object 0 fails, so the read consults the two
        // observations, and every read parks between them.
        let mut parked = store.hold_read_gap();
        io.fail_reads_of.store(0, Ordering::Relaxed);
        let stream = store.read(&provider(region_id), 1, None).await.unwrap();
        let read = tokio::spawn(collect_stream(stream));
        let release = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        assert!(is_indexed(&store, 0));

        // A whole collection of object 0 runs in that window: it is
        // registered, deleted, unindexed and taken out of the set again. The
        // observation the read has left is the catalog, which reports the
        // object as collected, so the read skips it. Observing the catalog
        // first would have left the read with an object that looked indexed
        // and no longer looked deleting.
        io.fail_reads_of.store(u64::MAX, Ordering::Relaxed);
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert!(!is_indexed(&store, 0));
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);

        release.send(()).unwrap();
        let read_entries = timeout(WAIT, read).await.unwrap().unwrap().unwrap();
        assert_eq!(entries(&[(id(1, 1), "a2")]), read_entries);
    }

    #[tokio::test]
    async fn test_store_failed_delete_lets_the_read_error_take_its_normal_path() {
        let _serialized = SERIALIZED.lock().await;
        let region_id = region(1);

        for on_corrupted_segment in [CorruptedSegmentAction::Skip, CorruptedSegmentAction::Fail] {
            for corrupted in [false, true] {
                // Each case starts on its own prefix, so the read still lists
                // the object whose delete is about to fail.
                let config = on_corruption(on_corrupted_segment, eager());
                let (io, mut parked) = FaultyIo::holding_deletes();
                let store = ObjectStoreLogStore::open(io.clone(), &config)
                    .await
                    .unwrap();
                append(&store, region_id, "a1").await.unwrap();
                append(&store, region_id, "a2").await.unwrap();
                let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

                // The read meets object 0 while its delete is pending, either
                // through a fetch failure that has nothing to do with the
                // collection or through a segment that does not decode.
                let stream = store.read(&provider(region_id), 1, None).await.unwrap();
                store
                    .obsolete(&provider(region_id), region_id, id(0, 1))
                    .await
                    .unwrap();
                let (_, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
                if corrupted {
                    io.damage_reads_of.store(0, Ordering::Relaxed);
                } else {
                    io.fail_reads_of.store(0, Ordering::Relaxed);
                }
                let read = tokio::spawn(collect_stream(stream));
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
                assert!(!read.is_finished());

                // The delete fails, so the object stays present and indexed
                // and the error the read met is reported rather than hidden.
                release.send(false).unwrap();
                let read = timeout(WAIT, read).await.unwrap().unwrap();
                let case = format!("{on_corrupted_segment:?}, corrupted {corrupted}");
                match (corrupted, on_corrupted_segment) {
                    // A fetch failure always fails the read.
                    (false, _) => {
                        let error = read.unwrap_err();
                        assert!(
                            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                                if path == &io.object_path(0)),
                            "unexpected error ({case}): {error:?}"
                        );
                        assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
                        assert_eq!(
                            skipped,
                            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
                        );
                    }
                    // The segment is skipped, recorded as a hole and counted.
                    (true, CorruptedSegmentAction::Skip) => {
                        assert_eq!(entries(&[(id(1, 1), "a2")]), read.unwrap(), "{case}");
                        assert_eq!(
                            vec![WalHole {
                                path: io.object_path(0),
                                object_seq: 0,
                                min_entry_id: 1,
                                max_entry_id: 1,
                            }],
                            store.wal_holes(&provider(region_id)).unwrap()
                        );
                        assert_eq!(
                            skipped + 1,
                            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
                        );
                    }
                    // The read fails with the decode error.
                    (true, CorruptedSegmentAction::Fail) => {
                        let error = read.unwrap_err();
                        assert_invalid_object(
                            &error,
                            &io.object_path(0),
                            &format!("segment of region {region_id} checksum mismatch"),
                        );
                        assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
                        assert_eq!(
                            skipped,
                            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
                        );
                    }
                }
                assert!(is_indexed(&store, 0), "{case}");
                assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await, "{case}");
            }
        }
    }

    #[tokio::test]
    async fn test_store_retries_a_failed_delete_at_the_next_collection() {
        let _serialized = SERIALIZED.lock().await;
        let io = Arc::new(FaultyIo::new());
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();
        let deleted = METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get();
        let failed = METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get();

        // The delete fails: the object stays, indexed, and the failure is
        // counted.
        io.fail_next_delete.store(true, Ordering::Relaxed);
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        assert!(is_indexed(&store, 0));
        assert_eq!(deleted, METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get());
        assert_eq!(
            failed + 1,
            METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get()
        );

        // The next collection deletes it.
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);
        assert!(!is_indexed(&store, 0));
        assert_eq!(
            deleted + 1,
            METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get()
        );
        assert_eq!(
            failed + 1,
            METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get()
        );
    }

    #[tokio::test]
    async fn test_store_does_not_collect_after_stop_began() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // Stop began but the actor has not exited: the watermark is
        // recorded and nothing is collected.
        store.begin_stop();
        obsolete_and_collect(&store, region_id, id(0, 1)).await;
        assert_eq!(
            Some(&id(0, 1)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        store.stop().await.unwrap();
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);

        // After the actor exited a watermark is recorded alone.
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        assert!(read(&store, region_id, 0).await.is_empty());
    }

    #[tokio::test]
    async fn test_store_read_waits_for_its_own_attempt_not_for_the_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = FaultyIo::holding_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // The read is polled by the test alone, so the failure and the retry
        // below both land while it is waiting for the first attempt.
        let stream = store.read(&provider(region_id), 1, None).await.unwrap();
        store
            .obsolete(&provider(region_id), region_id, id(0, 1))
            .await
            .unwrap();
        let (_, first) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        io.fail_reads_of.store(0, Ordering::Relaxed);
        let read = collect_stream(stream);
        tokio::pin!(read);
        for _ in 0..16 {
            assert!(futures::poll!(read.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }

        // The first attempt fails, which leaves the object indexed, and a
        // second collection retries it under a new attempt that is parked in
        // turn. The read has not been polled in between.
        first.send(false).unwrap();
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert!(is_indexed(&store, 0));
        store
            .obsolete(&provider(region_id), region_id, id(0, 1))
            .await
            .unwrap();
        let (retried_seq, second) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        assert_eq!(0, retried_seq);

        // The attempt the read holds has settled, so the retry of the same
        // object does not hold it up: it reads the catalog, finds the object
        // indexed and reports the error it met. Waiting for the sequence
        // instead of the attempt would wait for the retry, and for every
        // attempt after it.
        let mut finished = None;
        for _ in 0..1024 {
            if let std::task::Poll::Ready(result) = futures::poll!(read.as_mut()) {
                finished = Some(result);
                break;
            }
            tokio::task::yield_now().await;
        }
        let error = finished
            .expect("the read is still waiting for the retry of the object it met")
            .unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                if path == &io.object_path(0)),
            "unexpected error: {error:?}"
        );
        second.send(true).unwrap();
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert!(!is_indexed(&store, 0));
    }

    #[tokio::test]
    async fn test_store_dropped_without_stop_settles_the_deletes_it_abandons() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = FaultyIo::holding_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // The read meets object 0 while its delete is parked, so it waits for
        // that attempt.
        let stream = store.read(&provider(region_id), 1, None).await.unwrap();
        store
            .obsolete(&provider(region_id), region_id, id(0, 1))
            .await
            .unwrap();
        let (_, _release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
        io.fail_reads_of.store(0, Ordering::Relaxed);
        let read = tokio::spawn(collect_stream(stream));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!read.is_finished());

        // The store is dropped without `stop`: the actor loses its commands,
        // exits and drops the parked delete. The stream outlives it and must
        // not wait for an attempt that will never complete; the object is
        // still there and indexed, so the error it met is returned.
        drop(store);
        let error = timeout(WAIT, read).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                if path == &io.object_path(0)),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_bounds_the_deletes_of_one_collection() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = FaultyIo::holding_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        // Twice the cap of collectable objects, plus the one that is kept.
        let objects = 2 * MAX_IN_FLIGHT_DELETES;
        for index in 0..=objects {
            append(&store, region_id, &format!("a{index}"))
                .await
                .unwrap();
        }
        let all = (0..=objects as u64).collect::<Vec<_>>();
        assert_eq!(all, object_seqs(io.as_ref()).await);

        // Every object below the last is collectable, but the collection
        // takes only the cap and answers the caller of `obsolete` at once.
        timeout(
            WAIT,
            store.obsolete(&provider(region_id), region_id, id(objects as u64 - 1, 1)),
        )
        .await
        .unwrap()
        .unwrap();
        let mut releases = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_DELETES {
            releases.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(parked.try_recv().is_err(), "more deletes than the cap ran");
        assert_eq!(
            (0..MAX_IN_FLIGHT_DELETES as u64).collect::<Vec<_>>(),
            releases.iter().map(|(seq, _)| *seq).collect::<Vec<_>>()
        );
        assert_eq!(all, object_seqs(io.as_ref()).await);

        // Appends are admitted and acknowledged while the deletes are parked.
        let response = timeout(WAIT, append(&store, region_id, "while-deleting"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Some(&id(objects as u64 + 1, 1)),
            response.last_entry_ids.get(&region_id)
        );

        // Each delete that succeeds refills the collection, so the rest of
        // the backlog drains without another watermark, still within the cap.
        for (_, release) in releases {
            release.send(true).unwrap();
        }
        for _ in 0..MAX_IN_FLIGHT_DELETES {
            let (_, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            release.send(true).unwrap();
        }
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert!(parked.try_recv().is_err());
        assert_eq!(
            vec![objects as u64, objects as u64 + 1],
            object_seqs(io.as_ref()).await
        );
    }

    #[tokio::test]
    async fn test_store_failing_deletes_do_not_block_the_objects_behind_them() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = FaultyIo::holding_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        // Twice the cap of collectable objects, plus the one that is kept.
        let objects = 2 * MAX_IN_FLIGHT_DELETES;
        for index in 0..=objects {
            append(&store, region_id, &format!("a{index}"))
                .await
                .unwrap();
        }
        let watermark = id(objects as u64 - 1, 1);
        let all = (0..=objects as u64).collect::<Vec<_>>();

        // The first pass takes the first four objects and every one of their
        // deletes fails, so they stay present and indexed.
        store
            .obsolete(&provider(region_id), region_id, watermark)
            .await
            .unwrap();
        let mut attempted = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_DELETES {
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            attempted.push(object_seq);
            release.send(false).unwrap();
        }
        assert_eq!(
            (0..MAX_IN_FLIGHT_DELETES as u64).collect::<Vec<_>>(),
            attempted
        );
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert_eq!(all, object_seqs(io.as_ref()).await);

        // The next pass continues after them instead of taking the same four
        // again, so the objects behind the failures are attempted and go.
        store
            .obsolete(&provider(region_id), region_id, watermark)
            .await
            .unwrap();
        let mut attempted = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_DELETES {
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            attempted.push(object_seq);
            release.send(true).unwrap();
        }
        assert_eq!(
            (MAX_IN_FLIGHT_DELETES as u64..objects as u64).collect::<Vec<_>>(),
            attempted
        );

        // Their successful deletes wrap the sweep, which retries the four that
        // failed; every delete succeeds from now on, so only the object that
        // is always kept is left.
        let drain = tokio::spawn(async move {
            while let Some((_, release)) = parked.recv().await {
                let _ = release.send(true);
            }
        });
        wait_for_objects(io.as_ref(), &[objects as u64]).await;
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        drain.abort();
    }

    #[tokio::test]
    async fn test_store_stop_waits_for_a_delete_in_flight() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut gates, mut delete_gates) = GatedIo::gating_deletes();
        let store = ObjectStoreLogStore::open(io.clone(), &eager())
            .await
            .unwrap();
        let region_id = region(1);
        for data in ["a1", "a2"] {
            let pending = {
                let store = store.clone();
                tokio::spawn(async move { append(&store, region_id, data).await })
            };
            timeout(WAIT, gates.recv())
                .await
                .unwrap()
                .unwrap()
                .send(true)
                .unwrap();
            timeout(WAIT, pending).await.unwrap().unwrap().unwrap();
        }

        // The delete of object 0 is blocked when stop is requested: stop
        // waits for it, and the object is unindexed before stop returns.
        store
            .obsolete(&provider(region_id), region_id, id(0, 1))
            .await
            .unwrap();
        let delete_gate = timeout(WAIT, delete_gates.recv()).await.unwrap().unwrap();
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!stop.is_finished());
        assert!(is_indexed(&store, 0));

        delete_gate.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert!(!is_indexed(&store, 0));
        assert_eq!(vec![1], object_seqs(io.as_ref()).await);
        assert!(delete_gates.try_recv().is_err());
    }

    /// Object access that fails a conditional create on request, either before
    /// or after the object was actually written, fails every create, fails a
    /// listing, fails the reads of one object or one range of it, damages
    /// range reads, holds and fails deletes, or answers after a delay.
    struct FaultyIo {
        inner: ObjectStoreIo,
        fail_next_put: AtomicBool,
        fail_after_next_put: AtomicBool,
        /// Fails every conditional create, like an unwritable object store.
        fail_puts: AtomicBool,
        /// Writes the object of the next conditional create before the create
        /// runs, so that it meets the object a create whose response was lost
        /// left behind and reconciles it.
        reconcile_next_put: AtomicBool,
        fail_next_list: AtomicBool,
        /// Milliseconds every read and every conditional create waits before
        /// it answers, so a case can force a wait on the timers.
        answer_after_millis: AtomicU64,
        /// Nanoseconds of the shortest wait actually served since a case reset
        /// it; `u64::MAX` while none has been. An operation that waited here
        /// took at least this long, which is the measured floor a case bounds
        /// a timer by.
        shortest_wait_nanos: AtomicU64,
        /// Sequence of the object whose reads fail; `u64::MAX` fails none.
        fail_reads_of: AtomicU64,
        /// Offset of the read of that object which fails; `u64::MAX` fails
        /// every read of it.
        fail_reads_at: AtomicU64,
        damage_next_range_read: AtomicBool,
        /// Sequence of the object whose range reads are always damaged, so a
        /// segment stays undecodable however often it is fetched;
        /// `u64::MAX` damages none.
        damage_reads_of: AtomicU64,
        fail_next_delete: AtomicBool,
        /// When set, every delete parks until the test releases it and
        /// decides whether it proceeds or fails, so a test can hold an
        /// object in the in-flight set of a collection and settle it either
        /// way.
        held_deletes: Option<mpsc::UnboundedSender<(u64, oneshot::Sender<bool>)>>,
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
                fail_puts: AtomicBool::new(false),
                reconcile_next_put: AtomicBool::new(false),
                fail_next_list: AtomicBool::new(false),
                answer_after_millis: AtomicU64::new(0),
                shortest_wait_nanos: AtomicU64::new(u64::MAX),
                fail_reads_of: AtomicU64::new(u64::MAX),
                fail_reads_at: AtomicU64::new(u64::MAX),
                damage_next_range_read: AtomicBool::new(false),
                damage_reads_of: AtomicU64::new(u64::MAX),
                fail_next_delete: AtomicBool::new(false),
                held_deletes: None,
            }
        }

        /// Parks every delete until the test releases it with the outcome it
        /// should have: `true` removes the object, `false` fails the delete
        /// and leaves the object in place.
        fn holding_deletes() -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            Self::holding_deletes_over(memory_store())
        }

        fn holding_deletes_over(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            let (held_deletes, parked) = mpsc::unbounded_channel();
            let io = Self {
                held_deletes: Some(held_deletes),
                ..Self::over(object_store)
            };
            (Arc::new(io), parked)
        }

        async fn answer_after(&self) {
            let millis = self.answer_after_millis.load(Ordering::Relaxed);
            if millis > 0 {
                let waiting_since = Instant::now();
                tokio::time::sleep(Duration::from_millis(millis)).await;
                let waited = waiting_since.elapsed().as_nanos() as u64;
                self.shortest_wait_nanos
                    .fetch_min(waited, Ordering::Relaxed);
            }
        }

        /// Forgets the waits served so far, so that the next
        /// [`shortest_wait`](Self::shortest_wait) covers one operation alone.
        fn forget_waits(&self) {
            self.shortest_wait_nanos.store(u64::MAX, Ordering::Relaxed);
        }

        /// The shortest wait served since then, which every operation that
        /// waited took at least.
        fn shortest_wait(&self) -> Duration {
            let nanos = self.shortest_wait_nanos.load(Ordering::Relaxed);
            assert_ne!(u64::MAX, nanos, "the object store served no wait");
            Duration::from_nanos(nanos)
        }

        fn check_read(&self, object_seq: u64, offset: u64) -> Result<()> {
            let fail_at = self.fail_reads_at.load(Ordering::Relaxed);
            if self.fail_reads_of.load(Ordering::Relaxed) == object_seq
                && (fail_at == u64::MAX || fail_at == offset)
            {
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
            if self.fail_next_put.swap(false, Ordering::Relaxed)
                || self.fail_puts.load(Ordering::Relaxed)
            {
                return injected_failure("write", self.inner.object_path(object_seq));
            }
            self.answer_after().await;
            if self.reconcile_next_put.swap(false, Ordering::Relaxed) {
                self.inner
                    .put_if_absent(object_seq, content.clone())
                    .await?;
            }
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if self.fail_after_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure("write", self.inner.object_path(object_seq));
            }
            Ok(result)
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.check_read(object_seq, 0)?;
            self.answer_after().await;
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.check_read(object_seq, offset)?;
            self.answer_after().await;
            let bytes = self.inner.get_range(object_seq, offset, len).await?;
            if self.damage_next_range_read.swap(false, Ordering::Relaxed)
                || self.damage_reads_of.load(Ordering::Relaxed) == object_seq
            {
                let mut damaged = bytes.to_vec();
                if let Some(last) = damaged.last_mut() {
                    *last ^= 1;
                }
                return Ok(Bytes::from(damaged));
            }
            Ok(bytes)
        }

        async fn delete(&self, object_seq: u64) -> Result<()> {
            if self.fail_next_delete.swap(false, Ordering::Relaxed) {
                return injected_failure("delete", self.inner.object_path(object_seq));
            }
            // The object is in the in-flight set of the collection for as
            // long as the test parks the delete here.
            if let Some(held) = &self.held_deletes {
                let (release, released) = oneshot::channel();
                held.send((object_seq, release)).unwrap();
                if !released.await.unwrap() {
                    return injected_failure("delete", self.inner.object_path(object_seq));
                }
            }
            self.inner.delete(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            if self.fail_next_list.swap(false, Ordering::Relaxed) {
                return injected_failure("list", format!("{PREFIX}/objects/"));
            }
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    type Gates = mpsc::UnboundedReceiver<oneshot::Sender<bool>>;

    /// Object access whose conditional creates, and on request deletes, block
    /// until the test decides whether they proceed or fail with a transient
    /// error.
    struct GatedIo {
        inner: ObjectStoreIo,
        gates: mpsc::UnboundedSender<oneshot::Sender<bool>>,
        delete_gates: Option<mpsc::UnboundedSender<oneshot::Sender<bool>>>,
    }

    impl GatedIo {
        fn new() -> (Arc<Self>, Gates) {
            Self::over(memory_store())
        }

        fn over(object_store: ObjectStore) -> (Arc<Self>, Gates) {
            let (gates, gate_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                gates,
                delete_gates: None,
            };
            (Arc::new(io), gate_rx)
        }

        /// Gates deletes as well as creates.
        fn gating_deletes() -> (Arc<Self>, Gates, Gates) {
            let (gates, gate_rx) = mpsc::unbounded_channel();
            let (delete_gates, delete_gate_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(memory_store(), PREFIX).unwrap(),
                gates,
                delete_gates: Some(delete_gates),
            };
            (Arc::new(io), gate_rx, delete_gate_rx)
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

        async fn delete(&self, object_seq: u64) -> Result<()> {
            if let Some(delete_gates) = &self.delete_gates {
                let (gate, opened) = oneshot::channel();
                delete_gates.send(gate).unwrap();
                if !opened.await.unwrap() {
                    return injected_failure("delete", self.inner.object_path(object_seq));
                }
            }
            self.inner.delete(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    /// Object access whose reads, or conditional creates, park until the test
    /// releases them, counting how many are in flight. Objects must be short
    /// enough to be read whole, so every object costs exactly one read.
    struct ParkedIo {
        inner: ObjectStoreIo,
        parked: mpsc::UnboundedSender<(u64, oneshot::Sender<()>)>,
        park_creates: bool,
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
            Self::new(object_store, false)
        }

        fn parking_creates() -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<()>)>,
        ) {
            Self::new(memory_store(), true)
        }

        fn new(
            object_store: ObjectStore,
            park_creates: bool,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<()>)>,
        ) {
            let (parked, parked_rx) = mpsc::unbounded_channel();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                parked,
                park_creates,
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
            if self.park_creates {
                self.park(object_seq).await;
            }
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            if !self.park_creates {
                self.park(object_seq).await;
            }
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            if !self.park_creates {
                self.park(object_seq).await;
            }
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn delete(&self, object_seq: u64) -> Result<()> {
            self.inner.delete(object_seq).await
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

        async fn delete(&self, object_seq: u64) -> Result<()> {
            self.inner.delete(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }
    }

    /// Every failure the object store access of the store can produce, driven
    /// through the store, each asserting the outcome the *Failure matrix* of
    /// `docs/rfcs/2026-09-06-object-store-wal.md` documents for it.
    ///
    /// The cases run in one test, in order, so that the counters and gauges
    /// they assert on move only by what the case itself did; [`SERIALIZED`]
    /// keeps the rest of the module out while they do.
    mod fault_matrix {
        use prometheus::Histogram;

        use super::*;

        /// A row of the *Failure matrix*, named by its *Situation* cell.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Row {
            /// create succeeds, or identical retry
            Created,
            /// transient I/O error, `durable` mode, no later object created
            TransientRollsBack,
            /// transient I/O error, `durable` mode, a later object is durable
            TransientHistoryGap,
            /// transient I/O error, `enqueued` mode
            TransientRepeated,
            /// transient I/O error, `enqueued` mode after `stop` began
            TransientAfterStop,
            /// conflicting object
            Conflict,
            /// encoding or catalog error
            CatalogError,
            /// create succeeds at the last representable sequence
            LastSequence,
        }

        /// Every row of the matrix, in the order the RFC lists them.
        const ROWS: [Row; 8] = [
            Row::Created,
            Row::TransientRollsBack,
            Row::TransientHistoryGap,
            Row::TransientRepeated,
            Row::TransientAfterStop,
            Row::Conflict,
            Row::CatalogError,
            Row::LastSequence,
        ];

        /// One failure of the object store access and the row it exercises. A
        /// failure of a read or of a listing is not the outcome of a conditional
        /// create, so the matrix has no row for it.
        struct Case {
            /// What the object store does.
            fault: &'static str,
            row: Option<Row>,
            run: fn() -> BoxFuture<'static, ()>,
        }

        /// The counters the cases assert on, sampled around a case.
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
        struct Counters {
            created_objects: u64,
            /// Samples of the seal-to-durable histogram, which every object
            /// counted as created also feeds.
            seal_to_durable: u64,
            create_failures: u64,
            create_conflicts: u64,
            poisoned: u64,
            /// Samples of the object size and object entry histograms, which
            /// every batch that seals and encodes feeds once each.
            sealed_objects: u64,
            /// What those two histograms recorded: the bytes of the objects
            /// the case sealed, and the entries they hold. The entry sum tells
            /// an object apart from the entries in it, which the sample counts
            /// cannot.
            object_bytes: u64,
            object_entries: u64,
            /// Samples of the acknowledgement histogram, which every append
            /// the `durable` mode acknowledges feeds once.
            acknowledged_appends: u64,
            /// Objects recovery read footers from, and samples of the recovery
            /// histogram, which every recovery that succeeds feeds once.
            recovered_objects: u64,
            recoveries: u64,
            /// Samples of the read histogram, which every read stream that is
            /// driven to its end feeds once.
            reads: u64,
            deleted_objects: u64,
            failed_deletes: u64,
            /// A read of the matrix never meets a segment that does not
            /// decode, so every case expects this to stand still.
            skipped_segments: u64,
            stalled_appends: u64,
            /// Samples of the stalled-append histogram, which every append the
            /// backlog thresholds held back feeds once, on whichever path it
            /// leaves the queue by.
            stalled_waits: u64,
        }

        /// What a histogram of whole numbers has recorded so far. The sums
        /// are exact at these magnitudes, so a case states them as counts.
        fn sum_of(histogram: &Histogram) -> u64 {
            let sum = histogram.get_sample_sum();
            assert_eq!(
                sum,
                sum.round(),
                "a histogram of whole numbers recorded {sum}"
            );
            sum as u64
        }

        /// The length of the object a batch of `entries` encodes into, which is
        /// what the object size histogram records when the batch seals. Entry
        /// ids do not affect the length, so the positions of the batch will do.
        fn object_bytes_of(entries: &[(RegionId, &str)]) -> u64 {
            let mut positions = HashMap::new();
            let records = entries
                .iter()
                .map(|(region_id, data)| {
                    let position = positions.entry(*region_id).or_insert(0);
                    *position += 1;
                    Record {
                        region_id: *region_id,
                        entry_id: entry_id(0, *position),
                        payload: Bytes::from(data.as_bytes().to_vec()),
                    }
                })
                .collect::<Vec<_>>();
            encode_object(
                Header {
                    object_seq: 0,
                    writer_instance: [0; 16],
                },
                &records,
            )
            .unwrap()
            .bytes
            .len() as u64
        }

        /// The length of the object that the batch of one two-byte entry of one
        /// region, which most cases seal, encodes into.
        fn one_entry_object() -> u64 {
            object_bytes_of(&[(region(1), "a1")])
        }

        /// The sum and the count of a timing histogram, taken before a case
        /// forces a wait on it.
        fn timer_before(histogram: &Histogram) -> (f64, u64) {
            (histogram.get_sample_sum(), histogram.get_sample_count())
        }

        /// One observation of a timing histogram: the seconds it recorded for
        /// the one sample it took since `before`, the wait the case forced on
        /// it, and what the case itself took around that wait.
        #[derive(Debug, Clone, Copy)]
        struct Timed {
            recorded: f64,
            /// The wait the case forced, as it was measured rather than as it
            /// was asked for: a request told to wait 20ms may have waited far
            /// longer, and the timer has to have covered that.
            held: Duration,
            elapsed: Duration,
        }

        impl Timed {
            fn since(
                histogram: &Histogram,
                before: (f64, u64),
                held: Duration,
                elapsed: Duration,
            ) -> Self {
                assert_eq!(
                    before.1 + 1,
                    histogram.get_sample_count(),
                    "expected the timer to take exactly one sample"
                );
                Self {
                    recorded: histogram.get_sample_sum() - before.0,
                    held,
                    elapsed,
                }
            }

            /// Asserts the two observations of one timer, taken around waits of
            /// different lengths: each lies in the interval its own wait forces,
            /// and the two differ. A timer that records a constant satisfies at
            /// most one of the intervals and never the second check, whatever
            /// constant it records.
            fn assert_pair(name: &str, short: Self, long: Self) {
                for observed in [short, long] {
                    assert!(
                        observed.recorded >= observed.held.as_secs_f64()
                            && observed.recorded <= observed.elapsed.as_secs_f64(),
                        "the {name} timer recorded {}s around a wait of {:?}, expected at most {:?}",
                        observed.recorded,
                        observed.held,
                        observed.elapsed
                    );
                }
                assert_ne!(
                    short.recorded, long.recorded,
                    "the {name} timer recorded one duration around waits of {:?} and {:?}",
                    short.held, long.held
                );
            }
        }

        /// Samples of the two object shape histograms, which a sealed batch
        /// feeds together, so that one field covers both and a batch that fed
        /// only one of them fails the case.
        fn sealed_objects() -> u64 {
            let bytes = METRIC_OBJECT_STORE_WAL_OBJECT_BYTES.get_sample_count();
            let entries = METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES.get_sample_count();
            assert_eq!(
                bytes, entries,
                "an object was sized but its entries were not counted"
            );
            bytes
        }

        impl Counters {
            fn sample() -> Self {
                Self {
                    created_objects: METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL.get(),
                    seal_to_durable: METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS
                        .get_sample_count(),
                    sealed_objects: sealed_objects(),
                    object_bytes: sum_of(&METRIC_OBJECT_STORE_WAL_OBJECT_BYTES),
                    object_entries: sum_of(&METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES),
                    acknowledged_appends: METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS
                        .get_sample_count(),
                    recovered_objects: METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL.get(),
                    recoveries: METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS.get_sample_count(),
                    reads: METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count(),
                    create_failures: METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL.get(),
                    create_conflicts: METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL.get(),
                    poisoned: METRIC_OBJECT_STORE_WAL_POISONED_TOTAL.get(),
                    deleted_objects: METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get(),
                    failed_deletes: METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get(),
                    skipped_segments: METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get(),
                    stalled_appends: METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get(),
                    stalled_waits: METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS
                        .get_sample_count(),
                }
            }

            /// The counters recorded since `before`.
            fn since(before: Self) -> Self {
                let now = Self::sample();
                Self {
                    created_objects: now.created_objects - before.created_objects,
                    seal_to_durable: now.seal_to_durable - before.seal_to_durable,
                    sealed_objects: now.sealed_objects - before.sealed_objects,
                    object_bytes: now.object_bytes - before.object_bytes,
                    object_entries: now.object_entries - before.object_entries,
                    acknowledged_appends: now.acknowledged_appends - before.acknowledged_appends,
                    recovered_objects: now.recovered_objects - before.recovered_objects,
                    recoveries: now.recoveries - before.recoveries,
                    reads: now.reads - before.reads,
                    create_failures: now.create_failures - before.create_failures,
                    create_conflicts: now.create_conflicts - before.create_conflicts,
                    poisoned: now.poisoned - before.poisoned,
                    deleted_objects: now.deleted_objects - before.deleted_objects,
                    failed_deletes: now.failed_deletes - before.failed_deletes,
                    skipped_segments: now.skipped_segments - before.skipped_segments,
                    stalled_appends: now.stalled_appends - before.stalled_appends,
                    stalled_waits: now.stalled_waits - before.stalled_waits,
                }
            }
        }

        /// Asserts that the store has exactly the objects `indexed`, by their
        /// sequence, on both indexed gauges, taking their sizes from the object
        /// store itself. An object a failed create left behind, or one a
        /// poisoned store never indexed, is listed but must not be counted, so
        /// this is checked at the failure itself, before any retry or reopen
        /// can index it after all.
        async fn assert_indexed(io: &dyn WalObjectIo, indexed: &[u64]) {
            let bytes = io
                .list()
                .await
                .unwrap()
                .into_iter()
                .filter(|object| indexed.contains(&object.object_seq))
                .map(|object| object.size)
                .sum::<u64>();
            assert_eq!(
                (indexed.len() as i64, bytes as i64),
                indexed_gauges(),
                "expected the gauges to hold the objects {indexed:?}"
            );
        }

        /// Opens a store over the object access a case injects and samples the
        /// counters once it is open, which is how a case that brings its own
        /// object access starts: the recovery of an empty prefix belongs to the
        /// fixture, not to what the case is measured on.
        async fn opened_over(
            io: Arc<dyn WalObjectIo>,
            config: &ObjectStoreWalConfig,
        ) -> (Arc<ObjectStoreLogStore>, Counters) {
            let store = ObjectStoreLogStore::open(io, config).await.unwrap();
            (store, Counters::sample())
        }

        /// Spawns `count` appends, each sealing a batch of its own, and collects
        /// the gate of every create they start, in sequence order. The creates
        /// are parked at their gates, so the case decides which of them
        /// succeeds, which fails and in what order.
        async fn gated_appends(
            store: &Arc<ObjectStoreLogStore>,
            region_id: RegionId,
            gates: &mut Gates,
            count: usize,
        ) -> (
            Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>>,
            Vec<oneshot::Sender<bool>>,
        ) {
            let appends = spawn_appends(store, region_id, count).await;
            let mut open = Vec::with_capacity(count);
            for _ in 0..count {
                open.push(timeout(WAIT, gates.recv()).await.unwrap().unwrap());
            }
            (appends, open)
        }

        /// Awaits every append and asserts each failed with the error
        /// `recognise` accepts, which is what a row that fails a batch and every
        /// later one means.
        async fn assert_appends_failed(
            appends: Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>>,
            recognise: impl Fn(&Error) -> bool,
        ) {
            for append in appends {
                let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
                assert!(
                    recognise(unwrap_shared(&error)),
                    "unexpected error: {error:?}"
                );
            }
        }

        /// Writes the object of another writer at sequence 0, after the store
        /// has opened so that its recovery does not meet an object that is not
        /// one of its own.
        async fn put_foreign_object(object_store: ObjectStore) {
            ObjectStoreIo::new(object_store, PREFIX)
                .unwrap()
                .put_if_absent(0, Bytes::from_static(b"foreign"))
                .await
                .unwrap();
        }

        /// Asserts that a read of the poisoned store fails with the terminal
        /// error rather than returning what the store still has indexed.
        async fn assert_read_is_terminal(store: &ObjectStoreLogStore, region_id: RegionId) {
            let error = store
                .read(&provider(region_id), 1, None)
                .await
                .err()
                .expect("a read of a poisoned store must fail");
            assert!(
                terminal(&store.terminal_error).is_some(),
                "the store was expected to be poisoned"
            );
            assert!(
                matches!(&error, Error::ObjectStoreWal { .. }),
                "unexpected error: {error:?}"
            );
        }

        fn indexed_gauges() -> (i64, i64) {
            (
                METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get(),
                METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get(),
            )
        }

        #[tokio::test]
        async fn test_store_fault_matrix() {
            let _serialized = SERIALIZED.lock().await;
            let cases = [
                Case {
                    fault: "a create that succeeds",
                    row: Some(Row::Created),
                    run: || Box::pin(create_succeeds()),
                },
                Case {
                    fault: "a create that finds an object with identical content",
                    row: Some(Row::Created),
                    run: || Box::pin(create_finds_an_identical_object()),
                },
                Case {
                    fault: "a create whose response is lost",
                    row: Some(Row::TransientRollsBack),
                    run: || Box::pin(create_response_is_lost()),
                },
                Case {
                    fault: "an object store that is unwritable, `durable` mode",
                    row: Some(Row::TransientRollsBack),
                    run: || Box::pin(object_store_is_unwritable()),
                },
                Case {
                    fault: "a create that fails transiently before a durable object",
                    row: Some(Row::TransientHistoryGap),
                    run: || Box::pin(create_fails_transiently_before_a_durable_object()),
                },
                Case {
                    fault: "a create that fails transiently, `enqueued` mode",
                    row: Some(Row::TransientRepeated),
                    run: || Box::pin(create_fails_transiently_enqueued()),
                },
                Case {
                    fault: "a create that fails transiently after `stop` began",
                    row: Some(Row::TransientAfterStop),
                    run: || Box::pin(create_fails_transiently_after_stop_began()),
                },
                Case {
                    fault: "a create that finds an object with different content, `durable` mode",
                    row: Some(Row::Conflict),
                    run: || Box::pin(create_finds_a_different_object()),
                },
                Case {
                    fault: "a create that finds an object with different content, `enqueued` mode",
                    row: Some(Row::Conflict),
                    run: || Box::pin(create_finds_a_different_object_enqueued()),
                },
                Case {
                    fault: "a create that completes after the conflict poisoned the store",
                    row: Some(Row::Conflict),
                    run: || Box::pin(create_completes_after_the_store_was_poisoned()),
                },
                Case {
                    fault: "a created object the catalog rejects",
                    row: Some(Row::CatalogError),
                    run: || Box::pin(catalog_rejects_the_created_object()),
                },
                Case {
                    fault: "a create at the last representable object sequence",
                    row: Some(Row::LastSequence),
                    run: || Box::pin(create_takes_the_last_object_sequence()),
                },
                Case {
                    fault: "a listing that fails at recovery",
                    row: None,
                    run: || Box::pin(listing_fails_at_recovery()),
                },
                Case {
                    fault: "a fetch of a trailer that fails at recovery",
                    row: None,
                    run: || Box::pin(trailer_fetch_fails_at_recovery()),
                },
                Case {
                    fault: "a fetch of a footer that fails at recovery",
                    row: None,
                    run: || Box::pin(footer_fetch_fails_at_recovery()),
                },
                Case {
                    fault: "an object store that answers after a delay",
                    row: None,
                    run: || Box::pin(object_store_answers_slowly()),
                },
                Case {
                    fault: "a recovery the catalog rejects after every footer was fetched",
                    row: None,
                    run: || Box::pin(recovery_is_rejected_after_the_footers()),
                },
                Case {
                    fault: "a fetch of a segment that fails at a read",
                    row: None,
                    run: || Box::pin(segment_fetch_fails_at_a_read()),
                },
                Case {
                    fault: "a delete that fails at a collection",
                    row: None,
                    run: || Box::pin(delete_fails_at_a_collection()),
                },
                Case {
                    fault: "a store dropped while an append is stalled behind a create",
                    row: None,
                    run: || Box::pin(stalled_append_released_by_the_teardown()),
                },
                Case {
                    fault: "a create that stops while appends are stalled behind it",
                    row: None,
                    run: || Box::pin(stalled_appends_released_by_a_stop()),
                },
            ];

            common_telemetry::init_default_ut_logging();
            for case in &cases {
                common_telemetry::info!("Fault matrix case: {}", case.fault);
                (case.run)().await;
            }
            for row in ROWS {
                assert!(
                    cases.iter().any(|case| case.row == Some(row)),
                    "no case covers the failure matrix row {row:?}"
                );
            }
        }

        /// The create advances the sequence, the batch is acknowledged and the
        /// store stays healthy.
        async fn create_succeeds() {
            let before = Counters::sample();
            let store = open(memory_store(), &eager()).await;
            let region_id = region(1);

            let other = region(2);

            append(&store, region_id, "a1").await.unwrap();
            // A batch of several entries of two regions, so that the entries
            // the objects hold are not the count of the objects.
            let response = store
                .append_batch(vec![
                    entry(&store, region_id, "a2"),
                    entry(&store, region_id, "a3"),
                    entry(&store, other, "b1"),
                ])
                .await
                .unwrap();
            assert_eq!(Some(&id(1, 2)), response.last_entry_ids.get(&region_id));
            assert_eq!(Some(&id(1, 1)), response.last_entry_ids.get(&other));
            assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
            assert_eq!(
                entries(&[(id(0, 1), "a1"), (id(1, 1), "a2"), (id(1, 2), "a3")]),
                read(&store, region_id, 1).await
            );

            assert_indexed(store.io.as_ref(), &[0, 1]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: one_entry_object()
                        + object_bytes_of(&[(region_id, "a2"), (region_id, "a3"), (other, "b1")]),
                    object_entries: 4,
                    acknowledged_appends: 2,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The object of a create whose response was lost holds exactly what this
        /// create writes, so the conditional create accepts it as a retry of
        /// itself and the batch is acknowledged like any other.
        async fn create_finds_an_identical_object() {
            let io = Arc::new(FaultyIo::new());
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;

            io.reconcile_next_put.store(true, Ordering::Relaxed);
            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(0, 1)), response.last_entry_ids.get(&region_id));
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_eq!(
                entries(&[(id(0, 1), "a1")]),
                read(&store, region_id, 1).await
            );

            assert_indexed(io.as_ref(), &[0]).await;

            // The sequence advanced: the next batch takes the next one.
            append(&store, region_id, "a2").await.unwrap();
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0, 1]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    acknowledged_appends: 2,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The object is written and both the response and the read-back of the
        /// conditional create are lost: the append fails, the sequence rolls back
        /// and the retry of the same entries writes the same object, which the
        /// conditional create then reconciles.
        async fn create_response_is_lost() {
            let io = Arc::new(FaultyIo::new());
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;

            io.fail_after_next_put.store(true, Ordering::Relaxed);
            let error = append(&store, region_id, "a1").await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            // The object exists but was never indexed, so a read sees nothing
            // and neither gauge counts it.
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_eq!(0, latest(&store, region_id));
            assert!(read(&store, region_id, 1).await.is_empty());
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(0, 1)), response.last_entry_ids.get(&region_id));
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_eq!(
                entries(&[(id(0, 1), "a1")]),
                read(&store, region_id, 1).await
            );
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    acknowledged_appends: 1,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Every create fails the same way: no append is acknowledged, nothing is
        /// written and the store stays healthy, so the first append that the
        /// object store accepts takes the sequence and the ids of the first one.
        async fn object_store_is_unwritable() {
            let io = Arc::new(FaultyIo::new());
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;

            io.fail_puts.store(true, Ordering::Relaxed);
            for _ in 0..3 {
                let error = append(&store, region_id, "a1").await.unwrap_err();
                assert!(
                    matches!(
                        unwrap_shared(&error),
                        Error::WalObjectStore {
                            operation: "write",
                            ..
                        }
                    ),
                    "unexpected error: {error:?}"
                );
                assert_eq!(RetryHint::Retryable, error.retry_hint());
            }
            assert!(object_seqs(io.as_ref()).await.is_empty());
            assert_indexed(io.as_ref(), &[]).await;
            // The store is healthy, so a read works and sees nothing.
            assert!(read(&store, region_id, 1).await.is_empty());
            assert_eq!(
                Counters {
                    create_failures: 3,
                    sealed_objects: 3,
                    object_bytes: 3 * one_entry_object(),
                    object_entries: 3,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            io.fail_puts.store(false, Ordering::Relaxed);
            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(0, 1)), response.last_entry_ids.get(&region_id));
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                entries(&[(id(0, 1), "a1")]),
                read(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 3,
                    sealed_objects: 4,
                    object_bytes: 4 * one_entry_object(),
                    object_entries: 4,
                    acknowledged_appends: 1,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The later object cannot be rolled back, so the sequence stays where it
        /// is, both batches fail and the store poisons itself. The entries of the
        /// later object were never acknowledged, but a store opened afterwards
        /// indexes it and a read replays them.
        async fn create_fails_transiently_before_a_durable_object() {
            let object_store = memory_store();
            let (io, mut gates) = GatedIo::over(object_store.clone());
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;
            let (appends, mut open) = gated_appends(&store, region_id, &mut gates, 2).await;

            open.remove(0).send(false).unwrap();
            open.remove(0).send(true).unwrap();
            assert_appends_failed(appends, |error| {
                matches!(
                    error,
                    Error::WalObjectHistoryGap {
                        object_seq: 0,
                        later_object_seq: 1,
                        ..
                    }
                )
            })
            .await;
            assert_eq!(vec![1], object_seqs(io.as_ref()).await);
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            assert_read_is_terminal(&store, region_id).await;
            // The durable object was never indexed by this store.
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
            drop(store);

            let store = ObjectStoreLogStore::try_new(object_store, &eager())
                .await
                .unwrap();
            assert_eq!(
                entries(&[(id(1, 1), "a2")]),
                read(&store, region_id, 1).await
            );
            assert_indexed(store.io.as_ref(), &[1]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    recovered_objects: 1,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The append was acknowledged on admission, so nobody is left to retry:
        /// the store repeats the create under the same sequence and the entry
        /// becomes durable without the caller learning of the failure.
        async fn create_fails_transiently_enqueued() {
            let (io, mut gates) = GatedIo::new();
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &enqueued(eager())).await;

            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(0, 1)), response.last_entry_ids.get(&region_id));
            timeout(WAIT, gates.recv())
                .await
                .unwrap()
                .unwrap()
                .send(false)
                .unwrap();

            // The retry is held at its gate, so the failed attempt is what the
            // store stands at: the entry was acknowledged but is not durable,
            // nothing is written, nothing is indexed and the store is healthy.
            let retry = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
            assert!(object_seqs(io.as_ref()).await.is_empty());
            assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
            assert!(read(&store, region_id, 1).await.is_empty());
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            // The repeat writes the object under the same sequence and the
            // caller, which was answered on admission, learns nothing of it.
            retry.send(true).unwrap();
            timeout(WAIT, store.wait_durable(&provider(region_id), id(0, 1)))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_eq!(
                entries(&[(id(0, 1), "a1")]),
                read(&store, region_id, 1).await
            );
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Once stop began the create is no longer repeated: the acknowledged
        /// backlog is dropped, nothing is written, the store is not poisoned and
        /// `stop` reports the loss.
        async fn create_fails_transiently_after_stop_began() {
            let (io, mut gates) = GatedIo::new();
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &enqueued(manual())).await;
            append(&store, region_id, "a1").await.unwrap();
            let seal = {
                let store = store.clone();
                tokio::spawn(async move { store.seal_open_batch().await })
            };
            let gate = timeout(WAIT, gates.recv()).await.unwrap().unwrap();

            store.begin_stop();
            gate.send(false).unwrap();
            timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
            let error = store.stop().await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
            assert!(object_seqs(io.as_ref()).await.is_empty());
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        /// The object at the sequence holds different content: the sequence stays
        /// where it is, the waiter fails and the store poisons itself, so every
        /// later operation, a read included, fails with the same error.
        async fn create_finds_a_different_object() {
            let object_store = memory_store();
            let (io, mut gates) = GatedIo::over(object_store.clone());
            let region_id = region(1);
            let store = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap();
            put_foreign_object(object_store).await;
            let before = Counters::sample();
            let (appends, mut open) = gated_appends(&store, region_id, &mut gates, 2).await;

            // Object 1 is created but cannot be indexed before object 0, whose
            // create meets the foreign object: both waiters fail.
            open.remove(1).send(true).unwrap();
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert!(appends.iter().all(|append| !append.is_finished()));
            open.remove(0).send(true).unwrap();
            let path = io.object_path(0);
            assert_appends_failed(appends, |error| {
                matches!(error, Error::WalObjectConflict { path: actual, .. } if *actual == path)
            })
            .await;
            let error = store
                .read(&provider(region_id), 1, None)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            // The object that was created was never indexed.
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_conflicts: 1,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The conflict poisons the store before the create of the later batch
        /// completes, so that create has no batch left to index. Its object is
        /// durable all the same, and the write path counts it and times it;
        /// nothing else about the outcome changes.
        async fn create_completes_after_the_store_was_poisoned() {
            let object_store = memory_store();
            let (io, mut gates) = GatedIo::over(object_store.clone());
            let region_id = region(1);
            let store = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap();
            put_foreign_object(object_store).await;
            let before = Counters::sample();
            let (appends, mut open) = gated_appends(&store, region_id, &mut gates, 2).await;

            // Object 0 conflicts first: the store poisons itself and gives up
            // on the batch of object 1, whose create is still in flight.
            open.remove(0).send(true).unwrap();
            assert_appends_failed(appends, |error| {
                matches!(error, Error::WalObjectConflict { .. })
            })
            .await;
            assert_eq!(
                Counters {
                    create_conflicts: 1,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            // The abandoned create completes and its object is durable.
            open.remove(0).send(true).unwrap();
            wait_until(|| Counters::since(before).created_objects == 1).await;
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert_read_is_terminal(&store, region_id).await;
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_conflicts: 1,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Polls until `settled` holds, so a case can wait for what the actor
        /// records after it has answered every waiter.
        async fn wait_until(settled: impl Fn() -> bool) {
            let deadline = Instant::now() + WAIT;
            while !settled() {
                assert!(Instant::now() < deadline, "the actor never settled");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        /// The same conflict in the `enqueued` mode, where the acknowledgement
        /// already returned and cannot be revoked: it surfaces at the durability
        /// wait, and `stop` reports the acknowledged entry that was lost.
        async fn create_finds_a_different_object_enqueued() {
            let object_store = memory_store();
            let store = open(object_store.clone(), &enqueued(eager())).await;
            let region_id = region(1);
            ObjectStoreIo::new(object_store, PREFIX)
                .unwrap()
                .put_if_absent(0, Bytes::from_static(b"foreign"))
                .await
                .unwrap();
            let before = Counters::sample();

            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(0, 1)), response.last_entry_ids.get(&region_id));
            let error = timeout(WAIT, store.wait_durable(&provider(region_id), id(0, 1)))
                .await
                .unwrap()
                .unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            assert_read_is_terminal(&store, region_id).await;
            assert_indexed(store.io.as_ref(), &[]).await;
            let error = store.stop().await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(
                Counters {
                    create_conflicts: 1,
                    poisoned: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        /// The sequence of the batch is indexed while its create is in flight, so
        /// the catalog rejects the object the create returns: the waiter fails and
        /// the store poisons itself, leaving an object it never indexed, which a
        /// store opened afterwards indexes and a read replays.
        async fn catalog_rejects_the_created_object() {
            let object_store = memory_store();
            let (io, mut gates) = GatedIo::over(object_store.clone());
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;
            let (appends, mut open) = gated_appends(&store, region_id, &mut gates, 2).await;

            // Object 1 is created and waits behind object 0, whose sequence
            // the catalog is holding by the time its create returns: neither
            // batch is durable, so both waiters fail.
            open.remove(1).send(true).unwrap();
            store
                .catalog
                .write()
                .unwrap()
                .insert_object(0, vec![occupying_footer_entry()])
                .unwrap();
            open.remove(0).send(true).unwrap();
            assert_appends_failed(appends, |error| {
                matches!(error, Error::CorruptedWalObject { .. })
            })
            .await;
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            assert_read_is_terminal(&store, region_id).await;
            // Neither object was indexed by this store, whatever the catalog
            // the case put a sequence into holds.
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
            drop(store);

            // A store opened afterwards indexes the objects this one gave up
            // on, and a read replays them.
            let store = ObjectStoreLogStore::try_new(object_store, &eager())
                .await
                .unwrap();
            assert_eq!(
                entries(&[(id(0, 1), "a1"), (id(1, 1), "a2")]),
                read(&store, region_id, 1).await
            );
            assert_indexed(store.io.as_ref(), &[0, 1]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    poisoned: 1,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    recovered_objects: 2,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// A footer entry of a region the store does not write, so that indexing
        /// it occupies an object sequence.
        fn occupying_footer_entry() -> FooterEntry {
            FooterEntry {
                region_id: region(9),
                min_entry_id: 1,
                max_entry_id: 1,
                entry_count: 1,
                segment_offset: HEADER_LEN as u64,
                segment_len: 1,
                segment_crc32: 0,
            }
        }

        /// The batch takes the last sequence an entry id can name: it is created
        /// and acknowledged, and the store poisons itself because no later batch
        /// can be allocated a sequence.
        async fn create_takes_the_last_object_sequence() {
            let object_store = memory_store();
            let region_id = region(1);
            let last_object_seq = OBJECT_SEQ_LIMIT - 1;
            put_object(
                &object_store,
                last_object_seq - 1,
                region_id,
                &[id(last_object_seq - 1, 1)],
            )
            .await;
            let store = open(object_store, &eager()).await;
            // The entry of the second append is built while the store is
            // still healthy, since a poisoned store hands out none.
            let second = entry(&store, region_id, "a2");
            let before = Counters::sample();

            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(
                Some(&id(last_object_seq, 1)),
                response.last_entry_ids.get(&region_id)
            );
            let error = store.append_batch(vec![second]).await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectSequenceExhausted { last_object_seq: actual, .. }
                    if *actual == last_object_seq),
                "unexpected error: {error:?}"
            );
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            assert_read_is_terminal(&store, region_id).await;
            // The batch was acknowledged, so its object is indexed, alongside
            // the one the prefix was seeded with.
            assert_indexed(store.io.as_ref(), &[last_object_seq - 1, last_object_seq]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    poisoned: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    acknowledged_appends: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// No store is built, so nothing is acknowledged and nothing is poisoned;
        /// a retry recovers the whole prefix.
        async fn listing_fails_at_recovery() {
            let object_store = memory_store();
            populate(&object_store, 2, 2).await;
            zero_the_indexed_gauges().await;
            let io = Arc::new(FaultyIo::over(object_store));
            let before = Counters::sample();

            io.fail_next_list.store(true, Ordering::Relaxed);
            let error = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap_err();
            assert!(
                matches!(
                    &error,
                    Error::WalObjectStore {
                        operation: "list",
                        ..
                    }
                ),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            // Nothing of the prefix reached either gauge, and no object was
            // counted as recovered.
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(Counters::default(), Counters::since(before));

            let store = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap();
            assert_indexed(io.as_ref(), &[0, 1]).await;
            assert_eq!(2, read(&store, region(1), 1).await.len());
            assert_eq!(
                Counters {
                    recovered_objects: 2,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Opens a store on an empty prefix of its own, which puts both indexed
        /// gauges back to zero, so that a case can tell what the recovery it is
        /// about to fail published from what the prefix already held.
        async fn zero_the_indexed_gauges() {
            let store = open(memory_store(), &eager()).await;
            store.stop().await.unwrap();
            assert_eq!((0, 0), indexed_gauges());
        }

        /// The fetch of the window at the end of the object, which carries the
        /// trailer, fails: recovery abandons the prefix and no store is built, so
        /// nothing is acknowledged and nothing is poisoned.
        async fn trailer_fetch_fails_at_recovery() {
            let (object_store, trailer_offset, _) = put_object_with_a_long_footer().await;
            recovery_fails_on_a_read_at(object_store, trailer_offset).await;
        }

        /// The footer of the object starts before that window, so recovery
        /// fetches it separately; that fetch fails with the same outcome.
        async fn footer_fetch_fails_at_recovery() {
            let (object_store, _, footer_offset) = put_object_with_a_long_footer().await;
            recovery_fails_on_a_read_at(object_store, footer_offset).await;
        }

        async fn recovery_fails_on_a_read_at(object_store: ObjectStore, offset: u64) {
            zero_the_indexed_gauges().await;
            let io = Arc::new(FaultyIo::over(object_store));
            let before = Counters::sample();
            io.fail_reads_of.store(0, Ordering::Relaxed);
            io.fail_reads_at.store(offset, Ordering::Relaxed);

            let error = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                    if path == &io.object_path(0)),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            // The listing found the object, but the recovery that abandoned it
            // published neither gauge and counted nothing as recovered.
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(Counters::default(), Counters::since(before));

            io.fail_reads_of.store(u64::MAX, Ordering::Relaxed);
            let store = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap();
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(1, read(&store, region(1), 1).await.len());
            assert_eq!(
                Counters {
                    recovered_objects: 1,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The object store answers every request after a delay the case sets,
        /// which is how the timers of recovery, of a read and of the write path
        /// are read for the durations they recorded and not only for how many.
        /// Each is observed around two delays, so that a duration that is
        /// measured falls in the interval its delay forces and differs between
        /// the two; a timer that records one value cannot do both.
        async fn object_store_answers_slowly() {
            const SHORT: Duration = Duration::from_millis(20);
            const LONG: Duration = Duration::from_millis(120);
            let object_store = memory_store();
            let region_id = region(1);
            put_object(&object_store, 0, region_id, &[id(0, 1)]).await;
            let io = Arc::new(FaultyIo::over(object_store));
            let before = Counters::sample();

            let mut recoveries = Vec::new();
            let mut reads = Vec::new();
            let mut seals = Vec::new();
            let mut acknowledgements = Vec::new();
            for (delay, object_seq) in [(SHORT, 1), (LONG, 2)] {
                io.answer_after_millis
                    .store(delay.as_millis() as u64, Ordering::Relaxed);

                let timer = timer_before(&METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS);
                io.forget_waits();
                let started = Instant::now();
                let store = ObjectStoreLogStore::open(io.clone(), &eager())
                    .await
                    .unwrap();
                recoveries.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS,
                    timer,
                    io.shortest_wait(),
                    started.elapsed(),
                ));

                let timer = timer_before(&METRIC_OBJECT_STORE_WAL_READ_SECONDS);
                io.forget_waits();
                let started = Instant::now();
                assert_eq!(object_seq as usize, read(&store, region_id, 1).await.len());
                reads.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_READ_SECONDS,
                    timer,
                    io.shortest_wait(),
                    started.elapsed(),
                ));

                let seal = timer_before(&METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS);
                let acknowledgement = timer_before(&METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS);
                io.forget_waits();
                let started = Instant::now();
                append(&store, region_id, "a1").await.unwrap();
                let elapsed = started.elapsed();
                let waited = io.shortest_wait();
                seals.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS,
                    seal,
                    waited,
                    elapsed,
                ));
                acknowledgements.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS,
                    acknowledgement,
                    waited,
                    elapsed,
                ));

                assert_eq!(
                    (0..=object_seq).collect::<Vec<_>>(),
                    object_seqs(io.as_ref()).await
                );
                store.stop().await.unwrap();
            }
            Timed::assert_pair("recovery", recoveries[0], recoveries[1]);
            Timed::assert_pair("read", reads[0], reads[1]);
            Timed::assert_pair("seal to durable", seals[0], seals[1]);
            Timed::assert_pair(
                "append acknowledgement",
                acknowledgements[0],
                acknowledgements[1],
            );

            io.answer_after_millis.store(0, Ordering::Relaxed);
            assert_indexed(io.as_ref(), &[0, 1, 2]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    acknowledged_appends: 2,
                    recovered_objects: 3,
                    recoveries: 2,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        /// Every footer is fetched and the catalog then rejects the prefix,
        /// which is the other way a recovery is abandoned: the objects it read
        /// are counted as recovered no more than the ones a failed fetch left
        /// unread, and neither gauge is published.
        async fn recovery_is_rejected_after_the_footers() {
            let object_store = memory_store();
            let region_id = region(1);
            // The entry ranges of the region do not increase with the object
            // sequence, which the catalog refuses to index.
            put_object(&object_store, 0, region_id, &[id(3, 1), id(3, 2)]).await;
            put_object(&object_store, 1, region_id, &[id(2, 1)]).await;
            zero_the_indexed_gauges().await;
            let io = Arc::new(FaultyIo::over(object_store));
            let before = Counters::sample();

            let error = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::InvalidWalObject { path, source, .. }
                    if path == &io.object_path(1)
                        && matches!(**source, Error::CorruptedWalObject { .. })),
                "unexpected error: {error:?}"
            );
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(Counters::default(), Counters::since(before));
        }

        /// Writes one object whose footer is longer than the window recovery reads
        /// from the end of an object, so that recovery fetches its header, that
        /// window and its footer separately, and returns the offsets of the window
        /// and of the footer.
        async fn put_object_with_a_long_footer() -> (ObjectStore, u64, u64) {
            let object_store = memory_store();
            let regions = (RECOVERY_TAIL_WINDOW / FOOTER_ENTRY_LEN + 100) as u32;
            let store = open(object_store.clone(), &eager()).await;
            let wide_entries = (1..=regions)
                .map(|number| entry(&store, region(number), "wide"))
                .collect::<Vec<_>>();
            store.append_batch(wide_entries).await.unwrap();
            store.stop().await.unwrap();
            drop(store);

            let path = object_path(&object_store, 0);
            let bytes = object_store.read(&path).await.unwrap().to_vec();
            let (trailer, _) = footer_of(&bytes);
            assert!(trailer.footer_offset < (bytes.len() - RECOVERY_TAIL_WINDOW) as u64);
            (
                object_store,
                (bytes.len() - RECOVERY_TAIL_WINDOW) as u64,
                trailer.footer_offset,
            )
        }

        /// The read fails rather than skipping the segment: no hole is recorded,
        /// the store stays healthy and a read after the object store recovers
        /// returns every entry.
        async fn segment_fetch_fails_at_a_read() {
            let object_store = memory_store();
            populate(&object_store, 2, 1).await;
            let io = Arc::new(FaultyIo::over(object_store));
            let region_id = region(1);
            let (store, before) = opened_over(io.clone(), &eager()).await;

            io.fail_reads_of.store(1, Ordering::Relaxed);
            let error = store
                .read(&provider(region_id), 1, None)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                    if path == &io.object_path(1)),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
            assert_indexed(io.as_ref(), &[0, 1]).await;

            io.fail_reads_of.store(u64::MAX, Ordering::Relaxed);
            assert_eq!(2, read(&store, region_id, 1).await.len());
            assert_indexed(io.as_ref(), &[0, 1]).await;
            assert_eq!(
                Counters {
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The object stays present and indexed and the store stays healthy,
        /// so the indexed gauges do not move; the delete that succeeds takes
        /// the object off both of them. That a failed delete is repeated at
        /// the next collection is covered on its own.
        async fn delete_fails_at_a_collection() {
            let io = Arc::new(FaultyIo::new());
            let store = ObjectStoreLogStore::open(io.clone(), &eager())
                .await
                .unwrap();
            let region_id = region(1);
            append(&store, region_id, "a1").await.unwrap();
            append(&store, region_id, "a2").await.unwrap();
            let before = Counters::sample();

            io.fail_next_delete.store(true, Ordering::Relaxed);
            obsolete_and_collect(&store, region_id, id(0, 1)).await;
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert!(is_indexed(&store, 0));
            assert_indexed(io.as_ref(), &[0, 1]).await;
            // The object is still there, and a read hides its entries because
            // the watermark covers them, not because it was collected.
            assert_eq!(
                entries(&[(id(1, 1), "a2")]),
                read(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    failed_deletes: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            // The object the next collection deletes leaves both gauges.
            obsolete_and_collect(&store, region_id, id(0, 1)).await;
            assert_eq!(vec![1], object_seqs(io.as_ref()).await);
            assert!(!is_indexed(&store, 0));
            assert_indexed(io.as_ref(), &[1]).await;
            assert_eq!(
                entries(&[(id(1, 1), "a2")]),
                read(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    deleted_objects: 1,
                    failed_deletes: 1,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The store is dropped rather than stopped while an append is still
        /// held back, so the actor is torn down with the queue as it stands:
        /// the append is answered by nothing, its caller having gone, and the
        /// wait it spent in the queue is recorded like any other.
        async fn stalled_append_released_by_the_teardown() {
            let config = ObjectStoreWalConfig {
                max_unpersisted_bytes: ReadableSize(1),
                ..enqueued(manual())
            };
            let before = Counters::sample();
            let (store, io, gates, stalled, gate) = stall_second_append(config).await;
            assert_eq!(
                Counters {
                    stalled_appends: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    recoveries: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            // The caller of the stalled append gives up and the last handle to
            // the store goes with it, so the actor sees its commands end. The
            // create stays parked, and is dropped with the actor rather than
            // released, which is why the gate outlives the store here.
            stalled.abort();
            drop(store);
            wait_until(|| Counters::since(before).stalled_waits == 1).await;
            drop(gate);
            assert!(object_seqs(io.as_ref()).await.is_empty());
            assert_indexed(io.as_ref(), &[]).await;
            assert_eq!(
                Counters {
                    stalled_appends: 1,
                    stalled_waits: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    recoveries: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            drop(gates);
        }

        /// An append the backlog thresholds hold back is never admitted, since
        /// the create it waits for is still parked when `stop` begins: it
        /// receives the stopped error, and the wait it spent in the queue is
        /// recorded all the same. Those are the longest waits there are, so
        /// dropping them would bias the histogram towards the healthy path.
        async fn stalled_appends_released_by_a_stop() {
            let config = ObjectStoreWalConfig {
                max_unpersisted_bytes: ReadableSize(1),
                ..enqueued(manual())
            };
            const SHORT: Duration = Duration::from_millis(20);
            const LONG: Duration = Duration::from_millis(120);
            let region_id = region(1);
            let before = Counters::sample();
            let stalled_at = Instant::now();
            let (store, io, mut gates, stalled, gate) = stall_second_append(config).await;
            assert_eq!(
                Counters {
                    stalled_appends: 1,
                    sealed_objects: 1,
                    object_bytes: one_entry_object(),
                    object_entries: 1,
                    recoveries: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            // The first stalled append waits for the upload and is admitted,
            // which is one of the two ways out of the queue.
            let timer = timer_before(&METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS);
            let holding_since = Instant::now();
            tokio::time::sleep(SHORT).await;
            let held = holding_since.elapsed();
            gate.send(true).unwrap();
            timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
            let admitted = Timed::since(
                &METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
                timer,
                held,
                stalled_at.elapsed(),
            );

            // The admitted entry puts the backlog back at the threshold, so the
            // next append stalls behind the upload it seals; the stop releases
            // it with the stopped error instead, which is the other way out.
            let stalled_at = Instant::now();
            let refused = {
                let store = store.clone();
                tokio::spawn(async move { append(&store, region_id, "a3").await })
            };
            let sealed = timeout(WAIT, gates.recv()).await.unwrap().unwrap();
            let timer = timer_before(&METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS);
            let holding_since = Instant::now();
            tokio::time::sleep(LONG).await;
            let held = holding_since.elapsed();
            let stop = {
                let store = store.clone();
                tokio::spawn(async move { store.stop().await })
            };
            let error = timeout(WAIT, refused).await.unwrap().unwrap().unwrap_err();
            let held_back = Timed::since(
                &METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
                timer,
                held,
                stalled_at.elapsed(),
            );
            assert!(
                matches!(error, Error::ObjectStoreWalStopped { .. }),
                "unexpected error: {error:?}"
            );
            Timed::assert_pair("stalled append", admitted, held_back);

            sealed.send(true).unwrap();
            timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0, 1]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    recoveries: 1,
                    stalled_appends: 2,
                    stalled_waits: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }
    }
}

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

//! Object store WAL construction, recovery, writes and region reads.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use bytes::Bytes;
use common_telemetry::{debug, error, info, warn};
use common_wal::config::object_store::{AckMode, CorruptedSegmentAction, ObjectStoreWalConfig};
use futures::future::{BoxFuture, OptionFuture};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use object_store::{ErrorKind, ObjectStore};
use snafu::{IntoError, OptionExt, ResultExt, ensure};
use store_api::logstore::entry::{Entry, NaiveEntry};
use store_api::logstore::provider::{ObjectStoreProvider, Provider};
use store_api::logstore::{AppendBatchResponse, EntryId, LogStore, SendableEntryStream};
use store_api::storage::RegionId;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tokio_util::task::AbortOnDropHandle;

use crate::error::{
    CorruptedWalObjectSnafu, Error, IncompleteWalEntrySnafu, InvalidProviderSnafu,
    InvalidWalObjectSnafu, InvalidWalObjectStoreSnafu, MismatchedWalPrefixSnafu,
    MismatchedWalRegionSnafu, MissingWalHistorySnafu, ObjectStoreWalFailingSnafu,
    ObjectStoreWalSnafu, ObjectStoreWalStoppedSnafu, Result, UnconfirmedWalEpochStartSnafu,
    WalObjectSequenceExhaustedSnafu, WalObjectSequenceUnsettledSnafu, WalObjectUnconfirmedSnafu,
    WalSegmentFetchSnafu, WalStartWithdrawnSnafu,
};
use crate::metrics::{
    METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS, METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL,
    METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL, METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL,
    METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL, METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL,
    METRIC_OBJECT_STORE_WAL_FAILING, METRIC_OBJECT_STORE_WAL_INDEXED_BYTES,
    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS,
    METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL, METRIC_OBJECT_STORE_WAL_OBJECT_BYTES,
    METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES, METRIC_OBJECT_STORE_WAL_POISONED_TOTAL,
    METRIC_OBJECT_STORE_WAL_READ_SECONDS, METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL,
    METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS, METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL,
    METRIC_OBJECT_STORE_WAL_RELEASE_RECORDS_TOTAL, METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS,
    METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL, METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
    METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL,
};
use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, OpenBatch, entry_id, sequence_floor};
use crate::object_store_wal::catalog::ObjectCatalog;
use crate::object_store_wal::format::{
    ChainLink, EncodedObject, FixedTrailer, FooterEntry, HEADER_LEN, Header, MIN_OBJECT_LEN,
    RELEASE_REGION_ID, Record, Release, TRAILER_LEN, decode_footer, decode_header, decode_releases,
    decode_segment, decode_trailer, encode_object, encode_releases, footer_range, object_len,
    verify_segment_ranges,
};
use crate::object_store_wal::io::{ListedObject, ObjectStoreIo, PutResult};

const COMMAND_BUFFER: usize = 1024;
/// Appends queued for the actor; a caller beyond them waits to send.
const APPEND_BUFFER: usize = 16;
const MIN_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
const MAX_IN_FLIGHT_CREATES: usize = 4;
const MAX_IN_FLIGHT_DELETES: usize = 4;
/// Delay before a create that failed transiently is attempted again: every
/// append was answered on admission, so no caller is left to retry it.
const CREATE_RETRY_DELAY: Duration = Duration::from_millis(100);
/// Longest delay from a failed attempt of a backlog that the object store
/// refuses with an error that requires intervention, see [`Failing`], to the
/// next attempt.
/// The delay doubles from [`CREATE_RETRY_DELAY`] after every attempt. At the
/// cap a dead bucket costs four requests a minute, a conditional create and
/// the read that reconciles it every 30 seconds; a longer cap would keep
/// appends rejected longer after the cause is fixed.
const MAX_FAILING_RETRY_DELAY: Duration = Duration::from_secs(30);
/// How often a store logs that the object store still refuses its backlog.
const FAILING_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Number of sealed batches that may wait to be created or indexed. One
/// admission can seal two batches, the open batch before an append that would
/// exhaust its positions and the append's own batch, so the actor admits an
/// append only while two more fit and holds it back otherwise: a stalled
/// object store holds callers back instead of growing the backlog. No other
/// seal passes the limit either: a release, a timer tick and `stop` wait for
/// room, see [`Actor::seal`].
const MAX_SEALED_BATCHES: usize = 2 * MAX_IN_FLIGHT_CREATES;
/// Number of sequences above the newest object a recovery lists that an
/// earlier instance can have handed out, which this instance therefore never
/// assigns.
///
/// An instance hands out a sequence when it admits the first entry into the
/// open batch, which keeps it when it is sealed. Every batch it sealed below
/// its lowest batch that is not indexed is indexed, hence durable and listed,
/// or collected below a later indexed object. The sealed batches and the open
/// batch take consecutive sequences: a raised sequence floor moves the next
/// sequence only while no batch is sealed or open, and the batch that marks
/// the floor is indexed before any other batch is sealed, see
/// [`Actor::raise_sequence_floor`]. The sealed batches that are not indexed
/// and an open batch that holds ids take at most [`MAX_SEALED_BATCHES`]
/// sequences together: an admission needs room for two more sealed batches,
/// so it leaves at most `MAX_SEALED_BATCHES - 1` sealed batches beside the
/// open batch, also when it first seals the open batch because the append
/// would exhaust its positions; every later seal takes the open batch with
/// it, and a release, a timer tick and `stop` seal only while fewer than
/// [`MAX_SEALED_BATCHES`] batches are sealed. So every sequence an earlier
/// instance handed out is at most this far above the newest listed object,
/// or is the floor object of a raise whose object was never indexed, which
/// holds no entry.
pub const UNLISTED_SEQUENCES: u64 = MAX_SEALED_BATCHES as u64;
/// How long after an open, or after the last recheck, the collections delete
/// the sequences the start object skipped again, see [`UNLISTED_SEQUENCES`]:
/// the delete of an absent object succeeds, and a late object of an earlier
/// instance can land at its sequence afterwards, at any time, since nothing
/// bounds when a write that its client gave up on completes. The interval
/// doubles after every recheck up to [`MAX_LATE_OBJECT_RECHECK`], so the
/// rechecks of a store cost a few deletes an hour once it has run a while.
#[cfg(not(test))]
const LATE_OBJECT_RECHECK: Duration = Duration::from_secs(60);
#[cfg(test)]
const LATE_OBJECT_RECHECK: Duration = Duration::from_millis(50);
/// Longest interval between two rechecks of the skipped sequences.
#[cfg(not(test))]
const MAX_LATE_OBJECT_RECHECK: Duration = Duration::from_secs(3600);
#[cfg(test)]
const MAX_LATE_OBJECT_RECHECK: Duration = Duration::from_millis(100);
/// Number of objects whose footers recovery fetches at a time. On S3 an
/// object costs two reads, its header and its tail, tens of milliseconds per
/// lane however many vCPUs the node has, and recovery time scales with objects
/// divided by concurrency up to 128 lanes without throttling: at 64 about
/// 10,000 objects recover in about 10 s instead of about 100 s at 8, while the
/// reads use about a third of S3's request rate per prefix, leaving room for
/// the data file reads of start-up. Each lane holds one
/// [`RECOVERY_TAIL_WINDOW`], 4 MiB in all.
const RECOVERY_CONCURRENCY: usize = 64;
/// Number of segments a read fetches ahead of the one whose entries it
/// yields. On S3 a replay that fetched each segment after the previous one
/// spent about 30 ms per object; with eight fetches in flight a segment
/// arrives about every 4 ms. The segments fetched ahead also take their bytes
/// from [`READ_PREFETCH_BYTES`].
const READ_PREFETCH_DEPTH: usize = 8;
/// Bytes of decoded segments that all reads of a store together fetch ahead; a
/// segment counts as its length, which bounds its payloads, plus a [`Record`]
/// per entry. A read always fetches the segment it yields next, as a read that
/// fetched one segment at a time did, and a later segment only while it fits
/// in what is left, so regions replayed in parallel hold one segment each, up
/// to about `max_batch_bytes` or more for one oversized append, plus at most
/// these bytes, however many regions open at once. While a fetch copies the
/// payloads out of the downloaded segment it briefly holds the segment's
/// length once more.
const READ_PREFETCH_BYTES: usize = 64 * 1024 * 1024;
/// Bytes recovery reads from the end of an object in one request. The window
/// holds the trailer and the footer of an object with up to 1364 regions of
/// 48 bytes each, so a second request for the footer is rare.
const RECOVERY_TAIL_WINDOW: usize = 64 * 1024;

/// A log store over the immutable WAL objects under one prefix.
///
/// Appends are admitted into an open batch and return on admission with their
/// entry ids. A background actor seals the batch when it reaches the size
/// limit or the flush interval elapses and creates the object under the next
/// sequence while it keeps admitting entries into the next batch; up to
/// [`MAX_IN_FLIGHT_CREATES`] creates run at a time. Once a caller asks the
/// store to drain, it seals the open batch at once and refuses later appends.
/// Objects are indexed in the catalog in sequence order, and an entry is durable once its object is
/// indexed. In the `durable` acknowledgement mode the caller waits with
/// [`wait_write_durable`](LogStore::wait_write_durable) before it answers a
/// writer; in the `enqueued` mode it answers at once. While
/// [`MAX_SEALED_BATCHES`] batches wait to become durable, or the unpersisted
/// backlog is at a threshold, no append is admitted, and appends are rejected
/// while the store is failing: the object store refuses the backlog with an
/// error that requires intervention, or an upload did not complete within
/// `max_unpersisted_age`. A segment that a read cannot decode is handled as
/// `on_corrupted_segment` says. Objects below the reclaim boundary of the last
/// indexed object are deleted in the background. A release of a region seals
/// an object with a release record of the region and returns once that
/// object is durable and indexed.
pub struct ObjectStoreLogStore {
    prefix: String,
    /// The storage the prefix is read from, as configured, for messages.
    storage: String,
    ack_mode: AckMode,
    on_corrupted_segment: CorruptedSegmentAction,
    accept_missing_history: bool,
    /// Whether the prefix held no object when the store opened.
    found_empty: bool,
    /// The sequence above the newest object holding entries that recovery
    /// indexed, zero when it indexed none.
    history_end: u64,
    started_regions: StartedRegions,
    io: Arc<dyn WalObjectIo>,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    /// Segments that reads skipped, per region and by object sequence.
    wal_holes: WalHoles,
    /// Objects with a delete in flight, which a read consults.
    deleting: Arc<DeletingObjects>,
    /// Bytes the reads may fetch ahead, see [`READ_PREFETCH_BYTES`].
    read_prefetch_budget: Arc<Semaphore>,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    /// Whether the last create of the backlog failed, see
    /// [`is_failing`](LogStore::is_failing).
    last_create_failed: Arc<AtomicBool>,
    /// Notified by [`begin_drain`](LogStore::begin_drain).
    drain_requested: Arc<Notify>,
    command_tx: mpsc::Sender<Command>,
    append_tx: mpsc::Sender<QueuedAppend>,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    held_back_appends: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Sender<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
    #[cfg(any(test, feature = "testing"))]
    next_create_fails_after_write: Arc<AtomicBool>,
    #[cfg(any(test, feature = "testing"))]
    parked_creates: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    durability_waits: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    failed_creates: watch::Receiver<usize>,
    #[cfg(any(test, feature = "testing"))]
    failing_state: watch::Receiver<bool>,
}

type ObsoleteEntryIds = Arc<Mutex<HashMap<RegionId, EntryId>>>;

/// Regions whose history starts in the store: those created on it and those
/// it opened by accepting a missing history. The store registers a region and
/// the actor decides a withdrawal under this lock, so neither misses the
/// other.
type StartedRegions = Arc<Mutex<HashSet<RegionId>>>;

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

type TerminalError = Arc<Mutex<Option<Arc<Error>>>>;

/// The delete attempts in flight, one per object.
///
/// The actor registers an attempt before its delete starts, unindexes the
/// object if the delete succeeded, and only then settles the attempt and takes
/// it out, so an object without an attempt was either never collected or is
/// already unindexed. [`is_collected`] depends on that order.
#[derive(Debug, Default)]
struct DeletingObjects {
    /// The settlement flag of the attempt in flight per object sequence.
    attempts: Mutex<BTreeMap<u64, watch::Sender<bool>>>,
}

impl DeletingObjects {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, watch::Sender<bool>>> {
        self.attempts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn start(&self, object_seq: u64) {
        self.lock().insert(object_seq, watch::channel(false).0);
    }

    fn contains(&self, object_seq: u64) -> bool {
        self.lock().contains_key(&object_seq)
    }

    /// Settles the attempt of `object_seq` and takes it out, releasing the
    /// reads that wait for it.
    fn finish(&self, object_seq: u64) {
        if let Some(settled) = self.lock().remove(&object_seq) {
            settled.send_replace(true);
        }
    }

    /// Settles every attempt the actor will never complete because it exited:
    /// their objects stay indexed, so waiting reads report their error.
    fn abandon_all(&self) {
        for (_, settled) in std::mem::take(&mut *self.lock()) {
            settled.send_replace(true);
        }
    }

    /// Returns the settlement of the attempt in flight for `object_seq`, or
    /// `None` when no delete of it is in flight.
    fn attempt(&self, object_seq: u64) -> Option<watch::Receiver<bool>> {
        self.lock().get(&object_seq).map(watch::Sender::subscribe)
    }

    /// Waits until no delete is in flight.
    #[cfg(any(test, feature = "testing"))]
    async fn wait_until_empty(&self) {
        loop {
            let in_flight = self
                .lock()
                .values()
                .map(watch::Sender::subscribe)
                .collect::<Vec<_>>();
            if in_flight.is_empty() {
                return;
            }
            for mut settled in in_flight {
                let _ = settled.wait_for(|settled| *settled).await;
            }
        }
    }
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
    /// Builds the store under the node and generation prefix derived from `config`,
    /// recovering the catalog from the objects that already exist and writing
    /// the object that starts the epoch of this instance. Recovery fails on the
    /// first corrupted or conflicting object.
    pub async fn try_new(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
        node_id: u64,
        generation: u64,
    ) -> Result<Arc<Self>> {
        let prefix = config.node_prefix(node_id, generation);
        let io = ObjectStoreIo::new(object_store, &prefix)?;
        Self::open(Arc::new(io), config, prefix).await
    }

    async fn open(
        io: Arc<dyn WalObjectIo>,
        config: &ObjectStoreWalConfig,
        prefix: String,
    ) -> Result<Arc<Self>> {
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
        let recovery_started_at = Instant::now();
        let Recovered {
            mut catalog,
            next_object_seq,
            durable_entry_ids,
            tip,
            max_epoch,
            reclaim_boundary,
            unindexed,
            released_entry_ids,
            recovered_objects,
        } = recover(io.as_ref()).await?;
        // Every epoch is one above the sequence of the start object its
        // instance created, and every object is at or above its start object,
        // so an epoch above the next sequence names no instance that ran.
        ensure!(
            max_epoch <= next_object_seq,
            CorruptedWalObjectSnafu {
                reason: format!(
                    "an object carries epoch {max_epoch}, above the next sequence {next_object_seq}"
                ),
            }
        );
        let found_empty = next_object_seq == 0;
        let history_end = durable_entry_ids
            .values()
            .map(|entry_id| sequence_floor(*entry_id))
            .max()
            .unwrap_or(0);
        // Only a recovery that succeeded is reported, so that the objects
        // counted and the time measured are those of the same recoveries.
        METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL.inc_by(recovered_objects);
        METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS
            .observe(recovery_started_at.elapsed().as_secs_f64());
        publish_indexed(&catalog);
        // A chain without any segment holds no history, so the start object
        // reclaims every object below it. A start object that another
        // instance withdraws after this recovery listed it is then below the
        // boundary, and its delete leaves this chain complete.
        // An earlier instance can have handed out sequences above the newest
        // listed object, whose objects can land after the listing; this
        // instance starts above them and never assigns one, see
        // [`UNLISTED_SEQUENCES`]. An empty prefix had no instance that handed
        // out a sequence: an instance hands one out only after its start
        // object is created.
        let start_object_seq = if found_empty {
            0
        } else {
            next_object_seq
                .checked_add(UNLISTED_SEQUENCES)
                .filter(|object_seq| *object_seq < OBJECT_SEQ_LIMIT)
                .context(WalObjectSequenceExhaustedSnafu {
                    last_object_seq: next_object_seq - 1,
                })?
        };
        let start_boundary = if durable_entry_ids.is_empty() {
            start_object_seq
        } else {
            reclaim_boundary
        };
        let (start, moved_past) =
            start_epoch(io.as_ref(), start_object_seq, tip, start_boundary).await?;
        let epoch = start.epoch;
        // The epoch identifies this instance in every object it writes.
        info!(
            "Opened object store WAL under {prefix} at epoch {epoch}, start object {}",
            start.object_seq
        );
        catalog
            .insert_object(start.object_seq, Vec::new())
            .with_context(|_| InvalidWalObjectSnafu {
                path: io.object_path(start.object_seq),
            })?;
        METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.inc();
        METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.add(object_len(&[]) as i64);
        // The release records are the only watermarks recorded yet.
        let writer_reclaim_boundary =
            catalog.reclaim_boundary(reclaim_boundary, &released_entry_ids);
        let catalog = Arc::new(RwLock::new(catalog));
        let obsolete_entry_ids = Arc::new(Mutex::new(released_entry_ids));
        let wal_holes = WalHoles::default();
        let deleting = Arc::new(DeletingObjects::default());
        let terminal_error = TerminalError::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let last_create_failed = Arc::new(AtomicBool::new(false));
        let drain_requested = Arc::new(Notify::new());
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let (append_tx, append_rx) = mpsc::channel(APPEND_BUFFER);
        #[cfg(any(test, feature = "testing"))]
        let (admitted_appends_tx, admitted_appends_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (held_back_appends_tx, held_back_appends_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (creates_held_tx, creates_held_rx) = watch::channel(false);
        #[cfg(any(test, feature = "testing"))]
        let creates_fail = Arc::new(AtomicBool::new(false));
        #[cfg(any(test, feature = "testing"))]
        let next_create_fails_after_write = Arc::new(AtomicBool::new(false));
        #[cfg(any(test, feature = "testing"))]
        let (parked_creates_tx, parked_creates_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (durability_waits_tx, durability_waits_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (failed_creates_tx, failed_creates_rx) = watch::channel(0);
        #[cfg(any(test, feature = "testing"))]
        let (failing_tx, failing_rx) = watch::channel(false);

        let started_regions = StartedRegions::default();
        let actor = Actor {
            io: io.clone(),
            started_regions: started_regions.clone(),
            withdrawal: Withdrawal::Kept,
            catalog: catalog.clone(),
            obsolete_entry_ids: obsolete_entry_ids.clone(),
            wal_holes: wal_holes.clone(),
            terminal_error: terminal_error.clone(),
            stopped: stopped.clone(),
            last_create_failed: last_create_failed.clone(),
            drain_requested: drain_requested.clone(),
            command_rx,
            append_rx,
            ack_mode: config.ack_mode,
            max_unpersisted_bytes,
            max_unpersisted_age: config.max_unpersisted_age,
            open_batch: OpenBatch::new(max_batch_bytes),
            issued_entry_ids: durable_entry_ids,
            open_admitted_at: Vec::new(),
            sealed: VecDeque::new(),
            creates: FuturesUnordered::new(),
            stalled: None,
            failing: None,
            transient_bound: config.max_unpersisted_age,
            healthy_since: Instant::now(),
            stop_began_at: None,
            held_releases: Vec::new(),
            durable_waiters: DurableWaiters::default(),
            draining: false,
            stop: Vec::new(),
            stop_error: None,
            next_object_seq: start
                .object_seq
                .checked_add(1)
                .filter(|next_object_seq| *next_object_seq < OBJECT_SEQ_LIMIT),
            epoch,
            last_indexed: start,
            reclaim_boundary: writer_reclaim_boundary,
            durable_reclaim_boundary: reclaim_boundary,
            // A late object of an earlier instance in the skipped sequences
            // is collected once the boundary passes it.
            unindexed: unindexed
                .into_iter()
                .chain(next_object_seq..start_object_seq)
                .chain(moved_past)
                .collect(),
            skipped: (next_object_seq < start_object_seq).then(|| SkippedSequences {
                sequences: next_object_seq..start_object_seq,
                interval: LATE_OBJECT_RECHECK,
                next_check: Instant::now() + LATE_OBJECT_RECHECK,
            }),
            rechecked: HashSet::new(),
            failed_releases: BTreeMap::new(),
            creating: HashSet::new(),
            deleting: deleting.clone(),
            deletes: FuturesUnordered::new(),
            failed_deletes: HashSet::new(),
            flush_interval: config.flush_interval,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_tx,
            #[cfg(any(test, feature = "testing"))]
            held_back_appends: held_back_appends_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail: creates_fail.clone(),
            #[cfg(any(test, feature = "testing"))]
            next_create_fails_after_write: next_create_fails_after_write.clone(),
            #[cfg(any(test, feature = "testing"))]
            parked_creates: Arc::new(parked_creates_tx),
            #[cfg(any(test, feature = "testing"))]
            durability_waits: durability_waits_tx,
            #[cfg(any(test, feature = "testing"))]
            failed_creates: failed_creates_tx,
            #[cfg(any(test, feature = "testing"))]
            failing_state: failing_tx,
        };
        #[cfg(test)]
        LIVE_ACTORS.fetch_add(1, Ordering::SeqCst);
        common_runtime::spawn_global(actor.run());
        let storage = match config.storage_provider.as_str() {
            "" => format!("the default storage ({})", io.storage()),
            name => format!("storage provider {name:?} ({})", io.storage()),
        };
        Ok(Arc::new(Self {
            prefix,
            storage,
            ack_mode: config.ack_mode,
            on_corrupted_segment: config.on_corrupted_segment,
            accept_missing_history: config.accept_missing_history,
            found_empty,
            history_end,
            started_regions,
            io,
            catalog,
            obsolete_entry_ids,
            wal_holes,
            deleting,
            read_prefetch_budget: Arc::new(Semaphore::new(READ_PREFETCH_BYTES)),
            terminal_error,
            stopped,
            last_create_failed,
            drain_requested,
            command_tx,
            append_tx,
            #[cfg(any(test, feature = "testing"))]
            admitted_appends: admitted_appends_rx,
            #[cfg(any(test, feature = "testing"))]
            held_back_appends: held_back_appends_rx,
            #[cfg(any(test, feature = "testing"))]
            creates_held: creates_held_tx,
            #[cfg(any(test, feature = "testing"))]
            creates_fail,
            #[cfg(any(test, feature = "testing"))]
            next_create_fails_after_write,
            #[cfg(any(test, feature = "testing"))]
            parked_creates: parked_creates_rx,
            #[cfg(any(test, feature = "testing"))]
            durability_waits: durability_waits_rx,
            #[cfg(any(test, feature = "testing"))]
            failed_creates: failed_creates_rx,
            #[cfg(any(test, feature = "testing"))]
            failing_state: failing_rx,
        }))
    }

    /// Returns the largest entry id of the provider's region whose object is
    /// durable and indexed, or zero for a region without such entries. In the
    /// `enqueued` acknowledgement mode an entry id that an append returned
    /// stays above this value until the object holding it is created.
    pub(crate) fn durable_entry_id(&self, provider: &Provider) -> Result<EntryId> {
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
    /// records them again when its reads meet the segments.
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

    /// Begins stopping the store; [`stop`](LogStore::stop) finishes it. From
    /// now on no append or release is admitted: the append held back, the
    /// appends queued for the actor and the releases waiting for their object
    /// fail with the stopped error. The open batch is sealed and the backlog
    /// keeps uploading, so the callers waiting for its entries are answered
    /// once their objects are durable, or with the error that drops the
    /// backlog when an upload fails or does not complete within
    /// `max_unpersisted_age` of the stop beginning.
    pub async fn begin_stop(&self) {
        self.stopped.store(true, Ordering::Release);
        // A closed channel means the actor already exited.
        let _ = self.command_tx.send(Command::BeginStop).await;
    }

    /// Waits until every entry of the region with an id at or below
    /// `entry_id` is durable and indexed. A durable entry is answered at
    /// once, also by a store that recorded a terminal error; the actor
    /// answers the others, see [`Actor::handle_wait_durable`].
    async fn wait(&self, provider: &Provider, entry_id: EntryId, kind: WaitKind) -> Result<()> {
        let region_id = self.region_of(provider)?;
        if entry_id <= self.durable_entry_id_of(region_id) {
            return Ok(());
        }
        let (response_tx, response_rx) = oneshot::channel();
        let answer = match self
            .command_tx
            .send(Command::WaitDurable {
                region_id,
                entry_id,
                kind,
                response: response_tx,
            })
            .await
        {
            Ok(()) => response_rx.await.ok(),
            Err(_) => None,
        };
        match answer {
            Some(result) => result,
            // The actor exited without handling the wait, after the last
            // object it indexed: the entries it holds are durable, no other
            // entry becomes durable any more.
            None if entry_id <= self.durable_entry_id_of(region_id) => Ok(()),
            None => ObjectStoreWalStoppedSnafu.fail(),
        }
    }

    /// Waits until every entry this store handed out for the region is
    /// durable, see [`Actor::handle_wait_durable`]. Once the actor exited
    /// nothing is in flight any more.
    async fn wait_for_issued(&self, region_id: RegionId) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        if self
            .command_tx
            .send(Command::WaitIssued {
                region_id,
                response: response_tx,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
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

    fn check_region(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_terminal()?;
        let provider_region = self.region_of(provider)?;
        ensure!(
            provider_region == region_id,
            MismatchedWalRegionSnafu {
                region_id,
                reason: format!("provider belongs to region {provider_region}"),
            }
        );
        Ok(())
    }

    fn check_terminal(&self) -> Result<()> {
        match terminal(&self.terminal_error) {
            Some(error) => Err(shared(&error)),
            None => Ok(()),
        }
    }

    /// Releases the region: its watermark moves up to the release in memory
    /// and a release record of it goes into an object sealed at once, and the
    /// call returns once that object is durable and indexed, or with the error
    /// that failed it. A `full` release reaches [`EntryId::MAX`]; any other
    /// the largest id the store handed out for the region, which every entry
    /// of the region it holds is at or below. A record whose release failed
    /// stays in the backlog or is carried by the next sealed batch. Recovery
    /// seeds the watermark from the record, so a store
    /// opened later collects the objects of a region that is never opened
    /// again.
    async fn release(&self, region_id: RegionId, full: bool) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::Release {
                region_id,
                full,
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }

    /// Returns why the prefix cannot hold the WAL history of `region_id`
    /// through the watermark `entry_id`, or `None` when it can.
    ///
    /// Two invariants of a prefix decide it. First, a prefix that any store
    /// opened is never empty afterwards, unless that store withdrew its start
    /// object, see [`withdraw_start`](Self::withdraw_start): every open writes
    /// its start object before it admits an append, and collection deletes
    /// only objects below the reclaim boundary of the last indexed object,
    /// which carries a boundary at or below its own sequence (a larger one
    /// fails its header check), so the newest indexed object always stays.
    /// Second, a watermark a region persisted names an object at or below
    /// the newest object holding entries that any later recovery of the
    /// prefix indexes. The entry it names was durable and indexed when the
    /// watermark was persisted, since Mito waits for durability before it
    /// publishes a watermark. No durable boundary passes the
    /// newest object holding entries: a batch carries at most the sequence
    /// after the last indexed object, which is below the batch, and a start
    /// object carries the boundary recovery found, or its own sequence on a
    /// chain without segments. So that object stays, and
    /// recovery indexes it on the chain of the tip. The segment of release
    /// records counts as entries: it never holds the reclaim boundary, so an
    /// object holding only releases can be the one that collection keeps
    /// while it deletes every older object. Objects without segments, such as
    /// the start objects of opens this check refused, do not move it.
    ///
    /// So the history is missing when this store found the prefix empty, or
    /// when the watermark names an object above the newest one holding
    /// entries that it found, unless the region's history starts in this
    /// store or the catalog holds the region's entries through the
    /// watermark: those of a region written since the store opened.
    fn missing_history(&self, region_id: RegionId, entry_id: EntryId) -> Option<String> {
        if self
            .started_regions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&region_id)
        {
            return None;
        }
        if self
            .catalog
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .region_max_entry_id(region_id)
            .is_some_and(|max_entry_id| max_entry_id >= entry_id)
        {
            return None;
        }
        if self.found_empty {
            return Some("the prefix held no object when the WAL opened".to_string());
        }
        let sequence_floor = sequence_floor(entry_id);
        (sequence_floor > self.history_end).then(|| {
            let held = match self.history_end {
                0 => "no entry".to_string(),
                history_end => format!("entries only in objects below {history_end}"),
            };
            format!(
                "its watermark, entry {entry_id}, names object {}, but the prefix held {held} when the WAL opened",
                sequence_floor - 1
            )
        })
    }

    /// Deletes the start object of a store that found its prefix empty and
    /// whose open of an existing region is refused, so that the next open
    /// finds the prefix empty again and refuses the region as well, instead
    /// of passing a region that never flushed. The actor gives the object up
    /// only while the store has admitted nothing, has no batch, holds no
    /// other object and no region's history started in it, and from then on
    /// the store accepts nothing, since a later object would link to the
    /// deleted one. The actor owns the delete like any collection, so `stop`
    /// waits for it and a cancelled caller does not abandon it, and every
    /// refusal returns only once it settled. A failed delete, or a crash
    /// before the delete completes, leaves the start object behind, and the
    /// next open passes a region that never flushed.
    async fn withdraw_start(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        if self
            .command_tx
            .send(Command::Withdraw {
                response: response_tx,
            })
            .await
            .is_ok()
        {
            let _ = response_rx.await;
        }
    }
}

/// The transient object store error of a create that a testing hook fails.
#[cfg(any(test, feature = "testing"))]
fn injected_create_failure(io: &dyn WalObjectIo, object_seq: u64) -> Result<PutResult> {
    let error = object_store::Error::new(
        object_store::ErrorKind::Unexpected,
        "injected create failure",
    )
    .set_temporary();
    Err(error).context(crate::error::WalObjectStoreSnafu {
        operation: "write",
        path: io.object_path(object_seq),
    })
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

    /// Waits until the actor has held back at least `expected` append calls
    /// since the store was built, see [`Actor::should_hold_back`].
    pub async fn wait_for_held_back_appends(&self, expected: usize) -> Result<()> {
        self.held_back_appends
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Returns the number of sealed batches that are not indexed yet, once
    /// the actor handled every command sent before.
    pub async fn sealed_batches(&self) -> Result<usize> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(Command::SealedBatches {
                response: response_tx,
            })
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)
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
    /// store error instead of writing.
    pub fn fail_creates(&self) {
        self.creates_fail.store(true, Ordering::Release);
    }

    /// Lets the creates that run from now on write again after
    /// [`fail_creates`](Self::fail_creates).
    pub fn allow_creates(&self) {
        self.creates_fail.store(false, Ordering::Release);
    }

    /// Makes the next create that writes its object report a transient object
    /// store error afterwards, so the object exists although its create
    /// failed.
    pub fn fail_next_create_after_write(&self) {
        self.next_create_fails_after_write
            .store(true, Ordering::Release);
    }

    /// Waits until at least `expected` creates have been parked by
    /// [`hold_creates`](Self::hold_creates) since the store was built.
    pub async fn wait_for_parked_creates(&self, expected: usize) -> Result<()> {
        self.parked_creates
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Waits until the actor has handled at least `expected` creates that the
    /// object store did not confirm since the store was built.
    pub async fn wait_for_failed_creates(&self, expected: usize) -> Result<()> {
        self.failed_creates
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Waits until the store is failing, see [`Failing`].
    pub async fn wait_until_failing(&self) -> Result<()> {
        self.failing_state
            .clone()
            .wait_for(|failing| *failing)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Waits until the store is not failing, see [`Failing`].
    pub async fn wait_until_not_failing(&self) -> Result<()> {
        self.failing_state
            .clone()
            .wait_for(|failing| !*failing)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Waits until at least `expected` calls of
    /// [`wait_durable`](LogStore::wait_durable) have had to wait for an entry
    /// that is not durable since the store was built.
    pub async fn wait_for_durability_waits(&self, expected: usize) -> Result<()> {
        self.durability_waits
            .clone()
            .wait_for(|count| *count >= expected)
            .await
            .ok()
            .map(|_| ())
            .context(ObjectStoreWalStoppedSnafu)
    }

    /// Ends the actor the way a crash of the process would: nothing more is
    /// written, creates that have not completed are dropped, and every caller
    /// still waiting for the store fails. Returns once the actor has exited.
    pub async fn crash(&self) {
        self.stopped.store(true, Ordering::Release);
        let (response_tx, response_rx) = oneshot::channel();
        if self
            .command_tx
            .send(Command::Crash {
                response: response_tx,
            })
            .await
            .is_ok()
        {
            let _ = response_rx.await;
        }
    }

    /// Waits until the actor handled every command sent before, which
    /// finishes the indexing or the `obsolete` that started a collection, and
    /// then until no object delete is in flight. A durability wait can return
    /// as soon as the catalog holds its entry, before the actor started the
    /// collection that indexing the object starts.
    pub async fn wait_for_garbage_collection(&self) {
        // An actor that exited has no delete left to start.
        let _ = self.sealed_batches().await;
        self.deleting.wait_until_empty().await;
    }

    /// Sets the stopped flag without sending a command, which is the state a
    /// store is in between setting the flag and the actor handling
    /// [`begin_stop`](Self::begin_stop) or [`stop`](LogStore::stop).
    pub fn raise_stopped_flag(&self) {
        self.stopped.store(true, Ordering::Release);
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

    /// Stops the store, beginning the stop first if
    /// [`begin_stop`](ObjectStoreLogStore::begin_stop) did not. Returns once
    /// the backlog is uploaded, with the error that dropped it when an upload
    /// failed, and at the latest `max_unpersisted_age` after the stop began,
    /// when it gives up the creates and deletes still in flight.
    async fn stop(&self) -> Result<()> {
        self.begin_stop().await;
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
                MismatchedWalRegionSnafu {
                    region_id: entry.region_id(),
                    reason: format!("provider belongs to region {region_id}"),
                }
            );
            ensure!(entry.is_complete(), IncompleteWalEntrySnafu { region_id });
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.append_tx
            .send((entries, response_tx))
            .await
            .ok()
            .context(ObjectStoreWalStoppedSnafu)?;
        response_rx.await.ok().context(ObjectStoreWalStoppedSnafu)?
    }

    /// Reads the provider's region from `entry_id`, hiding obsolete entries.
    /// The read first waits until every entry this store handed out for the
    /// region is durable, as a flush waits, so that a region closed and opened
    /// again while the store runs replays the entries of the backlog too.
    /// The catalog locates each segment, so a caller-supplied index is unnecessary.
    /// An object collected after the read listed it is skipped. Otherwise a
    /// segment that does not decode fails the read or is skipped and recorded
    /// as a hole, as `on_corrupted_segment` says; a failed fetch always fails
    /// the read. Up to [`READ_PREFETCH_DEPTH`] segments are fetched ahead
    /// while [`READ_PREFETCH_BYTES`] allows, and each outcome is handled in
    /// sequence order.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<'static, Entry, Error>> {
        self.check_terminal()?;
        let region_id = self.region_of(provider)?;
        self.wait_for_issued(region_id).await?;
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
        let prefetch_budget = self.read_prefetch_budget.clone();
        Ok(Box::pin(try_stream! {
            let started_at = Instant::now();
            let mut objects = objects.into_iter().peekable();
            let mut fetches = VecDeque::new();
            loop {
                // The next segment is always fetched, a later one only with
                // its bytes reserved from the budget.
                while fetches.len() < READ_PREFETCH_DEPTH {
                    let Some((_, entry)) = objects.peek() else {
                        break;
                    };
                    let permit = if fetches.is_empty() {
                        None
                    } else {
                        match try_reserve_prefetch_bytes(&prefetch_budget, entry) {
                            Some(permit) => Some(permit),
                            None => break,
                        }
                    };
                    let Some((object_seq, footer_entry)) = objects.next() else {
                        break;
                    };
                    let fetch =
                        spawn_fetch_segment(io.clone(), object_seq, footer_entry.clone(), permit);
                    fetches.push_back((object_seq, footer_entry, fetch));
                }
                let Some((object_seq, footer_entry, fetch)) = fetches.pop_front() else {
                    break;
                };
                // The permit is held until the entries of the segment are yielded.
                let (fetched, _permit) = fetch.await;
                let records = match fetched {
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
                        record_skipped_segment(&catalog, &wal_holes, io.as_ref(), object_seq, &footer_entry, &error);
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
                            data: record.payload.into(),
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
            .filter(|region_id| *region_id != RELEASE_REGION_ID)
            .collect::<BTreeSet<_>>();
        Ok(regions
            .into_iter()
            .map(|region_id| Provider::object_store_provider(region_id, self.prefix.clone()))
            .collect())
    }

    /// Moves the obsolete watermark of the region up to `entry_id`. The
    /// watermark never passes the durable entry id: an entry that is not
    /// durable yet is replayed after a crash, and hiding it would skip it.
    ///
    /// Every id the region is assigned from now on is greater than
    /// `entry_id`, whatever watermark is recorded: the sequence of the next
    /// object is raised above the object `entry_id` names unless it is there
    /// already, so a watermark assigned under another prefix, or one whose
    /// object this prefix no longer holds, is never passed by a new id. Zero
    /// names no object and needs no floor. Before Mito opens a region that
    /// writes this WAL from its persisted state, it calls
    /// [`check_region_history`](LogStore::check_region_history), which
    /// refuses a watermark naming an object at or above the next sequence
    /// the store found unless `accept_missing_history` is set, so the floor
    /// moves for such a region only once that loss was accepted.
    ///
    /// The watermark and the floor are applied together by the actor: a call
    /// cancelled before its command is queued publishes neither, while a
    /// command already queued still applies both. While the open batch holds
    /// ids under the next sequence the floor cannot move, and the call fails
    /// with [`Error::WalObjectSequenceUnsettled`] and records neither. The
    /// watermark is kept in memory; the caller re-establishes it after a
    /// restart unless it released the region, see
    /// [`release_region`](LogStore::release_region). An `entry_id` this store
    /// handed out needs no floor: it is never handed out again while the
    /// store runs.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        self.check_region(provider, region_id)?;
        let watermark = entry_id.min(self.durable_entry_id_of(region_id));
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

    /// Hides every entry of the region and lets collection delete its
    /// objects, by a release in full that is durable once this returns.
    async fn obsolete_all(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_region(provider, region_id)?;
        self.release(region_id, true).await
    }

    /// Releases the region through the largest id handed out for it, durably
    /// once this returns. Should the region id be used again, its new entries
    /// have greater ids and stay readable.
    async fn release_region(&self, provider: &Provider, region_id: RegionId) -> Result<()> {
        self.check_region(provider, region_id)?;
        self.release(region_id, false).await
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        self.check_region(provider, region_id)?;
        Ok(Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data,
        }))
    }

    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        self.durable_entry_id(provider)
    }

    /// Waits until every entry of the provider's region with an id at or
    /// below `entry_id` is durable and indexed. The wait lasts while the store
    /// is failing, since the backlog it keeps becomes durable once a create
    /// succeeds again; it fails once the store drops the entries, when it is
    /// poisoned or its stop drops the backlog.
    async fn wait_durable(&self, provider: &Provider, entry_id: EntryId) -> Result<()> {
        self.wait(provider, entry_id, WaitKind::Barrier).await
    }

    /// Answers true in the `durable` acknowledgement mode, whose appends
    /// return on admission like those of the `enqueued` mode.
    fn defers_write_durability(&self) -> bool {
        self.ack_mode == AckMode::Durable
    }

    /// Waits like [`wait_durable`](LogStore::wait_durable), except that the
    /// wait fails at once while the store is failing, and every such wait
    /// fails when the store becomes failing: a writer is answered within
    /// the bound of a run of failed or unanswered creates.
    async fn wait_write_durable(&self, provider: &Provider, entry_id: EntryId) -> Result<()> {
        self.wait(provider, entry_id, WaitKind::Write).await
    }

    /// True while the last create of a batch the store keeps failed, which
    /// includes the store being failing, see [`Failing`], and once the store
    /// recorded a terminal error or its actor exited. A store that began to
    /// stop keeps uploading its backlog and is not failing for that alone. A
    /// create that succeeds clears it.
    fn is_failing(&self) -> bool {
        self.last_create_failed.load(Ordering::Acquire)
            || terminal(&self.terminal_error).is_some()
            || self.command_tx.is_closed()
    }

    /// Seals the open batch now instead of on the next tick of
    /// `flush_interval`, and refuses every later append, also one held back,
    /// with the stopped error.
    fn begin_drain(&self) {
        self.drain_requested.notify_one();
    }

    /// Fails with [`Error::MissingWalHistory`] when the prefix cannot hold
    /// the history of the provider's region through `entry_id`, unless
    /// `accept_missing_history` is set: then the region's writes that were
    /// not flushed are lost, which is logged and counted.
    async fn check_region_history(&self, provider: &Provider, entry_id: EntryId) -> Result<()> {
        let region_id = self.region_of(provider)?;
        let Some(reason) = self.missing_history(region_id, entry_id) else {
            return self.check_terminal();
        };
        if !self.accept_missing_history {
            if self.found_empty {
                self.withdraw_start().await;
            }
            return MissingWalHistorySnafu {
                region_id,
                prefix: self.prefix.clone(),
                storage: self.storage.clone(),
                reason,
            }
            .fail();
        }
        self.check_terminal()?;
        warn!(
            "Opening region {region_id} without its WAL history, as accept_missing_history allows: prefix {} on {} does not hold it, {reason}; the region's writes above entry {entry_id} that were not flushed are lost",
            self.prefix, self.storage
        );
        METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.inc();
        self.start_region_history(provider)
    }

    /// Records the provider's region as one whose history starts in this
    /// store, which is kept in memory: a later store on the prefix judges
    /// the region by its watermark and the objects it finds.
    fn start_region_history(&self, provider: &Provider) -> Result<()> {
        let region_id = self.region_of(provider)?;
        let mut started_regions = self
            .started_regions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A withdrawal records its terminal error under the same lock.
        self.check_terminal()?;
        started_regions.insert(region_id);
        Ok(())
    }
}

type AppendResponse = oneshot::Sender<Result<AppendBatchResponse>>;

type QueuedAppend = (Vec<Entry>, AppendResponse);

enum Command {
    /// Answered once the region is durable through `entry_id`, or with the
    /// reason it will not be, see [`Actor::handle_wait_durable`].
    WaitDurable {
        region_id: RegionId,
        entry_id: EntryId,
        kind: WaitKind,
        response: oneshot::Sender<Result<()>>,
    },
    /// Answered once every entry handed out for the region is durable, as a
    /// [`WaitKind::Barrier`] wait.
    WaitIssued {
        region_id: RegionId,
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
    /// Answered once the object holding the release record of the region is
    /// durable and indexed, or with the reason it is not. A `full` release
    /// reaches [`EntryId::MAX`], any other the largest id handed out for the
    /// region.
    Release {
        region_id: RegionId,
        full: bool,
        response: oneshot::Sender<Result<()>>,
    },
    /// Answered once the store keeps its start object, or once the delete
    /// of the start object it gave up settled.
    Withdraw { response: oneshot::Sender<()> },
    /// Begins stopping, see [`ObjectStoreLogStore::begin_stop`].
    BeginStop,
    Stop {
        response: oneshot::Sender<Result<()>>,
    },
    #[cfg(any(test, feature = "testing"))]
    Seal {
        response: oneshot::Sender<Result<()>>,
    },
    #[cfg(any(test, feature = "testing"))]
    SealedBatches { response: oneshot::Sender<usize> },
    #[cfg(any(test, feature = "testing"))]
    Crash { response: oneshot::Sender<()> },
}

/// What a caller waiting for durable entries does while the store is
/// failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitKind {
    /// A barrier before a watermark is persisted keeps waiting: the store
    /// keeps the backlog, which becomes durable once a create succeeds.
    Barrier,
    /// A writer waiting to be answered fails at once, so that it is answered
    /// within the bound of a failing run of creates.
    Write,
}

/// A caller waiting until a region is durable through an entry id.
struct DurableWaiter {
    kind: WaitKind,
    response: oneshot::Sender<Result<()>>,
}

/// The callers waiting for durable entries, per region and by entry id, so
/// that indexing an object answers the waiters of its regions without
/// scanning the others.
#[derive(Default)]
struct DurableWaiters {
    regions: HashMap<RegionId, BTreeMap<EntryId, Vec<DurableWaiter>>>,
}

impl DurableWaiters {
    fn push(&mut self, region_id: RegionId, entry_id: EntryId, waiter: DurableWaiter) {
        let waiters = self
            .regions
            .entry(region_id)
            .or_default()
            .entry(entry_id)
            .or_default();
        // A caller that stopped waiting leaves a closed response behind.
        waiters.retain(|waiter| !waiter.response.is_closed());
        waiters.push(waiter);
    }

    /// Answers every waiter of the region whose entry id is below
    /// `lowest_pending`, the lowest id of the region that is not durable, or
    /// every waiter of the region when none is pending.
    fn resolve(&mut self, region_id: RegionId, lowest_pending: Option<EntryId>) {
        let Some(waiters) = self.regions.get_mut(&region_id) else {
            return;
        };
        let resolved = match lowest_pending {
            Some(lowest_pending) => {
                let pending = waiters.split_off(&lowest_pending);
                std::mem::replace(waiters, pending)
            }
            None => std::mem::take(waiters),
        };
        if waiters.is_empty() {
            self.regions.remove(&region_id);
        }
        for waiter in resolved.into_values().flatten() {
            let _ = waiter.response.send(Ok(()));
        }
    }

    /// Fails the waiters of `kind`, or every waiter when `kind` is `None`.
    fn fail(&mut self, kind: Option<WaitKind>, error: impl Fn() -> Error) {
        for waiters in self.regions.values_mut() {
            for list in waiters.values_mut() {
                for waiter in
                    list.extract_if(.., |waiter| kind.is_none_or(|kind| waiter.kind == kind))
                {
                    let _ = waiter.response.send(Err(error()));
                }
            }
            waiters.retain(|_, list| !list.is_empty());
        }
        self.regions.retain(|_, waiters| !waiters.is_empty());
    }
}

/// Where the conditional create of a sealed batch stands.
enum CreateState {
    /// The create has not started: no slot was free, or an earlier attempt
    /// failed with an object store error and is repeated.
    Pending,
    InFlight,
    Created,
}

/// A sealed and encoded batch that holds its object sequence and waits to be
/// created, indexed and acknowledged.
struct SealedBatch {
    object_seq: u64,
    reclaim_boundary: u64,
    bytes: Bytes,
    footer: Vec<FooterEntry>,
    releases: Vec<Release>,
    first_admitted_at: Instant,
    sealed_at: Instant,
    /// When each append of the batch was admitted, kept in the `durable`
    /// mode to time its acknowledgement once the object is indexed.
    admitted_at: Vec<Instant>,
    /// The smallest entry id of each region the batch holds.
    min_entry_ids: HashMap<RegionId, EntryId>,
    /// Whether the batch marks a raised sequence floor, see
    /// [`Actor::raise_sequence_floor`]; no other batch is sealed until it
    /// is indexed.
    floor: bool,
    /// Number of creates that were attempted for the batch.
    attempts: u32,
    /// Callers of a release whose record the batch carries, or of the
    /// `obsolete` that raised the floor the batch marks, answered once the
    /// object is durable and indexed.
    release_waiters: Vec<oneshot::Sender<Result<()>>>,
    #[cfg(any(test, feature = "testing"))]
    seal_waiters: Vec<oneshot::Sender<Result<()>>>,
    state: CreateState,
}

impl SealedBatch {
    fn is_in_flight(&self) -> bool {
        matches!(self.state, CreateState::InFlight)
    }

    fn fail(self, error: impl Fn() -> Error) {
        for waiter in self.release_waiters {
            let _ = waiter.send(Err(error()));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in self.seal_waiters {
            let _ = waiter.send(Err(error()));
        }
    }
}

type CreateOutcome = (u64, Result<PutResult>);

/// The sequences the start object skipped, which a late object of an
/// earlier instance can still take, see [`LATE_OBJECT_RECHECK`].
struct SkippedSequences {
    sequences: std::ops::Range<u64>,
    /// How long after the last recheck a collection deletes them again.
    interval: Duration,
    /// When a collection deletes them again next.
    next_check: Instant,
}

/// Whether the store gave up its start object, see
/// [`Actor::handle_withdraw`].
enum Withdrawal {
    Kept,
    /// The delete of the start object `object_seq` is in flight; `waiters`
    /// are answered once it settled.
    Deleting {
        object_seq: u64,
        waiters: Vec<oneshot::Sender<()>>,
    },
    Settled,
}

/// The state of a store whose backlog the object store refuses with an error
/// that requires intervention, see [`requires_intervention`], such as denied
/// access or a missing bucket, or whose lowest pending batch did not become
/// durable within `max_unpersisted_age` of being sealed, however its creates
/// went, see [`Actor::transient_deadline`].
///
/// Poisoning would drop the backlog, whose entries were handed out, and need
/// a restart, while such a cause can be fixed in place or pass. So the
/// backlog keeps its sequences and bytes, and its lowest pending batch is
/// attempted alone, once nothing is in flight, after a delay that doubles
/// from [`CREATE_RETRY_DELAY`] up to [`MAX_FAILING_RETRY_DELAY`]. Appends are
/// rejected at once with a retryable error that carries the last error: they
/// take no entry id and never wait behind the backlog. Releases fail with it
/// too, and their records wait for a create, and so do the writers waiting
/// for their entries, see [`WaitKind::Write`], while the barriers keep
/// waiting. The first create that succeeds ends the state.
struct Failing {
    /// The last error a create met, which rejected appends carry.
    error: Arc<Error>,
    since: Instant,
    /// The delay before the next attempt is due once nothing is in flight.
    delay: Duration,
    /// When the next attempt starts, `None` until it is scheduled.
    probe_at: Option<Instant>,
    failed_creates: u64,
    rejected_appends: u64,
    logged_at: Instant,
}

impl Failing {
    /// Schedules the next attempt after the current delay and doubles the
    /// delay for the one after it.
    fn schedule(&mut self) {
        self.probe_at = Some(Instant::now() + self.delay);
        self.delay = (self.delay * 2).min(MAX_FAILING_RETRY_DELAY);
    }

    /// Counts an admission the store refuses and returns the error it
    /// fails with.
    fn reject(&mut self) -> Error {
        self.rejected_appends += 1;
        METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.inc();
        ObjectStoreWalFailingSnafu.into_error(self.error.clone())
    }
}

/// The actor that owns the open batch and the sealed batches until they are
/// durable.
///
/// Every append is answered on admission with its entry ids, which are never
/// given up while the store runs. Sealed batches form a pipeline in sequence
/// order. A batch is created under its sequence as soon as one of
/// [`MAX_IN_FLIGHT_CREATES`] slots is free, but it is indexed, which makes its
/// entries durable, only once every earlier batch is, so the durable history
/// of a region never has a missing predecessor. Every batch links to its
/// pipeline predecessor, the batch sealed before it, or to the last indexed
/// object when no earlier batch waits, so recovery replays a batch only on
/// the chain of a later object. The outcomes of a create are:
///
/// | Situation | Sequence | Waiters | Store |
/// | --- | --- | --- | --- |
/// | created, or identical retry | advances | durable in order | healthy |
/// | transient error | unchanged | wait for the repeat | healthy: the create is repeated with the same bytes after [`CREATE_RETRY_DELAY`], which an identical retry accepts; failing once the batch is not durable within `max_unpersisted_age` |
/// | error that requires intervention | unchanged | barriers keep waiting; writers and releases fail, and the records stay in the backlog; later appends and releases are rejected with a retryable error | failing, see [`Failing`]: the lowest pending create is repeated alone with backoff, and the first create that succeeds ends it |
/// | object store error after `stop` began | never reused | fail with the stopped error | the backlog is dropped and `stop` reports the error |
/// | any conflicting object, encoding or catalog error | unchanged | every batch that is not indexed fails | poisoned |
/// | created at the last representable sequence | cannot advance | durable | poisoned: no later batch can be allocated a sequence |
///
/// A create still in flight when its batch failed runs to completion and its
/// outcome is ignored: whatever it stored is off the chain. After `stop`
/// began nothing is admitted, and the backlog is uploaded at once, a failing
/// store's included; creates in flight run to completion.
///
/// Appends arrive on their own channel. The actor holds one back while the
/// sealed batches leave no room for the two an admission can seal, at a
/// backlog threshold, or while a raised sequence floor is not durable, and
/// does not read the channel meanwhile, so commands such as `stop` are
/// handled while admission is held back. A failing store reads the channel
/// to reject every append.
///
/// The actor also collects garbage. Every batch carries the reclaim boundary
/// current when it was sealed, see [`ObjectCatalog::reclaim_boundary`], which
/// is recomputed after every `obsolete`, every release and every indexed
/// object and never moves down. The boundary of the last indexed object is
/// durable: every present object below it, indexed or not, is deleted in the
/// background, up to [`MAX_IN_FLIGHT_DELETES`] at a time, after an
/// `obsolete`, a release or an indexing that moved it. An indexed object is
/// unindexed once its delete succeeded; a failed delete is attempted again at
/// the next collection. Nothing is
/// collected once stop began or the store is poisoned, and deletes in flight
/// complete before `stop` returns.
struct Actor {
    io: Arc<dyn WalObjectIo>,
    started_regions: StartedRegions,
    withdrawal: Withdrawal,
    catalog: Arc<RwLock<ObjectCatalog>>,
    obsolete_entry_ids: ObsoleteEntryIds,
    wal_holes: WalHoles,
    terminal_error: TerminalError,
    stopped: Arc<AtomicBool>,
    /// Set when a create of the backlog fails, cleared when one succeeds.
    last_create_failed: Arc<AtomicBool>,
    drain_requested: Arc<Notify>,
    command_rx: mpsc::Receiver<Command>,
    append_rx: mpsc::Receiver<QueuedAppend>,
    ack_mode: AckMode,
    max_unpersisted_bytes: usize,
    max_unpersisted_age: Duration,
    open_batch: OpenBatch,
    /// Largest entry id ever handed out per region, whether it became
    /// durable or failed. Unlike the accepted ids of the open batch it never
    /// moves down.
    issued_entry_ids: HashMap<RegionId, EntryId>,
    /// When each append of the open batch was admitted, in the `durable`
    /// mode.
    open_admitted_at: Vec<Instant>,
    /// Batches that are not durable yet, in sequence order.
    sealed: VecDeque<SealedBatch>,
    /// Creates in flight, including those of batches the store gave up.
    creates: FuturesUnordered<BoxFuture<'static, CreateOutcome>>,
    /// The append held back while the sealed batches are full, the
    /// unpersisted backlog is at a threshold or a raised floor is not durable,
    /// and when it was held back. Later appends wait in their channel.
    stalled: Option<(QueuedAppend, Instant)>,
    /// Set while the store is failing, see [`Failing`].
    failing: Option<Failing>,
    /// How long the lowest pending batch may stay not durable after it was
    /// sealed, or after the store last left the failing state, before the
    /// store becomes failing: `max_unpersisted_age`.
    transient_bound: Duration,
    /// When the store opened or last left the failing state.
    healthy_since: Instant,
    /// When stop began, see [`stop_deadline`](Self::stop_deadline).
    stop_began_at: Option<Instant>,
    /// Releases waiting for room to seal their batch, see
    /// [`seal`](Self::seal).
    held_releases: Vec<(Release, oneshot::Sender<Result<()>>)>,
    durable_waiters: DurableWaiters,
    /// Set by `begin_drain`: no append is admitted any more.
    draining: bool,
    /// Callers of `stop`, answered once nothing is in flight.
    stop: Vec<oneshot::Sender<Result<()>>>,
    /// The failure `stop` reports once a backlog was dropped, recorded when
    /// it happens.
    stop_error: Option<Arc<Error>>,
    /// Sequence of the next sealed batch, `None` once the sequence is
    /// exhausted. The open batch assigns its entry ids from it. It is above
    /// every sealed batch and never moves back while the store runs.
    next_object_seq: Option<u64>,
    /// Epoch of this instance, carried by every object it writes.
    epoch: u64,
    /// The last indexed object, which is the start object until a batch is
    /// indexed. A batch sealed while no earlier batch waits extends it.
    last_indexed: ChainLink,
    /// The reclaim boundary the next sealed batch carries.
    reclaim_boundary: u64,
    /// The reclaim boundary of the last indexed object.
    durable_reclaim_boundary: u64,
    /// Sequences of objects that may be present but are not indexed: the
    /// objects recovery listed off the chain, the sequences the start object
    /// skipped and the batches a stop gave up.
    unindexed: BTreeSet<u64>,
    /// The sequences the start object skipped, which collections delete
    /// again, see [`LATE_OBJECT_RECHECK`].
    skipped: Option<SkippedSequences>,
    /// Skipped sequences whose delete a recheck started, which is not counted
    /// as a deleted object: most of them hold none.
    rechecked: HashSet<u64>,
    /// Release records whose release failed before their batch was sealed,
    /// or of batches a stop gave up, which the next sealed batch carries, so
    /// a release whose call failed still becomes durable while the store
    /// runs. A timer tick seals them when no entry is admitted.
    failed_releases: BTreeMap<RegionId, EntryId>,
    /// Sequences whose create is in flight.
    creating: HashSet<u64>,
    deleting: Arc<DeletingObjects>,
    deletes: FuturesUnordered<BoxFuture<'static, (u64, Result<()>)>>,
    /// Deletes that failed since the last collection started.
    failed_deletes: HashSet<u64>,
    flush_interval: Duration,
    #[cfg(any(test, feature = "testing"))]
    admitted_appends: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    held_back_appends: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    creates_held: watch::Receiver<bool>,
    #[cfg(any(test, feature = "testing"))]
    creates_fail: Arc<AtomicBool>,
    #[cfg(any(test, feature = "testing"))]
    next_create_fails_after_write: Arc<AtomicBool>,
    #[cfg(any(test, feature = "testing"))]
    parked_creates: Arc<watch::Sender<usize>>,
    #[cfg(any(test, feature = "testing"))]
    durability_waits: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    failed_creates: watch::Sender<usize>,
    #[cfg(any(test, feature = "testing"))]
    failing_state: watch::Sender<bool>,
}

impl Actor {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes immediately.
        interval.tick().await;
        let drain_requested = self.drain_requested.clone();

        loop {
            let probe_at = self.failing.as_ref().and_then(|failing| failing.probe_at);
            let transient_deadline = self.transient_deadline();
            let stop_deadline = self.stop_deadline();
            tokio::select! {
                _ = interval.tick() => {
                    // Nothing starts after stop began, which sealed the open
                    // batch.
                    if !self.is_stopped() {
                        self.flush_open_batch();
                    }
                }
                _ = drain_requested.notified(), if !self.draining => {
                    self.handle_drain();
                }
                Some((object_seq, result)) = self.creates.next(), if !self.creates.is_empty() => {
                    self.on_create_completed(object_seq, result);
                }
                Some((object_seq, result)) = self.deletes.next(), if !self.deletes.is_empty() => {
                    self.on_delete_completed(object_seq, result);
                }
                Some(()) = OptionFuture::from(probe_at.map(|at| tokio::time::sleep_until(at.into()))) => {
                    self.probe();
                }
                Some(()) = OptionFuture::from(transient_deadline.map(|at| tokio::time::sleep_until(at.into()))) => {
                    self.on_transient_deadline();
                }
                Some(()) = OptionFuture::from(stop_deadline.map(|at| tokio::time::sleep_until(at.into()))) => {
                    self.on_stop_deadline();
                }
                Some((entries, response)) = self.append_rx.recv(), if self.stalled.is_none() => {
                    self.handle_append(entries, response);
                }
                command = self.command_rx.recv() => match command {
                    Some(Command::WaitDurable { region_id, entry_id, kind, response }) => {
                        self.handle_wait_durable(region_id, entry_id, kind, response);
                    }
                    Some(Command::WaitIssued { region_id, response }) => {
                        let entry_id = self.issued_entry_ids.get(&region_id).copied().unwrap_or(0);
                        self.handle_wait_durable(region_id, entry_id, WaitKind::Barrier, response);
                    }
                    Some(Command::Obsolete { region_id, entry_id, watermark, response }) => {
                        self.handle_obsolete(region_id, entry_id, watermark, response);
                    }
                    Some(Command::Release { region_id, full, response }) => {
                        self.handle_release(region_id, full, response);
                    }
                    Some(Command::Withdraw { response }) => {
                        self.handle_withdraw(response);
                    }
                    Some(Command::BeginStop) => {
                        self.begin_stop();
                    }
                    Some(Command::Stop { response }) => {
                        self.handle_stop(response);
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Seal { response }) => {
                        self.handle_seal(response);
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::SealedBatches { response }) => {
                        let _ = response.send(self.sealed.len());
                    }
                    #[cfg(any(test, feature = "testing"))]
                    Some(Command::Crash { response }) => {
                        self.handle_crash();
                        let _ = response.send(());
                        return;
                    }
                    // Every sender is gone: the store was dropped without
                    // `stop`. The creates in flight are dropped with the actor.
                    None => return,
                },
            }
            if self.finish_stop() {
                return;
            }
        }
    }

    fn handle_append(&mut self, entries: Vec<Entry>, response: AppendResponse) {
        // The append was queued before `stop` set the flag or the store began
        // to drain; nothing that is not durable yet gets admitted after that.
        if self.is_stopped() || self.draining {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        if let Some(failing) = &mut self.failing {
            let _ = response.send(Err(failing.reject()));
            return;
        }
        if self.should_hold_back() {
            METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.inc();
            #[cfg(any(test, feature = "testing"))]
            self.held_back_appends.send_modify(|count| *count += 1);
            self.stalled = Some(((entries, response), Instant::now()));
            self.ensure_create_in_flight();
            return;
        }
        self.admit(entries, response);
    }

    /// Admits `entries` into the open batch, which assigns their ids under
    /// the next object sequence, and answers the caller with them.
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
        if self.ack_mode == AckMode::Durable {
            self.open_admitted_at.push(Instant::now());
        }
        let _ = response.send(Ok(AppendBatchResponse { last_entry_ids }));
        #[cfg(any(test, feature = "testing"))]
        self.admitted_appends.send_modify(|count| *count += 1);
        if self.open_batch.should_seal() {
            self.flush_open_batch();
        }
    }

    /// Returns true while an append has to wait for an upload: the sealed
    /// batches leave no room for the two an admission can seal, the
    /// unpersisted backlog is at a threshold, or a raised sequence floor is
    /// not durable yet.
    fn should_hold_back(&self) -> bool {
        self.sealed.len() + 2 > MAX_SEALED_BATCHES
            || self.backlog_at_threshold()
            || self.floor_pending()
    }

    /// Returns true while a batch that marks a raised sequence floor waits
    /// to be indexed.
    fn floor_pending(&self) -> bool {
        self.sealed.iter().any(|batch| batch.floor)
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

    /// Admits the stalled append once it no longer has to be held back.
    fn release_stalled(&mut self) {
        if self.stalled.is_none() {
            return;
        }
        if self.is_stopped() || self.draining {
            if let Some((_, response)) = self.take_stalled() {
                let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            }
            return;
        }
        if self.should_hold_back() {
            // The append needs an upload to complete.
            self.ensure_create_in_flight();
            return;
        }
        if let Some((entries, response)) = self.take_stalled() {
            self.admit(entries, response);
        }
    }

    /// Takes the held-back append out on whichever path releases it and
    /// records how long it was held back.
    fn take_stalled(&mut self) -> Option<QueuedAppend> {
        let (append, stalled_at) = self.stalled.take()?;
        METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.observe(stalled_at.elapsed().as_secs_f64());
        Some(append)
    }

    /// Seals the open batch as the object `next_object_seq` and starts its
    /// create when a slot is free. Returns whether a batch was sealed.
    fn flush_open_batch(&mut self) -> bool {
        self.seal(Vec::new())
    }

    /// Seals the open batch together with `releases` and the release records
    /// of failed batches as the object `next_object_seq`, even when the open
    /// batch holds no entry, and starts its create when a slot is free.
    /// Returns whether a batch was sealed.
    ///
    /// A failing store keeps the records of failed batches coalesced instead
    /// of sealing them alone behind its backlog on every tick, until `stop`
    /// seals them for its last attempt of the backlog. Nothing is sealed while
    /// [`MAX_SEALED_BATCHES`] batches wait or a raised floor is not durable,
    /// which bounds the sequences an instance hands out beyond its last
    /// durable object, see [`UNLISTED_SEQUENCES`]: admission holds appends
    /// back before that, a release waits in `held_releases`, and the open
    /// batch of a timer tick waits for the next one.
    fn seal(&mut self, releases: Vec<Release>) -> bool {
        self.seal_batch(releases, false)
    }

    /// Returns whether a batch can be sealed now, see [`seal`](Self::seal).
    fn has_room(&self) -> bool {
        self.sealed.len() < MAX_SEALED_BATCHES && !self.floor_pending()
    }

    fn seal_batch(&mut self, releases: Vec<Release>, floor: bool) -> bool {
        if !floor
            && self.open_batch.is_empty()
            && releases.is_empty()
            && (self.failed_releases.is_empty() || (self.failing.is_some() && !self.is_stopped()))
        {
            return false;
        }
        // Once stop dropped the backlog, nothing gets another attempt.
        if !self.has_room() || self.stop_error.is_some() {
            return false;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            if let Some((_, response)) = self.take_stalled() {
                let _ = response.send(Err(shared(&error)));
            }
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
        let header = Header {
            object_seq,
            epoch: self.epoch,
            prev: Some(
                self.sealed
                    .back()
                    .map_or(self.last_indexed, |batch| ChainLink {
                        object_seq: batch.object_seq,
                        epoch: self.epoch,
                    }),
            ),
            reclaim_boundary: self.reclaim_boundary,
        };
        let mut records = std::mem::take(&mut self.failed_releases);
        for release in releases {
            let entry_id = records.entry(release.region_id).or_insert(release.entry_id);
            *entry_id = (*entry_id).max(release.entry_id);
        }
        let encoded = match encode_batch(header, entries, &records) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.poison(error);
                return false;
            }
        };
        METRIC_OBJECT_STORE_WAL_OBJECT_BYTES.observe(encoded.bytes.len() as f64);
        METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES.observe(entry_count as f64);
        let min_entry_ids = encoded
            .footer
            .iter()
            .filter(|entry| entry.region_id != RELEASE_REGION_ID)
            .map(|entry| (entry.region_id, entry.min_entry_id))
            .collect();
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
            reclaim_boundary: self.reclaim_boundary,
            bytes: encoded.bytes,
            footer: encoded.footer,
            releases: records
                .into_iter()
                .map(|(region_id, entry_id)| Release {
                    region_id,
                    entry_id,
                })
                .collect(),
            first_admitted_at,
            sealed_at: Instant::now(),
            admitted_at: std::mem::take(&mut self.open_admitted_at),
            min_entry_ids,
            floor,
            attempts: 0,
            release_waiters: Vec::new(),
            #[cfg(any(test, feature = "testing"))]
            seal_waiters: Vec::new(),
            state: CreateState::Pending,
        });
        self.start_creates();
        true
    }

    /// Starts the creates of pending batches in sequence order while fewer
    /// than [`MAX_IN_FLIGHT_CREATES`] are in flight, counting the creates of
    /// batches the store gave up. A failing store only schedules the next
    /// attempt once nothing is in flight, see [`Failing`]; after stop began
    /// it uploads its backlog at once.
    fn start_creates(&mut self) {
        if !self.is_stopped()
            && let Some(failing) = &mut self.failing
        {
            if self.creates.is_empty() && failing.probe_at.is_none() {
                failing.schedule();
            }
            return;
        }
        let mut in_flight = self.creates.len();
        for index in 0..self.sealed.len() {
            if in_flight >= MAX_IN_FLIGHT_CREATES {
                break;
            }
            let batch = &self.sealed[index];
            if !matches!(batch.state, CreateState::Pending) {
                continue;
            }
            let delay = if batch.attempts == 0 {
                Duration::ZERO
            } else {
                CREATE_RETRY_DELAY
            };
            self.start_create(index, delay);
            in_flight += 1;
        }
    }

    /// Attempts the lowest pending batch of a failing store, whose delay
    /// elapsed. After stop began `start_creates` uploads the backlog instead.
    fn probe(&mut self) {
        let Some(failing) = &mut self.failing else {
            return;
        };
        failing.probe_at = None;
        if self.is_stopped() {
            return;
        }
        if let Some(index) = self
            .sealed
            .iter()
            .position(|batch| matches!(batch.state, CreateState::Pending))
        {
            self.start_create(index, Duration::ZERO);
        }
    }

    /// Starts the create of the sealed batch at `index` after `delay`.
    fn start_create(&mut self, index: usize, delay: Duration) {
        let batch = &mut self.sealed[index];
        batch.state = CreateState::InFlight;
        batch.attempts += 1;
        let object_seq = batch.object_seq;
        let sealed_at = batch.sealed_at;
        let bytes = batch.bytes.clone();
        let io = self.io.clone();
        self.creating.insert(object_seq);
        #[cfg(any(test, feature = "testing"))]
        let mut creates_held = self.creates_held.clone();
        #[cfg(any(test, feature = "testing"))]
        let creates_fail = self.creates_fail.clone();
        #[cfg(any(test, feature = "testing"))]
        let next_create_fails_after_write = self.next_create_fails_after_write.clone();
        #[cfg(any(test, feature = "testing"))]
        let parked_creates = self.parked_creates.clone();
        self.creates.push(Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            // The store was dropped while the create was parked: it never
            // runs.
            #[cfg(any(test, feature = "testing"))]
            if *creates_held.borrow() {
                parked_creates.send_modify(|count| *count += 1);
            }
            #[cfg(any(test, feature = "testing"))]
            if creates_held.wait_for(|held| !*held).await.is_err() {
                return (object_seq, Err(ObjectStoreWalStoppedSnafu.build()));
            }
            #[cfg(any(test, feature = "testing"))]
            if creates_fail.load(Ordering::Acquire) {
                let result = injected_create_failure(io.as_ref(), object_seq);
                record_create(&result, Some(sealed_at));
                return (object_seq, result);
            }
            let created = io.put_if_absent(object_seq, bytes).await;
            #[cfg(any(test, feature = "testing"))]
            let created = match created {
                Ok(_) if next_create_fails_after_write.swap(false, Ordering::AcqRel) => {
                    injected_create_failure(io.as_ref(), object_seq)
                }
                created => created,
            };
            record_create(&created, Some(sealed_at));
            (object_seq, created)
        }));
    }

    fn on_create_completed(&mut self, object_seq: u64, result: Result<PutResult>) {
        self.creating.remove(&object_seq);
        #[cfg(any(test, feature = "testing"))]
        if matches!(result, Err(Error::WalObjectStore { .. })) {
            self.failed_creates.send_modify(|count| *count += 1);
        }
        // A batch that the store gave up when it poisoned itself or dropped
        // its backlog: the object may exist, but it is off the chain, and
        // collection may delete it now.
        let Some(index) = self
            .sealed
            .iter()
            .position(|batch| batch.object_seq == object_seq)
        else {
            self.start_creates();
            self.start_deletes();
            return;
        };
        self.last_create_failed
            .store(result.is_err(), Ordering::Release);
        match result {
            Ok(_) => {
                self.sealed[index].state = CreateState::Created;
                self.leave_failing("a create succeeded");
            }
            // The entries were handed out, so the store repeats a create that
            // the object store did not confirm under the same sequence with
            // the same bytes, which an identical retry accepts: after
            // `CREATE_RETRY_DELAY`, or with backoff while the store is
            // failing.
            Err(error @ Error::WalObjectStore { .. }) if !self.is_stopped() => {
                self.sealed[index].state = CreateState::Pending;
                self.on_create_failed(error);
            }
            // After stop began the backlog gets one attempt, and a failure
            // drops it.
            Err(error @ Error::WalObjectStore { .. }) => self.drop_backlog(Arc::new(error)),
            // Any object at the sequence of a batch whose entries were handed
            // out, of an earlier epoch too, can neither be overwritten nor be
            // given up: recovery starts every instance above the sequences an
            // earlier one can have handed out, so such an object belongs to
            // another writer of the prefix.
            Err(error) => {
                self.poison(error);
                return;
            }
        }
        self.settle();
    }

    /// Records the failure of a create the store repeats. An error that
    /// requires intervention makes the store failing; while it is, every
    /// error becomes the one rejected appends carry.
    fn on_create_failed(&mut self, error: Error) {
        let Some(failing) = &mut self.failing else {
            if requires_intervention(&error) {
                self.enter_failing(
                    error,
                    "the object store refused a create with an error that repeating it cannot settle",
                    1,
                );
            }
            return;
        };
        failing.failed_creates += 1;
        failing.error = Arc::new(error);
        if failing.logged_at.elapsed() >= FAILING_LOG_INTERVAL {
            failing.logged_at = Instant::now();
            warn!(
                failing.error;
                "Object store WAL still cannot upload its backlog of {} objects after {:?}: {} creates failed, {} appends rejected, next attempt in {:?}",
                self.sealed.len(),
                failing.since.elapsed(),
                failing.failed_creates,
                failing.rejected_appends,
                failing.delay
            );
        }
    }

    /// Makes the store failing because of `reason`, with `error` as the error
    /// rejected appends carry, see [`Failing`].
    fn enter_failing(&mut self, error: Error, reason: &str, failed_creates: u64) {
        error!(
            error;
            "Object store WAL cannot upload its backlog of {} objects: {reason}. Appends are rejected until a create succeeds; the backlog is kept and attempted again with backoff",
            self.sealed.len()
        );
        METRIC_OBJECT_STORE_WAL_FAILING.inc();
        #[cfg(any(test, feature = "testing"))]
        self.failing_state.send_replace(true);
        // The append held back would wait for an upload that is not coming.
        let stalled = self.take_stalled();
        let now = Instant::now();
        let failing = self.failing.insert(Failing {
            error: Arc::new(error),
            since: now,
            delay: CREATE_RETRY_DELAY,
            probe_at: None,
            failed_creates,
            rejected_appends: 0,
            logged_at: now,
        });
        if let Some((_, response)) = stalled {
            let _ = response.send(Err(failing.reject()));
        }
        // So would a release. Its record stays in the backlog, or in
        // `failed_releases` when its batch was not sealed, and becomes
        // durable with the backlog.
        for batch in &mut self.sealed {
            for waiter in batch.release_waiters.drain(..) {
                let _ = waiter.send(Err(failing.reject()));
            }
        }
        let held_releases = std::mem::take(&mut self.held_releases);
        let mut kept = Vec::with_capacity(held_releases.len());
        for (release, waiter) in held_releases {
            let _ = waiter.send(Err(failing.reject()));
            kept.push(release);
        }
        // And a writer, which is answered within the bound of the run.
        let error = failing.error.clone();
        self.durable_waiters.fail(Some(WaitKind::Write), || {
            ObjectStoreWalFailingSnafu.into_error(error.clone())
        });
        for release in kept {
            self.keep_release(release);
        }
    }

    /// Ends the failing state, if the store is in it, because of `reason`.
    fn leave_failing(&mut self, reason: &str) {
        let Some(failing) = self.failing.take() else {
            return;
        };
        #[cfg(any(test, feature = "testing"))]
        self.failing_state.send_replace(false);
        self.healthy_since = Instant::now();
        METRIC_OBJECT_STORE_WAL_FAILING.dec();
        info!(
            "Object store WAL is no longer failing, {reason}, after {:?}: {} creates failed, {} appends rejected",
            failing.since.elapsed(),
            failing.failed_creates,
            failing.rejected_appends
        );
    }

    /// Returns when the store becomes failing unless its lowest pending
    /// batch is indexed first: `transient_bound` after that batch was sealed
    /// or the store last left the failing state, whichever is later. The
    /// deadline does not depend on what the creates of the batch did, so a
    /// run of transient failures and a create that never answers are both
    /// bounded. `None` while the store is failing, after stop began, which
    /// gives every batch one more attempt, and once it is terminal.
    fn transient_deadline(&self) -> Option<Instant> {
        if self.failing.is_some() || self.is_stopped() || terminal(&self.terminal_error).is_some() {
            return None;
        }
        let front = self.sealed.front()?;
        Some(front.sealed_at.max(self.healthy_since) + self.transient_bound)
    }

    /// Makes the store failing once its lowest pending batch passed the
    /// transient deadline, without waiting for the creates in flight.
    fn on_transient_deadline(&mut self) {
        let Some(deadline) = self.transient_deadline() else {
            return;
        };
        if Instant::now() < deadline {
            return;
        }
        let Some(front) = self.sealed.front() else {
            return;
        };
        let error = WalObjectUnconfirmedSnafu {
            path: self.io.object_path(front.object_seq),
            bound: self.transient_bound,
        }
        .build();
        self.enter_failing(
            error,
            "an object did not become durable within max_unpersisted_age",
            0,
        );
        self.start_creates();
    }

    /// Returns when stop gives up the creates and deletes it waits for:
    /// `max_unpersisted_age` after stop began, while any is in flight.
    fn stop_deadline(&self) -> Option<Instant> {
        if self.creates.is_empty() && self.deletes.is_empty() {
            return None;
        }
        self.stop_began_at.map(|at| at + self.transient_bound)
    }

    /// Gives up what stop still waits for once the stop deadline passed, so
    /// that a create or a delete that never completes cannot hold the stop
    /// up: the backlog is dropped and `stop` reports it, the creates in
    /// flight are abandoned, whose objects may still land and are collected
    /// like those of any dropped batch, and the deletes in flight are
    /// abandoned, whose objects stay indexed.
    fn on_stop_deadline(&mut self) {
        let Some(deadline) = self.stop_deadline() else {
            return;
        };
        if Instant::now() < deadline {
            return;
        }
        warn!(
            "Object store WAL abandons {} creates and {} deletes that did not complete within {:?} after stop began",
            self.creates.len(),
            self.deletes.len(),
            self.transient_bound
        );
        if let Some(front) = self.sealed.front() {
            // The front batch was sealed before stop began, or when it did.
            let error = WalObjectUnconfirmedSnafu {
                path: self.io.object_path(front.object_seq),
                bound: self.transient_bound,
            }
            .build();
            self.drop_backlog(Arc::new(error));
        }
        self.creates = FuturesUnordered::new();
        self.creating.clear();
        self.deletes = FuturesUnordered::new();
        if let Withdrawal::Deleting { waiters, .. } = &mut self.withdrawal {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(());
            }
            self.withdrawal = Withdrawal::Settled;
        }
        self.deleting.abandon_all();
    }

    /// Indexes the sealed batches from the front as far as they are created,
    /// admits releases and appends that waited for room, and starts the
    /// creates that a free slot allows.
    fn settle(&mut self) {
        while let Some(front) = self.sealed.front() {
            if !matches!(front.state, CreateState::Created) || !self.index_front() {
                break;
            }
        }
        // Stop seals the records of releases that failed once there is room,
        // unless it dropped the backlog.
        if self.is_stopped() {
            self.flush_open_batch();
        }
        self.seal_held_releases();
        self.start_creates();
        self.release_stalled();
    }

    /// Indexes the created object at the front and answers its waiters.
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
        METRIC_OBJECT_STORE_WAL_RELEASE_RECORDS_TOTAL.inc_by(batch.releases.len() as u64);
        self.last_indexed = ChainLink {
            object_seq: batch.object_seq,
            epoch: self.epoch,
        };
        // The collection starts before the waiters learn that the object is
        // indexed.
        self.update_reclaim_boundary();
        if batch.reclaim_boundary > self.durable_reclaim_boundary {
            self.durable_reclaim_boundary = batch.reclaim_boundary;
            self.collect();
        }
        for admitted_at in &batch.admitted_at {
            METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS.observe(admitted_at.elapsed().as_secs_f64());
        }
        for waiter in batch.release_waiters {
            let _ = waiter.send(Ok(()));
        }
        #[cfg(any(test, feature = "testing"))]
        for waiter in batch.seal_waiters {
            let _ = waiter.send(Ok(()));
        }
        // Only the regions of the object can have become durable through a
        // later id.
        for region_id in batch.min_entry_ids.keys() {
            let lowest_pending = self.lowest_pending_entry_id(*region_id);
            self.durable_waiters.resolve(*region_id, lowest_pending);
        }
        true
    }

    /// Returns the lowest id of the region that was handed out and is not
    /// durable yet. The sealed batches are in sequence order and the open
    /// batch is above all of them. Only indexing takes an id out of the
    /// pending batches while the store runs; poisoning and a stop that drops
    /// the backlog take them out too and record the error that every later
    /// wait meets first, see [`handle_wait_durable`](Self::handle_wait_durable).
    /// So a handed-out id is never taken for durable before its object is
    /// indexed.
    fn lowest_pending_entry_id(&self, region_id: RegionId) -> Option<EntryId> {
        self.sealed
            .iter()
            .find_map(|batch| batch.min_entry_ids.get(&region_id).copied())
            .or_else(|| {
                self.next_object_seq
                    .filter(|_| self.open_batch.holds_region(region_id))
                    .map(|object_seq| entry_id(object_seq, 1))
            })
    }

    /// Drops the backlog after a create failed once stop began: every batch
    /// that is not indexed, including batches already created and batches
    /// whose create is in flight. Their sequences are not reused: a create
    /// that reported an error may still have written its object. Every
    /// caller waiting for the backlog learns that the store stopped, and
    /// `stop` reports `error`, whether its caller has arrived yet or not, as
    /// does every later wait for an entry that is not durable: entries that
    /// were handed out are lost.
    fn drop_backlog(&mut self, error: Arc<Error>) {
        for mut batch in std::mem::take(&mut self.sealed) {
            self.unindexed.insert(batch.object_seq);
            self.keep_releases(&mut batch);
            batch.fail(|| ObjectStoreWalStoppedSnafu.build());
        }
        self.reset_open_batch();
        self.durable_waiters
            .fail(None, || ObjectStoreWalStoppedSnafu.build());
        self.stop_error.get_or_insert(error);
        self.leave_failing("its backlog was dropped");
    }

    /// Keeps the release records of a batch that failed for the next sealed
    /// batch.
    fn keep_releases(&mut self, batch: &mut SealedBatch) {
        for release in batch.releases.drain(..) {
            self.keep_release(release);
        }
    }

    fn keep_release(&mut self, release: Release) {
        let entry_id = self
            .failed_releases
            .entry(release.region_id)
            .or_insert(release.entry_id);
        *entry_id = (*entry_id).max(release.entry_id);
    }

    /// Records `error` as terminal and fails every caller waiting for an
    /// entry that is not durable, every release waiting for its object and
    /// the append held back with it, or with the stopped error if the store
    /// was stopped meanwhile. Creates in flight run to
    /// completion, but their outcome is ignored: the backlog is discarded and
    /// `stop` reports it. Returns the recorded error.
    fn poison(&mut self, error: Error) -> Arc<Error> {
        let error = set_terminal(&self.terminal_error, error);
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
        for (_, waiter) in self.held_releases.drain(..) {
            let _ = waiter.send(Err(failure()));
        }
        self.reset_open_batch();
        if let Some((_, response)) = self.take_stalled() {
            let _ = response.send(Err(failure()));
        }
        self.durable_waiters.fail(None, failure);
        self.stop_error.get_or_insert(error.clone());
        self.leave_failing("the store is poisoned");
        error
    }

    /// Drops the entries of the open batch; the next admission hands out the
    /// same ids again under the sequence the batch is at.
    fn reset_open_batch(&mut self) {
        self.open_batch.reset();
        self.open_admitted_at.clear();
    }

    /// Answers a caller waiting until the region is durable through
    /// `entry_id`, now or once the entries are indexed.
    ///
    /// A durable entry is answered at once, also by a store that recorded a
    /// terminal error. Otherwise a dropped backlog or a poisoned store fails
    /// the wait, so no entry that was given up is ever certified, whether
    /// the wait arrived before or after the event. A store that is terminal
    /// because its sequence is exhausted still creates the batches it
    /// sealed, so a wait for their entries waits. While the store is
    /// failing, a [`WaitKind::Write`] wait fails at once and a
    /// [`WaitKind::Barrier`] wait waits.
    fn handle_wait_durable(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        kind: WaitKind,
        response: oneshot::Sender<Result<()>>,
    ) {
        let durable = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            catalog.region_max_entry_id(region_id).unwrap_or(0)
        };
        if entry_id <= durable {
            let _ = response.send(Ok(()));
            return;
        }
        if let Some(error) = &self.stop_error {
            let _ = response.send(Err(shared(error)));
            return;
        }
        let lowest_pending = self.lowest_pending_entry_id(region_id);
        if let Some(error) = terminal(&self.terminal_error)
            && lowest_pending.is_none_or(|pending| pending > entry_id)
        {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        if lowest_pending.is_none_or(|pending| pending > entry_id) {
            let _ = response.send(Ok(()));
            return;
        }
        if kind == WaitKind::Write
            && let Some(failing) = &self.failing
        {
            let _ = response.send(Err(
                ObjectStoreWalFailingSnafu.into_error(failing.error.clone())
            ));
            return;
        }
        self.durable_waiters
            .push(region_id, entry_id, DurableWaiter { kind, response });
        #[cfg(any(test, feature = "testing"))]
        self.durability_waits.send_modify(|count| *count += 1);
    }

    /// Starts draining: the open batch is sealed now, unless stop began,
    /// which sealed the `enqueued` backlog, and the held-back append and every
    /// later one are refused.
    fn handle_drain(&mut self) {
        self.draining = true;
        if !self.is_stopped() {
            self.flush_open_batch();
        }
        self.release_stalled();
    }

    /// Gives up the start object while it is the only object this store
    /// knows of: the catalog indexes nothing else, nothing was ever handed an
    /// id, no batch, append or release waits, no other object was listed or
    /// created, no region's history started in the store, and the store is
    /// neither failing nor terminal. A release seals its own batch without
    /// handing an id, so the catalog decides once its object is indexed, and
    /// the object it may then leave alone holds a segment. The store then
    /// records a terminal error, so nothing is admitted that would link to
    /// the object, and deletes it like a collected object; `response` is
    /// answered once that delete settled, or at once when the store keeps
    /// the object. A later request waits for the same delete.
    fn handle_withdraw(&mut self, response: oneshot::Sender<()>) {
        match &mut self.withdrawal {
            Withdrawal::Kept => {}
            Withdrawal::Deleting { waiters, .. } => {
                waiters.push(response);
                return;
            }
            Withdrawal::Settled => {
                let _ = response.send(());
                return;
            }
        }
        let started_regions = self
            .started_regions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !started_regions.is_empty()
            || terminal(&self.terminal_error).is_some()
            || self.failing.is_some()
            || !self.issued_entry_ids.is_empty()
            || !self.open_batch.is_empty()
            || !self.sealed.is_empty()
            || !self.creates.is_empty()
            || self.stalled.is_some()
            || !self.unindexed.is_empty()
            || !self.failed_releases.is_empty()
        {
            let _ = response.send(());
            return;
        }
        // The start object is the one object indexed, and the only one
        // without segments: a release object can be indexed alone once it
        // collected the start object.
        let start_object = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            let mut objects = catalog.objects_in_order();
            match (objects.next(), objects.next()) {
                (Some((object_seq, [])), None) => Some(object_seq),
                _ => None,
            }
        };
        let Some(object_seq) = start_object else {
            let _ = response.send(());
            return;
        };
        let path = self.io.object_path(object_seq);
        self.terminal_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(WalStartWithdrawnSnafu { path }.build()));
        drop(started_regions);
        self.deleting.start(object_seq);
        let io = self.io.clone();
        self.deletes.push(Box::pin(async move {
            (object_seq, io.delete(object_seq).await)
        }));
        self.withdrawal = Withdrawal::Deleting {
            object_seq,
            waiters: vec![response],
        };
    }

    /// Makes sure no id of the region at or below `entry_id` is assigned
    /// from now on, then records the obsolete watermark of the region;
    /// neither is done when the sequence cannot be raised. A raised floor is
    /// answered once the object that marks it is durable.
    fn handle_obsolete(
        &mut self,
        region_id: RegionId,
        entry_id: EntryId,
        watermark: EntryId,
        response: oneshot::Sender<Result<()>>,
    ) {
        let result = self
            .raise_sequence_floor(region_id, entry_id)
            .inspect(|_| record_obsolete(&self.obsolete_entry_ids, region_id, watermark));
        if result.is_ok() {
            self.update_reclaim_boundary();
            self.collect();
        }
        match result {
            Ok(true) => {
                if let Some(batch) = self.sealed.back_mut() {
                    batch.release_waiters.push(response);
                }
            }
            Ok(false) => {
                let _ = response.send(Ok(()));
            }
            Err(error) => {
                let _ = response.send(Err(error));
            }
        }
    }

    /// Moves the watermark of the region up to the release and seals the
    /// open batch with a release record of the region; `response` is
    /// answered once that object is durable and indexed. The batch carries
    /// the reclaim boundary the watermark allows, so the object makes both
    /// durable at once.
    ///
    /// The watermark is not clamped to the durable entry id: a released
    /// region is never replayed, and the record is durable only once every
    /// object sealed before it is. A release waits in `held_releases` while
    /// the sealed batches are full or a raised floor is not durable, and the
    /// releases held back are sealed together into the next batch that fits.
    fn handle_release(
        &mut self,
        region_id: RegionId,
        full: bool,
        response: oneshot::Sender<Result<()>>,
    ) {
        if self.is_stopped() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
            return;
        }
        if let Some(error) = terminal(&self.terminal_error) {
            let _ = response.send(Err(shared(&error)));
            return;
        }
        let entry_id = if full {
            EntryId::MAX
        } else {
            self.issued_entry_ids.get(&region_id).copied().unwrap_or(0)
        };
        record_obsolete(&self.obsolete_entry_ids, region_id, entry_id);
        self.update_reclaim_boundary();
        self.collect();
        let release = Release {
            region_id,
            entry_id,
        };
        // A failing store admits nothing behind its backlog: the release
        // fails at once, and its record is kept like that of a batch that
        // failed, sealed once a create succeeds.
        if let Some(failing) = &mut self.failing {
            let _ = response.send(Err(failing.reject()));
            self.keep_release(release);
            return;
        }
        self.held_releases.push((release, response));
        self.seal_held_releases();
    }

    /// Seals the releases held back into one batch once there is room, and
    /// answers their callers once that object is durable and indexed.
    fn seal_held_releases(&mut self) {
        if self.held_releases.is_empty() || !self.has_room() || self.is_stopped() {
            return;
        }
        let (releases, waiters): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.held_releases).into_iter().unzip();
        if self.seal(releases)
            && let Some(batch) = self.sealed.back_mut()
        {
            batch.release_waiters.extend(waiters);
            return;
        }
        // Sealing failed and poisoned the store.
        if let Some(error) = terminal(&self.terminal_error) {
            for waiter in waiters {
                let _ = waiter.send(Err(shared(&error)));
            }
        }
    }

    /// Raises the reclaim boundary to what the catalog and the obsolete
    /// watermarks allow.
    fn update_reclaim_boundary(&mut self) {
        let boundary = {
            let obsolete_entry_ids = self
                .obsolete_entry_ids
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.catalog
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .reclaim_boundary(self.reclaim_boundary, &obsolete_entry_ids)
        };
        self.reclaim_boundary = boundary;
    }

    /// Starts a collection, which also attempts the deletes that failed since
    /// the last one again.
    fn collect(&mut self) {
        self.recheck_skipped_sequences();
        self.failed_deletes.clear();
        self.start_deletes();
    }

    /// Makes the collection delete the sequences the start object skipped
    /// again once their recheck interval passed, so that a late object that
    /// landed after the delete of its absent sequence is collected too, see
    /// [`LATE_OBJECT_RECHECK`].
    fn recheck_skipped_sequences(&mut self) {
        let Some(skipped) = &mut self.skipped else {
            return;
        };
        let now = Instant::now();
        if now < skipped.next_check {
            return;
        }
        skipped.interval = (skipped.interval * 2).min(MAX_LATE_OBJECT_RECHECK);
        skipped.next_check = now + skipped.interval;
        for object_seq in skipped.sequences.clone() {
            if self.unindexed.insert(object_seq) {
                self.rechecked.insert(object_seq);
            }
        }
    }

    /// Starts deleting the objects below the durable reclaim boundary while
    /// fewer than [`MAX_IN_FLIGHT_DELETES`] deletes are in flight, skipping
    /// objects whose create is in flight or whose delete failed since the
    /// collection started.
    fn start_deletes(&mut self) {
        if self.is_stopped() || terminal(&self.terminal_error).is_some() {
            return;
        }
        let free = MAX_IN_FLIGHT_DELETES.saturating_sub(self.deletes.len());
        let boundary = self.durable_reclaim_boundary;
        let candidates = {
            let catalog = self.catalog.read().unwrap_or_else(PoisonError::into_inner);
            catalog
                .objects_in_order()
                .map(|(object_seq, _)| object_seq)
                .take_while(|object_seq| *object_seq < boundary)
                .chain(self.unindexed.range(..boundary).copied())
                .filter(|object_seq| {
                    !self.deleting.contains(*object_seq)
                        && !self.failed_deletes.contains(object_seq)
                        && !self.creating.contains(object_seq)
                })
                .take(free)
                .collect::<Vec<_>>()
        };
        for object_seq in candidates {
            self.deleting.start(object_seq);
            let io = self.io.clone();
            self.deletes.push(Box::pin(async move {
                (object_seq, io.delete(object_seq).await)
            }));
        }
    }

    /// Unindexes the object `object_seq` and drops its holes once its delete
    /// succeeded, then settles its attempt, so a read that the attempt
    /// releases sees the outcome in the catalog.
    fn on_delete_completed(&mut self, object_seq: u64, result: Result<()>) {
        if let Withdrawal::Deleting {
            object_seq: withdrawn,
            waiters,
        } = &mut self.withdrawal
            && *withdrawn == object_seq
        {
            let path = self.io.object_path(object_seq);
            match result {
                Ok(()) => {
                    let mut catalog = self.catalog.write().unwrap_or_else(PoisonError::into_inner);
                    if let Some(footer) = catalog.remove_object(object_seq) {
                        METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.dec();
                        METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.sub(object_len(&footer) as i64);
                    }
                    info!(
                        "Deleted the start object {path} of the object store WAL, whose prefix held no object, after refusing a region without its history"
                    );
                }
                Err(error) => warn!(
                    error;
                    "Failed to delete the start object {path} of the object store WAL after refusing a region without its history; the next open passes regions that never flushed"
                ),
            }
            for waiter in waiters.drain(..) {
                let _ = waiter.send(());
            }
            self.withdrawal = Withdrawal::Settled;
            self.deleting.finish(object_seq);
            return;
        }
        match result {
            Ok(()) => {
                self.unindexed.remove(&object_seq);
                let rechecked = self.rechecked.remove(&object_seq);
                let mut catalog = self.catalog.write().unwrap_or_else(PoisonError::into_inner);
                let footer = catalog.remove_object(object_seq);
                if let Some(footer) = &footer {
                    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.dec();
                    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.sub(object_len(footer) as i64);
                }
                let mut wal_holes = self
                    .wal_holes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                for entry in footer.iter().flatten() {
                    if let Some(holes) = wal_holes.get_mut(&entry.region_id) {
                        holes.remove(&object_seq);
                    }
                }
                if !rechecked {
                    METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.inc();
                }
            }
            Err(error) => {
                warn!(
                    error;
                    "Failed to delete WAL object {} (sequence {}), it is deleted again at the next collection",
                    self.io.object_path(object_seq),
                    object_seq
                );
                METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.inc();
                self.failed_deletes.insert(object_seq);
            }
        }
        self.start_deletes();
        self.deleting.finish(object_seq);
    }

    /// Moves the sequence of the next object above the object that holds
    /// `entry_id` unless it is there already, and returns whether it moved.
    /// The move fails while the open batch or a sealed batch holds ids or
    /// records, since the sealed sequences stay consecutive, see
    /// [`UNLISTED_SEQUENCES`]. A floor that moved is marked by an object
    /// without entries at the floor, and no other batch is sealed until that
    /// object is indexed, so a later recovery lists an object at or above the
    /// floor before any id above it is handed out. A floor that does not fit
    /// an entry id poisons the store like an exhausted sequence.
    fn raise_sequence_floor(&mut self, region_id: RegionId, entry_id: EntryId) -> Result<bool> {
        // Nothing is assigned an id after stop began.
        if self.is_stopped() {
            return Ok(false);
        }
        if let Some(error) = terminal(&self.terminal_error) {
            return Err(shared(&error));
        }
        // An id the store handed out is never handed out again while it
        // runs: a create that the object store did not confirm is repeated
        // under its sequence and any other failure poisons the store. Every
        // later id of the region is greater, so the id needs no floor even
        // before it is durable.
        if entry_id <= self.issued_entry_ids.get(&region_id).copied().unwrap_or(0) {
            return Ok(false);
        }
        let sequence_floor = sequence_floor(entry_id);
        let Some(next_object_seq) = self.next_object_seq else {
            return Ok(false);
        };
        if next_object_seq >= sequence_floor {
            return Ok(false);
        }
        ensure!(
            self.open_batch.is_empty() && self.sealed.is_empty() && self.held_releases.is_empty(),
            WalObjectSequenceUnsettledSnafu {
                object_seq: next_object_seq,
            }
        );
        if sequence_floor < OBJECT_SEQ_LIMIT {
            self.next_object_seq = Some(sequence_floor);
            if self.seal_batch(Vec::new(), true) {
                Ok(true)
            } else {
                Err(terminal(&self.terminal_error)
                    .map(|error| shared(&error))
                    .unwrap_or_else(|| ObjectStoreWalStoppedSnafu.build()))
            }
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

    /// Begins stopping, see [`ObjectStoreLogStore::begin_stop`]: nothing is
    /// admitted from now on, the append held back and the releases waiting
    /// for their objects fail with the stopped error, whose records stay in
    /// the backlog, and the open batch is sealed with the records of releases
    /// that failed, so that the whole backlog is uploaded at once, a failing
    /// store's included. Idempotent.
    fn begin_stop(&mut self) {
        self.stop_began_at.get_or_insert_with(Instant::now);
        if let Some((_, response)) = self.take_stalled() {
            let _ = response.send(Err(ObjectStoreWalStoppedSnafu.build()));
        }
        for (release, waiter) in std::mem::take(&mut self.held_releases) {
            let _ = waiter.send(Err(ObjectStoreWalStoppedSnafu.build()));
            self.keep_release(release);
        }
        for batch in &mut self.sealed {
            for waiter in batch.release_waiters.drain(..) {
                let _ = waiter.send(Err(ObjectStoreWalStoppedSnafu.build()));
            }
        }
        self.flush_open_batch();
        self.start_creates();
    }

    /// Stops: begins stopping and answers `response` from
    /// [`finish_stop`](Self::finish_stop) once nothing is in flight.
    fn handle_stop(&mut self, response: oneshot::Sender<Result<()>>) {
        self.stop.push(response);
        self.begin_stop();
    }

    /// Answers the callers of `stop` once every sealed batch is settled and
    /// every create and delete has completed or was abandoned at the stop
    /// deadline. Returns true when the actor is done.
    fn finish_stop(&mut self) -> bool {
        if self.stop.is_empty()
            || !self.sealed.is_empty()
            || !self.creates.is_empty()
            || !self.deletes.is_empty()
        {
            return false;
        }
        self.durable_waiters
            .fail(None, || ObjectStoreWalStoppedSnafu.build());
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

    /// Fails every caller that waits for the store; the creates are
    /// dropped with the actor.
    #[cfg(any(test, feature = "testing"))]
    fn handle_crash(&mut self) {
        let stopped = || ObjectStoreWalStoppedSnafu.build();
        for batch in self.sealed.drain(..) {
            batch.fail(stopped);
        }
        for (_, waiter) in self.held_releases.drain(..) {
            let _ = waiter.send(Err(stopped()));
        }
        self.reset_open_batch();
        if let Some((_, response)) = self.take_stalled() {
            let _ = response.send(Err(stopped()));
        }
        self.durable_waiters.fail(None, stopped);
        for response in self.stop.drain(..) {
            let _ = response.send(Err(stopped()));
        }
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Drop for Actor {
    /// Settles the deletes the actor still had in flight, so no read waits
    /// for an attempt that will never complete, and releases the append still
    /// held back, whose caller is gone or about to be.
    fn drop(&mut self) {
        self.deleting.abandon_all();
        self.take_stalled();
        self.leave_failing("its actor exited");
        #[cfg(test)]
        LIVE_ACTORS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Actors not dropped yet. An actor outlives the test that dropped its store
/// and may still move the process-wide metrics.
#[cfg(test)]
static LIVE_ACTORS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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

/// Counts the outcome of a conditional create and, for the object of a batch
/// sealed at `sealed_at`, times a success. The create of a batch is counted
/// whether or not the batch is still there to be indexed: an object a
/// poisoned store gave up on is as durable as any other.
fn record_create(result: &Result<PutResult>, sealed_at: Option<Instant>) {
    match result {
        Ok(_) => {
            if let Some(sealed_at) = sealed_at {
                METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS
                    .observe(sealed_at.elapsed().as_secs_f64());
            }
            METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL.inc();
        }
        Err(Error::WalObjectStore { .. }) => METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL.inc(),
        Err(Error::WalObjectConflict { .. }) => {
            METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL.inc()
        }
        Err(_) => {}
    }
}

/// Sets both indexed gauges to the objects `catalog` holds.
fn publish_indexed(catalog: &ObjectCatalog) {
    let (objects, bytes) = catalog
        .objects_in_order()
        .fold((0, 0), |(objects, bytes), (_, footer)| {
            (objects + 1, bytes + object_len(footer))
        });
    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.set(objects);
    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.set(bytes as i64);
}

/// Returns true for an object store error that repeating the request cannot
/// settle until someone changes the credentials, the configuration or the
/// bucket: denied access; an invalid configuration, which is how OpenDAL
/// reports a missing S3 bucket; and a write to a location that does not
/// exist, which is how OSS, GCS and Azure Blob Storage report a missing
/// bucket or container. A read that finds nothing is not one: it is what the
/// read that reconciles a create meets when the object is gone, and a repeat
/// creates it. Every other error, an HTTP 400 or 5xx, a timeout or a refused
/// connection, may pass by itself. A create that fails with an error
/// requiring intervention is still repeated, with the backoff [`Failing`]
/// describes.
///
/// Only the kind of the error decides. Its status cannot: on object storage
/// the store sits behind OpenDAL's retry layer, which marks every error it
/// returns as persistent, including those it never retried because they were
/// not temporary, so a 403 arrives with the status of a 503 that outlasted
/// the retries.
fn requires_intervention(error: &Error) -> bool {
    match error {
        Error::WalObjectStore {
            operation, error, ..
        } => match error.kind() {
            ErrorKind::PermissionDenied | ErrorKind::ConfigInvalid => true,
            ErrorKind::NotFound => *operation == "write",
            _ => false,
        },
        _ => false,
    }
}

/// Wraps an error that several callers receive.
fn shared(error: &Arc<Error>) -> Error {
    ObjectStoreWalSnafu.into_error(error.clone())
}

/// Returns whether collection deleted the object `object_seq` after a read
/// listed it. The set of deleting objects is observed before the catalog, the
/// reverse of the order the actor updates them, so an object that no longer
/// looks deleting but still looks indexed was never collected. An undecided
/// delete is waited for.
async fn is_collected(
    catalog: &RwLock<ObjectCatalog>,
    deleting: &DeletingObjects,
    object_seq: u64,
) -> bool {
    if let Some(mut settled) = deleting.attempt(object_seq) {
        let _ = settled.wait_for(|settled| *settled).await;
    }
    !catalog
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_object(object_seq)
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

/// Takes from `budget` the bytes the segment `entry` describes holds once
/// decoded, if they are free: its length plus a [`Record`] per entry.
fn try_reserve_prefetch_bytes(
    budget: &Arc<Semaphore>,
    entry: &FooterEntry,
) -> Option<OwnedSemaphorePermit> {
    let records = u64::from(entry.entry_count) * std::mem::size_of::<Record>() as u64;
    let bytes = u32::try_from(entry.segment_len.checked_add(records)?).ok()?;
    budget.clone().try_acquire_many_owned(bytes).ok()
}

/// Runs [`fetch_segment`] on a task of its own, so the fetch proceeds while
/// the read is busy with earlier segments, and returns the outcome with
/// `permit`. The task holds the permit, so the reserved bytes are released
/// only with the buffers of the fetch. Dropping the returned future, as a
/// dropped read does, aborts the task; a task that panics fails the fetch with
/// [`Error::WalSegmentFetch`].
fn spawn_fetch_segment(
    io: Arc<dyn WalObjectIo>,
    object_seq: u64,
    entry: FooterEntry,
    permit: Option<OwnedSemaphorePermit>,
) -> impl Future<Output = (Result<Vec<Record>>, Option<OwnedSemaphorePermit>)> + Send + 'static {
    let task_io = io.clone();
    let task = AbortOnDropHandle::new(common_runtime::spawn_global(async move {
        let fetched = fetch_segment(task_io.as_ref(), object_seq, &entry).await;
        (fetched, permit)
    }));
    async move {
        match task.await {
            Ok(fetched) => fetched,
            Err(error) => (
                Err(error).context(WalSegmentFetchSnafu {
                    path: io.object_path(object_seq),
                }),
                None,
            ),
        }
    }
}

/// Records the segment `entry` describes in the object `object_seq` as a hole
/// of its region, once per object, counts the skip and warns about it. An
/// object collected meanwhile is skipped without a hole: the catalog is held
/// while the hole is recorded, as a collection holds it while it drops the
/// holes of the object it removes.
fn record_skipped_segment(
    catalog: &RwLock<ObjectCatalog>,
    wal_holes: &WalHoles,
    io: &dyn WalObjectIo,
    object_seq: u64,
    entry: &FooterEntry,
    error: &Error,
) {
    let catalog = catalog.read().unwrap_or_else(PoisonError::into_inner);
    if !catalog.contains_object(object_seq) {
        debug!(
            "Skipped WAL object {} (sequence {}), collected after the read listed it: {error}",
            io.object_path(object_seq),
            object_seq
        );
        return;
    }
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

/// Encodes the entries of a batch, and its release records as the single
/// entry of [`RELEASE_REGION_ID`], whose id is at position 1 of the object;
/// segments are ordered by region id, so it is the last segment.
fn encode_batch(
    header: Header,
    entries: Vec<Entry>,
    releases: &BTreeMap<RegionId, EntryId>,
) -> Result<EncodedObject> {
    let release_record = (!releases.is_empty()).then(|| Record {
        region_id: RELEASE_REGION_ID,
        entry_id: entry_id(header.object_seq, 1),
        payload: encode_releases(releases),
    });
    let records = entries
        .into_iter()
        .map(|entry| Record {
            region_id: entry.region_id(),
            entry_id: entry.entry_id(),
            payload: Bytes::from(entry.into_bytes()),
        })
        .chain(release_record)
        .collect::<Vec<_>>();
    encode_object(header, &records)
}

/// What recovery rebuilt from the objects under a prefix.
#[derive(Debug)]
struct Recovered {
    catalog: ObjectCatalog,
    /// Sequence above every present object and every id the catalog holds.
    next_object_seq: u64,
    durable_entry_ids: HashMap<RegionId, EntryId>,
    /// The object the next object extends, `None` on an empty prefix.
    tip: Option<ChainLink>,
    /// Largest epoch any present object carries, zero on an empty prefix.
    max_epoch: u64,
    /// The reclaim boundary of the tip, zero on an empty prefix.
    reclaim_boundary: u64,
    /// Present objects that are not indexed, in sequence order.
    unindexed: Vec<u64>,
    /// The entry id each region is released through, from the release
    /// records of the indexed objects, see [`read_releases`].
    released_entry_ids: HashMap<RegionId, EntryId>,
    /// Number of objects whose header and footer were read.
    recovered_objects: u64,
}

/// The objects recovery fetched, ordered by sequence, and the listed objects
/// that were gone when their header or footer was fetched, with the error the
/// fetch met. A collection of an earlier instance may complete after the
/// listing.
struct FetchedObjects {
    objects: Vec<FetchedObject>,
    vanished: Vec<(u64, Error)>,
}

/// An object as recovery fetched it: its key, header and footer.
struct FetchedObject {
    object: ListedObject,
    header: Header,
    footer: Vec<FooterEntry>,
}

/// Rebuilds the catalog from object headers and footers,
/// so recovery costs a few small reads per object however large the objects
/// are. Segments are not read, except those of release records, see
/// [`read_releases`]; a segment checksum is verified by the read that decodes
/// it. Footers are fetched for up to [`RECOVERY_CONCURRENCY`] objects
/// at a time and indexed in sequence order, so the catalog checks the entry
/// ranges of every object against its predecessors like a sequential replay.
/// Only the objects on the chain [`select_chain`] picks are indexed.
async fn recover(io: &dyn WalObjectIo) -> Result<Recovered> {
    let objects = io.list().await?;
    let mut recovered = finish_recovery(fetch_footers(io, objects, RECOVERY_CONCURRENCY).await?)?;
    recovered.released_entry_ids = read_releases(io, &recovered.catalog).await?;
    Ok(recovered)
}

/// Reads the release records of the indexed objects, which seed the
/// watermarks of their regions: one range read of the segment of
/// [`RELEASE_REGION_ID`] per object that holds one. A segment that cannot be
/// fetched or does not decode fails recovery, whatever `on_corrupted_segment`
/// says: skipping it would lose the watermarks it seeds.
///
/// A release record needs no bookkeeping of its own. It is sealed once every
/// entry it covers was admitted, so it lies in the object holding the last
/// segment it covers or in a later one. Collection deletes, in any order,
/// only objects below the durable reclaim boundary, and recovery indexes only
/// objects at or above the boundary of the tip, which a start object carries
/// forward. So recovery indexes a release record whenever it indexes a
/// segment the record covers, and once the record is below the boundary so is
/// every segment it covers, which recovery never indexes again, whether
/// collection deleted it yet or not. A release in full also covers entries a
/// region would be assigned afterwards, so it is made only for a region that
/// writes nothing more.
async fn read_releases(
    io: &dyn WalObjectIo,
    catalog: &ObjectCatalog,
) -> Result<HashMap<RegionId, EntryId>> {
    let segments = catalog
        .objects_in_order()
        .filter_map(|(object_seq, footer)| {
            footer
                .iter()
                .find(|entry| entry.region_id == RELEASE_REGION_ID)
                .map(|entry| (object_seq, entry.clone()))
        })
        .collect::<Vec<_>>();
    let releases = futures::stream::iter(segments)
        .map(|(object_seq, entry)| async move {
            let bytes = io
                .get_range(object_seq, entry.segment_offset, entry.segment_len)
                .await?;
            decode_segment(&bytes, &entry)
                .and_then(|records| decode_releases(&records[0].payload))
                .with_context(|_| InvalidWalObjectSnafu {
                    path: io.object_path(object_seq),
                })
        })
        .buffered(RECOVERY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let mut released_entry_ids = HashMap::new();
    for release in releases.into_iter().flatten() {
        let entry_id = released_entry_ids
            .entry(release.region_id)
            .or_insert(release.entry_id);
        *entry_id = (*entry_id).max(release.entry_id);
    }
    Ok(released_entry_ids)
}

/// Indexes the chain of the fetched objects. Objects off the chain, orphans
/// and objects below the reclaim boundary of the tip, are not indexed, but no
/// later object takes their sequences, nor the sequences of vanished objects.
/// A vanished object at or above the boundary of the tip fails recovery with
/// the error its fetch met: collection never deletes such an object.
fn finish_recovery(fetched: FetchedObjects) -> Result<Recovered> {
    let FetchedObjects { objects, vanished } = fetched;
    let recovered_objects = objects.len() as u64;
    let headers = objects
        .iter()
        .map(|fetched| (fetched.object.object_seq, &fetched.header))
        .collect::<BTreeMap<_, _>>();
    let chain = select_chain(&headers);
    // Every open writes an object that starts a chain or extends a complete
    // one, and collection removes only objects below the reclaim boundary of
    // a later object, so present objects without any complete chain mean
    // objects of the chain are gone.
    ensure!(
        headers.is_empty() || !chain.is_empty(),
        CorruptedWalObjectSnafu {
            reason: format!(
                "no object among {} present objects completes a chain",
                headers.len()
            ),
        }
    );
    let tip = chain.last().map(|object_seq| ChainLink {
        object_seq: *object_seq,
        epoch: headers[object_seq].epoch,
    });
    let max_epoch = headers
        .values()
        .map(|header| header.epoch)
        .max()
        .unwrap_or(0);
    let last_listed = headers
        .keys()
        .copied()
        .chain(vanished.iter().map(|(object_seq, _)| *object_seq))
        .max();
    let after_listed = match last_listed {
        None => 0,
        Some(last_object_seq) => last_object_seq
            .checked_add(1)
            .context(WalObjectSequenceExhaustedSnafu { last_object_seq })?,
    };
    let reclaim_boundary = tip.map_or(0, |tip| headers[&tip.object_seq].reclaim_boundary);
    if let Some((_, error)) = vanished
        .into_iter()
        .find(|(object_seq, _)| *object_seq >= reclaim_boundary)
    {
        return Err(error);
    }
    let chain = chain.into_iter().collect::<HashSet<_>>();

    let mut catalog = ObjectCatalog::default();
    let mut unindexed = Vec::new();
    for FetchedObject { object, footer, .. } in objects {
        if chain.contains(&object.object_seq) {
            catalog
                .insert_object(object.object_seq, footer)
                .with_context(|_| InvalidWalObjectSnafu { path: object.path })?;
        } else {
            unindexed.push(object.object_seq);
        }
    }
    let next_object_seq = catalog.next_object_seq()?.max(after_listed);
    ensure!(
        next_object_seq < OBJECT_SEQ_LIMIT,
        WalObjectSequenceExhaustedSnafu {
            last_object_seq: next_object_seq - 1,
        }
    );
    let durable_entry_ids = durable_entry_ids(&catalog);
    Ok(Recovered {
        catalog,
        next_object_seq,
        durable_entry_ids,
        tip,
        max_epoch,
        reclaim_boundary,
        unindexed,
        released_entry_ids: HashMap::new(),
        recovered_objects,
    })
}

/// Returns, in sequence order, the objects on the chain that ends at the tip:
/// the complete object with the largest epoch, then the largest sequence.
///
/// An object is complete when every link on its chain holds, judged with the
/// reclaim boundary of that object: the chain starts at an object without a
/// predecessor or reaches a predecessor below the boundary, whether or not it
/// is present, and every other link names a present object that carries the
/// epoch the link records. The chain of the tip ends there, so objects below
/// its boundary are not replayed. Only one instance writes
/// under an epoch, so the epoch tells an object of the linking instance from
/// another object under the same sequence. A create that
/// was reported as failed may still leave its object, but every object
/// written after that failure links past it, and every instance writes under
/// an epoch above every object present when it opened, so neither such an
/// object nor a late object of an earlier instance ends the chosen chain.
fn select_chain(headers: &BTreeMap<u64, &Header>) -> Vec<u64> {
    let mut candidates = headers.iter().collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|(object_seq, header)| Reverse((header.epoch, **object_seq)));
    candidates
        .into_iter()
        .find_map(|(object_seq, _)| chain_ending_at(headers, *object_seq))
        .unwrap_or_default()
}

/// Returns, in sequence order, the chain that ends at `tip` if `tip` is
/// complete, see [`select_chain`].
fn chain_ending_at(headers: &BTreeMap<u64, &Header>, tip: u64) -> Option<Vec<u64>> {
    let reclaim_boundary = headers[&tip].reclaim_boundary;
    let mut chain = vec![tip];
    let mut header = headers[&tip];
    while let Some(link) = header.prev {
        if link.object_seq >= header.object_seq {
            return None;
        }
        if link.object_seq < reclaim_boundary {
            break;
        }
        header = headers
            .get(&link.object_seq)
            .filter(|prev| prev.epoch == link.epoch)?;
        chain.push(link.object_seq);
    }
    chain.reverse();
    Some(chain)
}

/// Writes the empty object that starts the epoch of this instance at
/// `object_seq`, linked to the recovered `tip` and carrying the recovered
/// `reclaim_boundary`, and returns it as the tip later objects extend,
/// together with the sequences of earlier-epoch objects it moved past. The
/// epoch is one above the sequence the create claims, so no two instances
/// share one however their opens interleave, and it is above the epoch of
/// every object recovery listed.
///
/// An object an earlier instance left at that sequence after recovery listed
/// the prefix has a lower epoch and never ends a chain, so the start object
/// moves to the next sequence and epoch. An object of an equal or later epoch
/// belongs to another writer of the prefix, and the conflict fails the open,
/// as does a create whose outcome is unknown: the next open counts the object
/// in either case. So does a create that finds the same bytes present:
/// another open that recovered the same objects writes an identical start
/// object, so this open cannot claim the epoch even if the object is its own.
async fn start_epoch(
    io: &dyn WalObjectIo,
    mut object_seq: u64,
    tip: Option<ChainLink>,
    reclaim_boundary: u64,
) -> Result<(ChainLink, Vec<u64>)> {
    let mut moved_past = Vec::new();
    loop {
        ensure!(
            object_seq < OBJECT_SEQ_LIMIT,
            WalObjectSequenceExhaustedSnafu {
                last_object_seq: OBJECT_SEQ_LIMIT - 1,
            }
        );
        let epoch = object_seq + 1;
        let header = Header {
            object_seq,
            epoch,
            prev: tip,
            reclaim_boundary,
        };
        let created = io
            .put_if_absent(object_seq, encode_object(header, &[])?.bytes)
            .await;
        record_create(&created, None);
        match created {
            Ok(PutResult::Created) => return Ok((ChainLink { object_seq, epoch }, moved_past)),
            Ok(PutResult::AlreadyPresent) => {
                return UnconfirmedWalEpochStartSnafu {
                    path: io.object_path(object_seq),
                    epoch,
                }
                .fail();
            }
            Err(error @ Error::WalObjectConflict { .. }) => {
                if epoch_of(io, object_seq).await? >= epoch {
                    return Err(error);
                }
                moved_past.push(object_seq);
                object_seq += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn epoch_of(io: &dyn WalObjectIo, object_seq: u64) -> Result<u64> {
    let head = io.get_range(object_seq, 0, HEADER_LEN as u64).await?;
    let header = decode_header(&head).with_context(|_| InvalidWalObjectSnafu {
        path: io.object_path(object_seq),
    })?;
    Ok(header.epoch)
}

/// Fetches and verifies the headers and footers of `objects`, up to
/// `concurrency` objects at a time, and returns them ordered by object sequence
/// whatever the order the fetches complete in. An object the object store no
/// longer has is returned as vanished; any other failure abandons the
/// remaining fetches.
async fn fetch_footers(
    io: &dyn WalObjectIo,
    objects: Vec<ListedObject>,
    concurrency: usize,
) -> Result<FetchedObjects> {
    let outcomes = futures::stream::iter(objects)
        .map(|object| async move {
            match fetch_footer(io, &object).await {
                Ok((header, footer)) => Ok(Ok(FetchedObject {
                    object,
                    header,
                    footer,
                })),
                Err(error) if is_not_found(&error) => Ok(Err((object.object_seq, error))),
                Err(error) => Err(error),
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let mut objects = Vec::with_capacity(outcomes.len());
    let mut vanished = Vec::new();
    for outcome in outcomes {
        match outcome {
            Ok(object) => objects.push(object),
            Err(gone) => vanished.push(gone),
        }
    }
    objects.sort_unstable_by_key(|fetched| fetched.object.object_seq);
    Ok(FetchedObjects { objects, vanished })
}

fn is_not_found(error: &Error) -> bool {
    matches!(error, Error::WalObjectStore { error, .. } if error.kind() == object_store::ErrorKind::NotFound)
}

/// Reads the header, trailer and footer of `object` and verifies them: the
/// header must carry the sequence of the key, the trailer must be well formed,
/// the footer must match the checksum the trailer holds and its segments must
/// tile the object body.
///
/// A short object is read whole. Otherwise the header and a window at the end
/// of the object are read concurrently, and the footer is read separately only
/// when it starts before the window.
async fn fetch_footer(
    io: &dyn WalObjectIo,
    object: &ListedObject,
) -> Result<(Header, Vec<FooterEntry>)> {
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

    let (header, trailer, footer_range) =
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
    Ok((header, footer))
}

/// Verifies the header and trailer of the object `object_seq` of `object_len`
/// bytes from its first bytes `head` and its last bytes `tail`, and returns
/// the header and the trailer with the range the footer occupies in the object.
fn locate_footer(
    object_seq: u64,
    object_len: usize,
    head: &[u8],
    tail: &[u8],
) -> Result<(Header, FixedTrailer, Range<usize>)> {
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
    Ok((header, trailer, footer_range))
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

    /// Describes the object store the objects live in, for messages.
    fn storage(&self) -> String;
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

    fn storage(&self) -> String {
        ObjectStoreIo::storage(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use common_base::readable_size::ReadableSize;
    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::status_code::StatusCode;
    use futures::FutureExt;
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use object_store::services::Memory;
    use object_store::util::with_retry_layers;
    use store_api::logstore::entry::{MultiplePartEntry, MultiplePartHeader};
    use tokio::time::timeout;

    use super::*;
    use crate::error::{WalObjectConflictSnafu, WalObjectStoreSnafu};
    use crate::object_store_wal::batch::{POSITION_LIMIT, entry_id};
    use crate::object_store_wal::format::{FOOTER_ENTRY_LEN, decode_object};

    const PREFIX: &str = "wal/datanodes/1/epochs/2";
    const WAIT: Duration = Duration::from_secs(30);
    /// Held by every test of the module: the metrics of the store are
    /// process-wide, so tests running in one process would otherwise move the
    /// counters and gauges that another test asserts on.
    static SERIALIZED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn memory_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
    }

    fn config(flush_interval: Duration, max_batch_bytes: u64) -> ObjectStoreWalConfig {
        ObjectStoreWalConfig {
            storage_provider: String::new(),
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
        ObjectStoreLogStore::try_new(object_store, config, 1, 2)
            .await
            .unwrap()
    }

    fn region(number: u32) -> RegionId {
        RegionId::new(1, number)
    }

    fn provider(region_id: RegionId) -> Provider {
        Provider::object_store_provider(region_id, PREFIX.to_string())
    }

    fn latest(store: &ObjectStoreLogStore, region_id: RegionId) -> EntryId {
        store.latest_entry_id(&provider(region_id)).unwrap()
    }

    /// The id of the entry at `position` of its region in object `object_seq`.
    fn id(object_seq: u64, position: u64) -> EntryId {
        entry_id(object_seq, position)
    }

    fn entry(store: &ObjectStoreLogStore, region_id: RegionId, data: &str) -> Entry {
        store
            .entry(data.as_bytes().to_vec(), 0, region_id, &provider(region_id))
            .unwrap()
    }

    /// Appends `entries` as a writer does: in the `durable` mode it returns
    /// once every region of the append is durable through its last id, as
    /// Mito answers its writers, and with the error of that wait otherwise.
    async fn append_acknowledged(
        store: &ObjectStoreLogStore,
        entries: Vec<Entry>,
    ) -> Result<AppendBatchResponse> {
        let response = store.append_batch(entries).await?;
        if store.defers_write_durability() {
            for (region_id, entry_id) in &response.last_entry_ids {
                store
                    .wait_write_durable(&provider(*region_id), *entry_id)
                    .await?;
            }
        }
        Ok(response)
    }

    async fn append(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        data: &str,
    ) -> Result<AppendBatchResponse> {
        append_acknowledged(store, vec![entry(store, region_id, data)]).await
    }

    fn spawn_append_batch(
        store: &Arc<ObjectStoreLogStore>,
        entries: Vec<Entry>,
    ) -> tokio::task::JoinHandle<Result<AppendBatchResponse>> {
        let store = store.clone();
        tokio::spawn(async move { append_acknowledged(&store, entries).await })
    }

    /// Appends `count` single-entry batches, waiting for each to be admitted
    /// before the next is sent, so they are admitted in order.
    async fn spawn_appends(
        store: &Arc<ObjectStoreLogStore>,
        region_id: RegionId,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>> {
        let admitted = *store.admitted_appends.borrow();
        let mut handles = Vec::with_capacity(count);
        for index in 1..=count {
            let entries = vec![entry(store, region_id, &format!("a{index}"))];
            handles.push(spawn_append_batch(store, entries));
            store
                .wait_for_admitted_appends(admitted + index)
                .await
                .unwrap();
        }
        handles
    }

    async fn object_seqs(io: &dyn WalObjectIo) -> Vec<u64> {
        io.list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .collect()
    }

    /// Lists the objects once no delete is in flight.
    async fn collected_object_seqs(store: &ObjectStoreLogStore) -> Vec<u64> {
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        object_seqs(store.io.as_ref()).await
    }

    fn unwrap_shared(error: &Error) -> &Error {
        match error {
            Error::ObjectStoreWal { source, .. } => source,
            other => panic!("expected a shared error, actual {other:?}"),
        }
    }

    fn assert_stopped(error: &Error) {
        assert!(
            matches!(error, Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
    }

    /// Waits for the next operation that `ParkedIo` parked.
    async fn next_parked_operation(
        parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) -> (u64, oneshot::Sender<bool>) {
        timeout(WAIT, parked.recv()).await.unwrap().unwrap()
    }

    /// Waits for `count` parked operations and returns their releases by
    /// sequence.
    async fn parked_operations(
        parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        count: usize,
    ) -> HashMap<u64, oneshot::Sender<bool>> {
        let mut releases = HashMap::new();
        for _ in 0..count {
            let (object_seq, release) = next_parked_operation(parked).await;
            releases.insert(object_seq, release);
        }
        releases
    }

    async fn open_over(
        io: Arc<dyn WalObjectIo>,
        config: &ObjectStoreWalConfig,
    ) -> Arc<ObjectStoreLogStore> {
        ObjectStoreLogStore::open(io, config, PREFIX.to_string())
            .await
            .unwrap()
    }

    async fn open_parking_creates(
        object_store: ObjectStore,
        config: &ObjectStoreWalConfig,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<ParkedIo>,
        mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) {
        let (io, parked) = ParkedIo::parking_creates(object_store);
        let store = open_over(io.clone(), config).await;
        // The start object of the open is created without parking.
        io.creates_parked.store(true, Ordering::SeqCst);
        (store, io, parked)
    }

    /// Round-trips a command through the actor, so an assertion that
    /// something did not happen runs after the actor handled every command
    /// sent before. The open batch must be empty, as under the eager config.
    async fn round_trip_actor(store: &ObjectStoreLogStore) {
        store.seal_open_batch().await.unwrap();
    }

    /// An in-memory object store behind the retry layer object storage has
    /// in production, whose writes and reads fail with the error kind their
    /// switch holds, recording when each refused write started. The retry
    /// layer reports such an error as persistent, as it reports a 403 of S3.
    struct RefusingStore {
        store: ObjectStore,
        writes: Arc<Mutex<Option<ErrorKind>>>,
        reads: Arc<Mutex<Option<ErrorKind>>>,
        refused_writes: Arc<Mutex<Vec<Instant>>>,
    }

    impl RefusingStore {
        fn new() -> Self {
            let writes = Arc::new(Mutex::new(None::<ErrorKind>));
            let reads = Arc::new(Mutex::new(None::<ErrorKind>));
            let refused_writes = Arc::new(Mutex::new(Vec::new()));
            let (refusing_writes, refused_reads) = (writes.clone(), reads.clone());
            let refused_at = refused_writes.clone();
            let mock = MockLayerBuilder::default()
                .writer_factory(Arc::new(move |_, _, inner| {
                    match *refusing_writes.lock().unwrap() {
                        Some(kind) => {
                            refused_at.lock().unwrap().push(Instant::now());
                            Box::new(RefusedWriter(kind))
                        }
                        None => inner,
                    }
                }))
                .reader_factory(Arc::new(move |_, _, inner| {
                    match *refused_reads.lock().unwrap() {
                        Some(kind) => Box::new(RefusedReader(kind)),
                        None => inner,
                    }
                }))
                .build()
                .unwrap();
            Self {
                store: with_retry_layers(memory_store().layer(mock)),
                writes,
                reads,
                refused_writes,
            }
        }

        fn refuse_writes(&self, kind: Option<ErrorKind>) {
            *self.writes.lock().unwrap() = kind;
        }

        fn refuse_reads(&self, kind: Option<ErrorKind>) {
            *self.reads.lock().unwrap() = kind;
        }
    }

    struct RefusedWriter(ErrorKind);

    impl oio::Write for RefusedWriter {
        async fn write(&mut self, _buffer: mock::Buffer) -> mock::Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            Err(mock::Error::new(self.0, "refused by the object store"))
        }

        async fn abort(&mut self) -> mock::Result<()> {
            Ok(())
        }
    }

    struct RefusedReader(ErrorKind);

    impl oio::Read for RefusedReader {
        async fn open(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Err(mock::Error::new(self.0, "refused by the object store"))
        }

        async fn read(
            &self,
            _range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, mock::Buffer)> {
            Err(mock::Error::new(self.0, "refused by the object store"))
        }
    }

    /// Asserts that an append was rejected because the object store refuses
    /// the backlog with `kind`: the error is retryable and carries the
    /// refusal.
    fn assert_rejected(error: &Error, kind: ErrorKind) {
        assert!(
            matches!(error, Error::ObjectStoreWalFailing { source, .. }
                if matches!(&**source, Error::WalObjectStore { error, .. } if error.kind() == kind)),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    }

    fn failing_stores() -> i64 {
        METRIC_OBJECT_STORE_WAL_FAILING.get()
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
            ObjectStoreWalConfig {
                max_unpersisted_bytes: ReadableSize(0),
                ..config(Duration::from_secs(1), 1)
            },
            ObjectStoreWalConfig {
                max_unpersisted_age: Duration::ZERO,
                ..config(Duration::from_secs(1), 1)
            },
        ] {
            let error = ObjectStoreLogStore::try_new(memory_store(), &config, 1, 2)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, Error::InvalidWalObjectStore { .. }),
                "unexpected error for {config:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_store_recovers_only_its_node_and_generation() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let config = ObjectStoreWalConfig::default();
        let identities = [(1, 2), (3, 2), (1, 4)];
        for (index, (node_id, generation)) in identities.iter().enumerate() {
            let io = ObjectStoreIo::new(
                object_store.clone(),
                config.node_prefix(*node_id, *generation),
            )
            .unwrap();
            put_fixture(
                &io,
                0,
                &[Record {
                    region_id: region(1),
                    entry_id: index as u64 + 1,
                    payload: Bytes::from_static(b"entry"),
                }],
            )
            .await;
        }
        // A root-level object must not be part of any node's recovery.
        let root_io = ObjectStoreIo::new(object_store.clone(), &config.prefix).unwrap();
        root_io
            .put_if_absent(0, Bytes::from_static(b"invalid"))
            .await
            .unwrap();
        for (index, (node_id, generation)) in identities.iter().enumerate() {
            let store =
                ObjectStoreLogStore::try_new(object_store.clone(), &config, *node_id, *generation)
                    .await
                    .unwrap();
            for (other_index, (other_node, other_generation)) in identities.iter().enumerate() {
                let provider = Provider::object_store_provider(
                    region(1),
                    config.node_prefix(*other_node, *other_generation),
                );
                if index == other_index {
                    assert_eq!(store.latest_entry_id(&provider).unwrap(), index as u64 + 1);
                } else {
                    assert!(matches!(
                        store.latest_entry_id(&provider),
                        Err(Error::MismatchedWalPrefix { .. })
                    ));
                }
            }
            let root_provider = Provider::object_store_provider(region(1), config.prefix.clone());
            assert!(matches!(
                store.latest_entry_id(&root_provider),
                Err(Error::MismatchedWalPrefix { .. })
            ));
            store.stop().await.unwrap();
        }
    }

    /// Rebuilds the catalog by decoding whole objects as a recovery oracle.
    async fn recover_by_decoding(io: &dyn WalObjectIo) -> Result<Recovered> {
        let mut objects = Vec::new();
        for object in io.list().await? {
            let bytes = io.get(object.object_seq).await?;
            let decoded = decode_object(&bytes)
                .and_then(|decoded| {
                    ensure!(
                        decoded.header.object_seq == object.object_seq,
                        CorruptedWalObjectSnafu {
                            reason: format!(
                                "header sequence {} does not match key sequence {}",
                                decoded.header.object_seq, object.object_seq
                            ),
                        }
                    );
                    Ok(decoded)
                })
                .with_context(|_| InvalidWalObjectSnafu {
                    path: object.path.clone(),
                })?;
            objects.push(FetchedObject {
                object,
                header: decoded.header,
                footer: decoded.footer,
            });
        }
        finish_recovery(FetchedObjects {
            objects,
            vanished: Vec::new(),
        })
    }

    fn assert_same_recovery(expected: &Recovered, actual: &Recovered) {
        assert_eq!(
            catalog_contents(&expected.catalog),
            catalog_contents(&actual.catalog)
        );
        assert_eq!(expected.next_object_seq, actual.next_object_seq);
        assert_eq!(expected.durable_entry_ids, actual.durable_entry_ids);
        assert_eq!(expected.tip, actual.tip);
        assert_eq!(expected.max_epoch, actual.max_epoch);
        assert_eq!(expected.reclaim_boundary, actual.reclaim_boundary);
        assert_eq!(expected.unindexed, actual.unindexed);
        assert_eq!(expected.released_entry_ids, actual.released_entry_ids);
    }

    fn catalog_contents(catalog: &ObjectCatalog) -> Vec<(u64, Vec<FooterEntry>)> {
        catalog
            .objects_in_order()
            .map(|(object_seq, footer)| (object_seq, footer.to_vec()))
            .collect()
    }

    async fn populate(object_store: &ObjectStore, objects: usize, regions: u32) {
        for object in 0..objects {
            let records = (1..=regions)
                .filter(|number| !(object + *number as usize).is_multiple_of(3))
                .map(|number| Record {
                    region_id: region(number),
                    entry_id: id(object as u64, 1),
                    payload: Bytes::from(format!("o{object}-r{number}")),
                })
                .collect::<Vec<_>>();
            put_records(object_store, object as u64, &records).await;
        }
    }

    async fn put_records(object_store: &ObjectStore, object_seq: u64, records: &[Record]) {
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        put_fixture(&io, object_seq, records).await;
    }

    /// The header of a fixture object at `object_seq`, which extends the
    /// present object right below it under the epoch of that object, so the
    /// fixtures a test writes in sequence order form one chain.
    async fn fixture_header(io: &ObjectStoreIo, object_seq: u64) -> Header {
        let below = io
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.object_seq)
            .filter(|seq| *seq < object_seq)
            .max();
        let prev = match below {
            Some(seq) => Some(decode_header(&io.get(seq).await.unwrap()).unwrap()),
            None => None,
        };
        Header {
            object_seq,
            epoch: prev.as_ref().map_or(1, |prev| prev.epoch),
            prev: prev.map(|prev| ChainLink {
                object_seq: prev.object_seq,
                epoch: prev.epoch,
            }),
            reclaim_boundary: 0,
        }
    }

    async fn put_fixture(io: &ObjectStoreIo, object_seq: u64, records: &[Record]) {
        let header = fixture_header(io, object_seq).await;
        put_object_with_header(io, header, records).await;
    }

    async fn put_object_with_header(io: &ObjectStoreIo, header: Header, records: &[Record]) {
        let object_seq = header.object_seq;
        let encoded = encode_object(header, records).unwrap();
        io.put_if_absent(object_seq, encoded.bytes).await.unwrap();
    }

    /// Puts an object of another writer with `epoch` under `object_seq`. A
    /// store opened on an empty prefix writes epoch 1.
    async fn put_foreign(object_store: &ObjectStore, object_seq: u64, epoch: u64) {
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let header = Header {
            object_seq,
            epoch,
            prev: None,
            reclaim_boundary: 0,
        };
        put_object_with_header(&io, header, &[]).await;
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
            let error = ObjectStoreLogStore::try_new(object_store.clone(), &config, 1, 2)
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

        let recovered = recover(&io).await.unwrap();
        let expected = recover_by_decoding(&io).await.unwrap();

        assert_eq!(40, catalog_contents(&recovered.catalog).len());
        assert_same_recovery(&expected, &recovered);
        assert_eq!(40, recovered.next_object_seq);
        let durable = recovered.durable_entry_ids;
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
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_corrupted_trailer_version_and_footer() {
        let _serialized = SERIALIZED.lock().await;
        type Corrupt = fn(&mut Vec<u8>);
        let cases: &[(&str, Corrupt)] = &[
            ("truncated object", |bytes| {
                bytes.truncate(MIN_OBJECT_LEN - 1)
            }),
            ("invalid header magic", |bytes| bytes[0] ^= 1),
            ("invalid footer range", |bytes| {
                let start = bytes.len() - TRAILER_LEN;
                bytes[start..start + 8].copy_from_slice(&0u64.to_be_bytes());
            }),
            ("overflows the object", |bytes| {
                let start = bytes.len() - TRAILER_LEN;
                bytes[start..start + 8].copy_from_slice(&u64::MAX.to_be_bytes());
            }),
            ("invalid trailer magic", |bytes| {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }),
            ("unsupported format version 3", |bytes| {
                bytes[8..10].copy_from_slice(&3u16.to_be_bytes());
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
            corrupt_object(&object_store, &path, *corrupt).await;

            assert_recovery_rejects(&object_store, &path, reason).await;
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_header_sequence_mismatch() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let encoded = encode_object(
            fixture_header(&io, 0).await,
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
    #[tokio::test]
    async fn test_store_recovery_fetches_a_footer_longer_than_the_tail_window() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_wide_object(&object_store).await;
        put_object(&object_store, 1, region(1), &[id(1, 1)]).await;

        let (io, reads) = RecordingIo::over(object_store.clone());
        let objects = io.list().await.unwrap();
        let wide = &objects[0];
        let (trailer, _) = footer_of(&object_store.read(&wide.path).await.unwrap().to_vec());
        assert!(trailer.footer_len > RECOVERY_TAIL_WINDOW as u64);

        let recovered = recover(io.as_ref()).await.unwrap();
        let expected = recover_by_decoding(io.as_ref()).await.unwrap();
        assert_same_recovery(&expected, &recovered);
        assert_eq!(WIDE_REGIONS as usize, recovered.durable_entry_ids.len());

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
        assert_eq!(id(1, 1), latest(&store, region(1)));
        assert_eq!(1, latest(&store, region(WIDE_REGIONS)));
        store.stop().await.unwrap();
    }

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
        put_records(object_store, object_seq, &records).await;
    }

    /// Regions enough for a footer longer than the window recovery reads
    /// from the end of an object, so that recovery fetches the header, that
    /// window and the footer of such an object separately.
    const WIDE_REGIONS: u32 = (RECOVERY_TAIL_WINDOW / FOOTER_ENTRY_LEN + 100) as u32;

    /// Writes object 0 with a footer longer than the tail window and returns
    /// the offsets of the window and of the footer.
    async fn put_wide_object(object_store: &ObjectStore) -> (u64, u64) {
        let records = (1..=WIDE_REGIONS)
            .map(|number| Record {
                region_id: region(number),
                entry_id: 1,
                payload: Bytes::from_static(b"wide"),
            })
            .collect::<Vec<_>>();
        put_records(object_store, 0, &records).await;
        let bytes = object_store
            .read(&object_path(object_store, 0))
            .await
            .unwrap()
            .to_vec();
        let (trailer, _) = footer_of(&bytes);
        let tail_start = (bytes.len() - RECOVERY_TAIL_WINDOW) as u64;
        assert!(trailer.footer_offset < tail_start);
        (tail_start, trailer.footer_offset)
    }

    #[tokio::test]
    async fn test_store_resumes_sequence_and_durable_ids() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let recovered = recover(&io).await.unwrap();
        assert_eq!(0, recovered.next_object_seq);
        assert!(recovered.durable_entry_ids.is_empty());

        put_object(&object_store, 2, region(1), &[1, id(5, 7)]).await;
        put_object(&object_store, 4, region(2), &[8]).await;
        let recovered = recover(&io).await.unwrap();
        assert_eq!(6, recovered.next_object_seq);
        assert_eq!(
            HashMap::from([(region(1), id(5, 7)), (region(2), 8)]),
            recovered.durable_entry_ids
        );
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(id(5, 7), latest(&store, region(1)));
        assert_eq!(8, latest(&store, region(2)));
        assert_eq!(0, latest(&store, region(3)));

        // The sequence resumes above the object the largest id names, 6,
        // and the sequences an earlier instance can have handed out above the
        // newest listed object, so the start object of the store takes object
        // 14 and the first new id of every region is greater than every old
        // one.
        let response = append_acknowledged(
            &store,
            vec![entry(&store, region(1), "a"), entry(&store, region(2), "b")],
        )
        .await
        .unwrap();
        assert_eq!(
            HashMap::from([(region(1), id(15, 1)), (region(2), id(15, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![2, 4, 14, 15], object_seqs(store.io.as_ref()).await);
        assert_eq!(
            expected_entries(
                region(1),
                &[
                    (1, "e1"),
                    (id(5, 7), &format!("e{}", id(5, 7))),
                    (id(15, 1), "a")
                ]
            ),
            read_entries(&store, region(1), 0).await
        );
        store.stop().await.unwrap();

        // The restart starts above the newest object, 15, in the same way.
        let store = open(object_store, &eager()).await;
        assert_eq!(id(15, 1), latest(&store, region(1)));
        let response = append(&store, region(2), "b2").await.unwrap();
        assert_eq!(
            HashMap::from([(region(2), id(25, 1))]),
            response.last_entry_ids
        );
        assert_eq!(
            expected_entries(region(2), &[(8, "e8"), (id(15, 1), "b"), (id(25, 1), "b2")]),
            read_entries(&store, region(2), 0).await
        );
        store.stop().await.unwrap();
    }

    /// Returns the message of the missing history error the check of
    /// `region_id` at `entry_id` fails with, or `None` when it passes.
    async fn missing_history(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Option<String> {
        match store
            .check_region_history(&provider(region_id), entry_id)
            .await
        {
            Ok(()) => None,
            Err(error @ Error::MissingWalHistory { .. }) => {
                assert_eq!(
                    common_error::status_code::StatusCode::InvalidArguments,
                    error.status_code()
                );
                Some(error.to_string())
            }
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }

    /// Copies every object under the prefix of `from` to the same key in `to`.
    async fn copy_objects(from: &ObjectStore, to: &ObjectStore) {
        let io = ObjectStoreIo::new(from.clone(), PREFIX).unwrap();
        for object in io.list().await.unwrap() {
            let bytes = from.read(&object.path).await.unwrap().to_vec();
            to.write(&object.path, bytes).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_history_is_missing_from_a_prefix_found_empty() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            storage_provider: "wal-store".to_string(),
            ..eager()
        };
        let object_store = memory_store();
        let store = open(object_store.clone(), &config).await;

        // A region written since the store opened holds its history through
        // its entries, not past them.
        let written = append(&store, region(2), "a").await.unwrap().last_entry_ids[&region(2)];
        assert_eq!(None, missing_history(&store, region(2), 0).await);
        assert_eq!(None, missing_history(&store, region(2), written).await);
        assert!(
            missing_history(&store, region(2), written + 1)
                .await
                .is_some()
        );

        // No other region has a history under a prefix that held no object,
        // not even one that never flushed. The store admitted an append, so
        // it keeps its objects.
        for entry_id in [0, id(0, 1), id(7, 1)] {
            let message = missing_history(&store, region(1), entry_id).await.unwrap();
            for expected in [
                &region(1).to_string(),
                PREFIX,
                // A memory store names itself by its address as its bucket.
                "storage provider \"wal-store\" (memory, bucket ",
                "root \"/\")",
                "the prefix held no object when the WAL opened",
                "copy every object under the prefix",
                "`[wal].accept_missing_history = true`",
            ] {
                assert!(message.contains(expected), "{message}");
            }
        }
        assert_eq!(vec![1], object_seqs(store.io.as_ref()).await);

        // A region created on the store has its history here without any
        // entry.
        store.start_region_history(&provider(region(3))).unwrap();
        assert_eq!(None, missing_history(&store, region(3), 0).await);

        // The provider must select the store's prefix.
        let foreign = Provider::object_store_provider(region(1), "wal/datanodes/9/epochs/2".into());
        assert!(matches!(
            store.check_region_history(&foreign, 0).await.unwrap_err(),
            Error::MismatchedWalPrefix { .. }
        ));
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_withdraws_its_start_object_when_it_refuses_a_region() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();

        // Every open of the prefix refuses the region, since each refusal
        // deletes the start object the open wrote and the next open finds
        // the prefix empty again.
        for _ in 0..2 {
            let store = open(object_store.clone(), &eager()).await;
            assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
            let indexed_objects = METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get();
            let indexed_bytes = METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get();
            assert!(missing_history(&store, region(1), 0).await.is_some());
            assert!(object_seqs(store.io.as_ref()).await.is_empty());
            // The catalog and the indexed gauges no longer hold the object.
            assert!(!store.catalog.read().unwrap().contains_object(0));
            assert_eq!(
                (indexed_objects - 1, indexed_bytes - object_len(&[]) as i64),
                (
                    METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get(),
                    METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get()
                )
            );

            // The store accepts nothing more, and refuses again without a
            // second delete.
            assert!(missing_history(&store, region(2), 0).await.is_some());
            let entry = Entry::Naive(NaiveEntry {
                provider: provider(region(2)),
                region_id: region(2),
                entry_id: 0,
                data: b"a".to_vec(),
            });
            for error in [
                store.append_batch(vec![entry]).await.unwrap_err(),
                store
                    .start_region_history(&provider(region(3)))
                    .unwrap_err(),
            ] {
                assert!(
                    matches!(unwrap_shared(&error), Error::WalStartWithdrawn { .. }),
                    "{error:?}"
                );
            }
            store.stop().await.unwrap();
        }

        // A store whose region history started keeps its start object.
        let store = open(object_store.clone(), &eager()).await;
        store.start_region_history(&provider(region(3))).unwrap();
        assert!(missing_history(&store, region(1), 0).await.is_some());
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        assert_eq!(None, missing_history(&store, region(3), 0).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_withdrawal_outlives_its_caller_and_holds_up_stop() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (io, mut parked) = ParkedIo::parking_creates(object_store.clone());
        let store = open_over(io.clone(), &eager()).await;
        io.deletes_parked.store(true, Ordering::SeqCst);

        // The refusal that withdraws the start object is cancelled once the
        // actor started its delete.
        let first = {
            let store = store.clone();
            tokio::spawn(async move { store.check_region_history(&provider(region(1)), 0).await })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        first.abort();

        // A later withdrawal request and stop reach the actor, and a
        // durability wait queued behind them is answered, so the actor
        // handled both; neither is answered while the delete is in flight.
        let (withdrawn_tx, mut withdrawn) = oneshot::channel();
        store
            .command_tx
            .send(Command::Withdraw {
                response: withdrawn_tx,
            })
            .await
            .unwrap();
        store.raise_stopped_flag();
        let (stopped_tx, mut stopped) = oneshot::channel();
        store
            .command_tx
            .send(Command::Stop {
                response: stopped_tx,
            })
            .await
            .unwrap();
        let (answered_tx, answered) = oneshot::channel();
        store
            .command_tx
            .send(Command::WaitDurable {
                region_id: region(1),
                kind: WaitKind::Barrier,
                entry_id: 1,
                response: answered_tx,
            })
            .await
            .unwrap();
        timeout(WAIT, answered).await.unwrap().unwrap().unwrap_err();
        assert!(withdrawn.try_recv().is_err());
        assert!(stopped.try_recv().is_err());

        release.send(true).unwrap();
        timeout(WAIT, withdrawn).await.unwrap().unwrap();
        timeout(WAIT, stopped).await.unwrap().unwrap().unwrap();
        assert!(object_seqs(io.as_ref()).await.is_empty());
        assert!(missing_history(&store, region(2), 0).await.is_some());

        // The next open finds the prefix empty and refuses the region again.
        let store = open(object_store, &eager()).await;
        assert!(missing_history(&store, region(1), 0).await.is_some());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_withdrawal_keeps_the_chain_of_a_successor() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (io, mut parked) = ParkedIo::parking_creates(object_store.clone());
        let store = open_over(io.clone(), &eager()).await;
        io.deletes_parked.store(true, Ordering::SeqCst);

        // While the withdrawal of start object 0 waits on its delete, a
        // successor opens the prefix, links its start object 9, above the
        // sequences the first store can have handed out, to object 0 and
        // stops before any append.
        let refused = {
            let store = store.clone();
            tokio::spawn(async move { store.check_region_history(&provider(region(1)), 0).await })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        let successor = open(object_store.clone(), &eager()).await;
        assert_eq!(vec![0, 9], object_seqs(successor.io.as_ref()).await);
        successor.stop().await.unwrap();
        release.send(true).unwrap();
        timeout(WAIT, refused).await.unwrap().unwrap().unwrap_err();
        store.stop().await.unwrap();

        // Object 9 reclaims the deleted object 0, so its chain is complete.
        let store = open(object_store, &eager()).await;
        assert_eq!(vec![9, 18], object_seqs(store.io.as_ref()).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_keeps_indexing_a_start_object_whose_withdrawal_failed() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = ParkedIo::parking_creates(memory_store());
        let store = open_over(io.clone(), &eager()).await;
        io.deletes_parked.store(true, Ordering::SeqCst);
        let indexed_objects = METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get();
        let indexed_bytes = METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get();

        let refused = {
            let store = store.clone();
            tokio::spawn(async move { store.check_region_history(&provider(region(1)), 0).await })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        release.send(false).unwrap();
        assert!(matches!(
            timeout(WAIT, refused).await.unwrap().unwrap().unwrap_err(),
            Error::MissingWalHistory { .. }
        ));

        // The object is still there, so the catalog and the indexed gauges
        // keep it.
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert!(store.catalog.read().unwrap().contains_object(0));
        assert_eq!(
            (indexed_objects, indexed_bytes),
            (
                METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get(),
                METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get()
            )
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_history_is_missing_above_the_newest_object_holding_entries() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        // The start object 0, then objects 1 and 2 of region 1.
        let store = open(object_store.clone(), &eager()).await;
        append(&store, region(1), "a").await.unwrap();
        append(&store, region(1), "b").await.unwrap();
        store.stop().await.unwrap();

        // Every restart writes one more start object, but the newest object
        // holding entries stays object 2, so a refused open does not let
        // the next one pass a watermark of object 3.
        for start_object in [11, 20] {
            let store = open(object_store.clone(), &eager()).await;
            assert!(
                store.storage.starts_with("the default storage (memory, ")
                    && store.storage.ends_with("root \"/\")"),
                "{}",
                store.storage
            );
            for region_id in [region(1), region(2)] {
                for entry_id in [0, id(1, 1), id(2, 1), id(2, POSITION_LIMIT - 1)] {
                    assert_eq!(None, missing_history(&store, region_id, entry_id).await);
                }
                for entry_id in [id(3, 1), id(9, 1)] {
                    let message = missing_history(&store, region_id, entry_id).await.unwrap();
                    let reason = format!(
                        "its watermark, entry {entry_id}, names object {}, but the prefix held entries only in objects below 3 when the WAL opened",
                        entry_id >> 20
                    );
                    assert!(message.contains(&reason), "{message}");
                }
            }
            assert_eq!(
                Some(&start_object),
                object_seqs(store.io.as_ref()).await.last()
            );
            store.stop().await.unwrap();
        }

        // An entry written since then lies above the objects found, and the
        // catalog holds it.
        let store = open(object_store, &eager()).await;
        let written = append(&store, region(2), "c").await.unwrap().last_entry_ids[&region(2)];
        assert_eq!(id(30, 1), written);
        assert_eq!(None, missing_history(&store, region(2), written).await);
        assert!(missing_history(&store, region(1), written).await.is_some());
        store.stop().await.unwrap();

        // A prefix that holds only start objects holds no entry.
        let object_store = memory_store();
        open(object_store.clone(), &eager())
            .await
            .stop()
            .await
            .unwrap();
        let store = open(object_store, &eager()).await;
        assert_eq!(None, missing_history(&store, region(1), 0).await);
        let message = missing_history(&store, region(1), id(0, 1)).await.unwrap();
        assert!(
            message.contains("names object 0, but the prefix held no entry when the WAL opened"),
            "{message}"
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_history_survives_collection_down_to_the_newest_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        // Region 1 flushes object 1; object 2 of region 2 carries the boundary
        // past it, so only object 2 is left once region 2 flushes it too.
        let flushed_1 = append(&store, region(1), "a").await.unwrap().last_entry_ids[&region(1)];
        store
            .obsolete(&provider(region(1)), region(1), flushed_1)
            .await
            .unwrap();
        let flushed_2 = append(&store, region(2), "b").await.unwrap().last_entry_ids[&region(2)];
        store
            .obsolete(&provider(region(2)), region(2), flushed_2)
            .await
            .unwrap();
        assert_eq!(vec![2], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        // The object region 1 flushed is gone, but the newest object stays
        // and keeps the next sequence above it.
        let store = open(object_store, &eager()).await;
        assert_eq!(0, latest(&store, region(1)));
        assert_eq!(None, missing_history(&store, region(1), flushed_1).await);
        assert_eq!(None, missing_history(&store, region(2), flushed_2).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_history_survives_collection_down_to_a_release_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        // Region 1 flushes object 1; the release of region 2 seals object 2,
        // which holds only its release record and carries the boundary past
        // object 1.
        let flushed = append(&store, region(1), "a").await.unwrap().last_entry_ids[&region(1)];
        store
            .obsolete(&provider(region(1)), region(1), flushed)
            .await
            .unwrap();
        store
            .release_region(&provider(region(2)), region(2))
            .await
            .unwrap();
        assert_eq!(vec![2], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        // The object holding the release record bounds the history.
        let store = open(object_store, &eager()).await;
        assert_eq!(None, missing_history(&store, region(1), flushed).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_keeps_its_objects_after_a_release_when_it_refuses_a_region() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        // The release seals object 1, which collects the start object.
        store
            .release_region(&provider(region(2)), region(2))
            .await
            .unwrap();
        assert_eq!(vec![1], collected_object_seqs(&store).await);

        // The refusal leaves the object holding the release record, and the
        // store keeps accepting appends.
        assert!(missing_history(&store, region(1), 0).await.is_some());
        assert_eq!(vec![1], object_seqs(store.io.as_ref()).await);
        append(&store, region(3), "a").await.unwrap();
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_history_moves_with_a_copy_of_the_objects() {
        let _serialized = SERIALIZED.lock().await;
        let old_location = memory_store();
        let store = open(old_location.clone(), &eager()).await;
        let flushed = append(&store, region(1), "a").await.unwrap().last_entry_ids[&region(1)];
        store
            .obsolete(&provider(region(1)), region(1), flushed)
            .await
            .unwrap();
        let unflushed = append(&store, region(1), "b").await.unwrap().last_entry_ids[&region(1)];
        assert_eq!(vec![2], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        // Another location without the objects does not hold the history.
        let store = open(memory_store(), &eager()).await;
        assert!(missing_history(&store, region(1), flushed).await.is_some());
        store.stop().await.unwrap();

        // A copy of every object does, and replays the unflushed entry.
        let new_location = memory_store();
        copy_objects(&old_location, &new_location).await;
        let store = open(new_location, &eager()).await;
        assert_eq!(None, missing_history(&store, region(1), flushed).await);
        assert_eq!(
            expected_entries(region(1), &[(unflushed, "b")]),
            read_entries(&store, region(1), flushed + 1).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_accepts_a_missing_history_when_configured() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            accept_missing_history: true,
            ..eager()
        };
        let store = open(memory_store(), &config).await;
        let accepted = METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.get();
        store
            .check_region_history(&provider(region(1)), id(5, 1))
            .await
            .unwrap();
        assert_eq!(
            accepted + 1,
            METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.get()
        );

        // The accepted region's history starts in this store, so a second
        // open of it is not counted again, and neither is a history the
        // prefix holds.
        store
            .check_region_history(&provider(region(1)), id(5, 1))
            .await
            .unwrap();
        let written = append(&store, region(2), "a").await.unwrap().last_entry_ids[&region(2)];
        store
            .check_region_history(&provider(region(2)), written)
            .await
            .unwrap();
        assert_eq!(
            accepted + 1,
            METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.get()
        );

        // Once Mito records the accepted watermark, the region's next id is
        // above it: the object marking the raised floor takes object 6.
        store
            .obsolete(&provider(region(1)), region(1), id(5, 1))
            .await
            .unwrap();
        assert_eq!(
            HashMap::from([(region(1), id(7, 1))]),
            append(&store, region(1), "b").await.unwrap().last_entry_ids
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_rejects_exhausted_sequence() {
        let _serialized = SERIALIZED.lock().await;
        for (object_seq, entry_id) in [(u64::MAX, 1), (OBJECT_SEQ_LIMIT - 1, 1), (0, u64::MAX)] {
            let object_store = memory_store();
            put_object(&object_store, object_seq, region(1), &[entry_id]).await;
            let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::WalObjectSequenceExhausted { .. }),
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_conflicting_entry_ranges() {
        let _serialized = SERIALIZED.lock().await;
        for entries in [[1, 2], [2, 3]] {
            let object_store = memory_store();
            put_object(&object_store, 0, region(1), &[1, 2]).await;
            put_object(&object_store, 1, region(1), &entries).await;
            let error = ObjectStoreLogStore::try_new(object_store.clone(), &eager(), 1, 2)
                .await
                .unwrap_err();
            assert_invalid_object(&error, &object_path(&object_store, 1), "entry");
        }
    }

    #[tokio::test]
    async fn test_store_rejects_foreign_providers() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        let other = Provider::object_store_provider(region_id, "other/prefix".to_string());
        let raft = Provider::raft_engine_provider(region_id.as_u64());
        for foreign in [&other, &raft] {
            let entry = Entry::Naive(NaiveEntry {
                provider: foreign.clone(),
                region_id,
                entry_id: 0,
                data: b"a1".to_vec(),
            });
            let errors = [
                store.append_batch(vec![entry]).await.unwrap_err(),
                store.latest_entry_id(foreign).unwrap_err(),
                store.read(foreign, 0).await.err().unwrap(),
                store.create_namespace(foreign).await.unwrap_err(),
                store.delete_namespace(foreign).await.unwrap_err(),
                store.obsolete(foreign, region_id, 1).await.unwrap_err(),
                store.obsolete_all(foreign, region_id).await.unwrap_err(),
                store.entry(Vec::new(), 1, region_id, foreign).unwrap_err(),
            ];
            for error in errors {
                if foreign == &raft {
                    assert!(matches!(error, Error::InvalidProvider { .. }), "{error:?}");
                } else {
                    assert!(
                        matches!(&error,
                        Error::MismatchedWalPrefix { expected, actual, .. }
                        if expected == PREFIX && actual == "other/prefix"),
                        "{error:?}"
                    );
                }
            }
        }
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_stop_is_idempotent() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let (first, second) = tokio::join!(store.stop(), store.stop());
        first.unwrap();
        second.unwrap();
        timeout(WAIT, store.command_tx.closed()).await.unwrap();
        store.stop().await.unwrap();
        assert!(store.stopped.load(Ordering::Acquire));
        // Even an empty append fails once stop began.
        assert_stopped(&store.append_batch(Vec::new()).await.unwrap_err());
    }

    /// Builds a store whose commands the test receives instead of an actor.
    fn store_without_actor() -> (ObjectStoreLogStore, mpsc::Receiver<Command>) {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let (append_tx, _) = mpsc::channel(APPEND_BUFFER);
        let store = ObjectStoreLogStore {
            prefix: PREFIX.to_string(),
            storage: String::new(),
            ack_mode: AckMode::Durable,
            on_corrupted_segment: CorruptedSegmentAction::Skip,
            accept_missing_history: false,
            found_empty: false,
            history_end: 0,
            started_regions: StartedRegions::default(),
            io: Arc::new(ObjectStoreIo::new(memory_store(), PREFIX).unwrap()),
            catalog: Arc::default(),
            obsolete_entry_ids: ObsoleteEntryIds::default(),
            wal_holes: WalHoles::default(),
            deleting: Arc::default(),
            read_prefetch_budget: Arc::new(Semaphore::new(READ_PREFETCH_BYTES)),
            terminal_error: Arc::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            last_create_failed: Arc::new(AtomicBool::new(false)),
            drain_requested: Arc::default(),
            command_tx,
            append_tx,
            admitted_appends: watch::channel(0).1,
            held_back_appends: watch::channel(0).1,
            creates_held: watch::channel(false).0,
            creates_fail: Arc::default(),
            next_create_fails_after_write: Arc::default(),
            parked_creates: watch::channel(0).1,
            durability_waits: watch::channel(0).1,
            failed_creates: watch::channel(0).1,
            failing_state: watch::channel(false).1,
        };
        (store, command_rx)
    }

    #[tokio::test]
    async fn test_store_stop_with_actor_gone() {
        let _serialized = SERIALIZED.lock().await;
        let (store, command_rx) = store_without_actor();
        drop(command_rx);
        store.stop().await.unwrap();
        store.stop().await.unwrap();
    }

    /// A wait the actor leaves unhandled when it exits, queued behind the
    /// indexing of its entry, answers from the catalog the actor left: Ok for
    /// an entry indexed by then, the stopped error for any other.
    #[tokio::test]
    async fn test_store_wait_left_unhandled_by_the_exiting_actor() {
        let _serialized = SERIALIZED.lock().await;
        let (store, mut command_rx) = store_without_actor();
        let store = Arc::new(store);
        let region_id = region(1);
        // Entry 1 is not durable when the wait is sent, and object 1, which
        // holds it, is indexed before the actor exits; entry 2 never becomes
        // durable.
        for (entry_id, durable) in [(id(1, 1), true), (id(2, 1), false)] {
            let wait = spawn_wait(&store, region_id, entry_id, WaitKind::Write);
            let command = timeout(WAIT, command_rx.recv()).await.unwrap().unwrap();
            assert!(matches!(command, Command::WaitDurable { .. }));
            if durable {
                store
                    .catalog
                    .write()
                    .unwrap()
                    .insert_object(
                        1,
                        vec![FooterEntry {
                            region_id,
                            min_entry_id: id(1, 1),
                            max_entry_id: id(1, 1),
                            entry_count: 1,
                            segment_offset: HEADER_LEN as u64,
                            segment_len: 1,
                            segment_crc32: 0,
                        }],
                    )
                    .unwrap();
            }
            drop(command);
            let answered = timeout(WAIT, wait).await.unwrap().unwrap();
            assert_eq!(durable, answered.is_ok(), "{answered:?}");
        }
        // A wait sent after the actor exited answers the same.
        drop(command_rx);
        timeout(
            WAIT,
            store.wait_write_durable(&provider(region_id), id(1, 1)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_stopped(
            &timeout(
                WAIT,
                store.wait_write_durable(&provider(region_id), id(2, 1)),
            )
            .await
            .unwrap()
            .unwrap_err(),
        );
    }

    #[tokio::test]
    async fn test_store_actor_exits_when_the_store_is_dropped() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let io = Arc::downgrade(&store.io);
        let stopped = Arc::downgrade(&store.stopped);
        drop(store);
        timeout(WAIT, async {
            while stopped.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(io.upgrade().is_none());
    }

    #[tokio::test]
    async fn test_store_retains_first_terminal_error() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let first = set_terminal(
            &store.terminal_error,
            CorruptedWalObjectSnafu { reason: "first" }.build(),
        );
        let second = set_terminal(
            &store.terminal_error,
            CorruptedWalObjectSnafu { reason: "second" }.build(),
        );
        assert!(Arc::ptr_eq(&first, &second));
        let region_id = region(1);
        let provider = provider(region_id);
        let errors = [
            store.append_batch(Vec::new()).await.unwrap_err(),
            store.latest_entry_id(&provider).unwrap_err(),
            store.read(&provider, 0).await.err().unwrap(),
            store.create_namespace(&provider).await.unwrap_err(),
            store.delete_namespace(&provider).await.unwrap_err(),
            store.list_namespaces().await.unwrap_err(),
            store.obsolete(&provider, region_id, 1).await.unwrap_err(),
            store.obsolete_all(&provider, region_id).await.unwrap_err(),
            store
                .entry(Vec::new(), 1, region_id, &provider)
                .unwrap_err(),
        ];
        for error in errors {
            match error {
                Error::ObjectStoreWal { source, .. } => assert!(Arc::ptr_eq(&first, &source)),
                error => panic!("{error:?}"),
            }
        }
        assert!(store.obsolete_entry_ids.lock().unwrap().is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_bounds_concurrency_and_orders_by_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let depth = RECOVERY_CONCURRENCY as u64;
        populate(&object_store, 2 * RECOVERY_CONCURRENCY, 3).await;
        let (io, mut parked) = ParkedIo::over(object_store);
        let objects = io.list().await.unwrap();

        let mut fetch = {
            let io = io.clone();
            tokio::spawn(async move {
                fetch_footers(io.as_ref(), objects, RECOVERY_CONCURRENCY)
                    .await
                    .unwrap()
                    .objects
                    .into_iter()
                    .map(|fetched| fetched.object.object_seq)
                    .collect::<Vec<_>>()
            })
        };
        let mut wave = Vec::new();
        for _ in 0..RECOVERY_CONCURRENCY {
            wave.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        assert_eq!(
            (0..depth).collect::<Vec<_>>(),
            wave.iter()
                .map(|(object_seq, _)| *object_seq)
                .collect::<Vec<_>>()
        );
        // No further fetch can start while a full wave is parked.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(parked.try_recv().is_err());

        // Complete the initial wave in reverse order, parking each replacement.
        let mut replacements = Vec::new();
        for (expected_next, (_, release)) in (depth..2 * depth).zip(wave.into_iter().rev()) {
            release.send(true).unwrap();
            let (object_seq, release) = timeout(WAIT, parked.recv()).await.unwrap().unwrap();
            assert_eq!(expected_next, object_seq);
            assert!(!fetch.is_finished());
            replacements.push(release);
        }
        for release in replacements {
            release.send(true).unwrap();
        }
        assert_eq!(
            (0..2 * depth).collect::<Vec<u64>>(),
            timeout(WAIT, &mut fetch).await.unwrap().unwrap()
        );
        assert_eq!(
            RECOVERY_CONCURRENCY,
            io.max_in_flight.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_store_recovery_abandons_pending_fetches_on_failure() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let objects = 2 * RECOVERY_CONCURRENCY as u64;
        populate(&object_store, objects as usize, 3).await;
        let (io, mut parked) = ParkedIo::over(object_store.clone());
        let fetch = {
            let io = io.clone();
            tokio::spawn(async move {
                ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
            })
        };
        let mut wave = Vec::new();
        for _ in 0..RECOVERY_CONCURRENCY {
            wave.push(timeout(WAIT, parked.recv()).await.unwrap().unwrap());
        }
        let (failed_seq, release) = wave.pop().unwrap();
        release.send(false).unwrap();
        let error = timeout(WAIT, fetch).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
            if path == &io.object_path(failed_seq))
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        for (_, release) in wave {
            assert!(release.is_closed());
        }
        assert!(parked.try_recv().is_err());
        let store = open(object_store, &eager()).await;
        assert_eq!(id(objects - 1, 1), latest(&store, region(1)));
        // The object that started the epoch of the store lies above the
        // populated objects and the sequences their writer can have handed
        // out.
        assert_eq!(
            objects + UNLISTED_SEQUENCES + 1,
            store.catalog.read().unwrap().next_object_seq().unwrap()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_reads_only_header_and_tail_of_large_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_records(
            &object_store,
            0,
            &[Record {
                region_id: region(1),
                entry_id: 1,
                payload: Bytes::from(vec![0; RECOVERY_TAIL_WINDOW * 2]),
            }],
        )
        .await;
        let path = object_path(&object_store, 0);
        corrupt_object(&object_store, &path, |bytes| bytes[HEADER_LEN + 20] ^= 1).await;
        let (io, reads) = RecordingIo::over(object_store);
        let object = io.list().await.unwrap().remove(0);
        let store = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap();
        assert_eq!(1, latest(&store, region(1)));
        let mut reads = reads.lock().unwrap().clone();
        reads.sort_unstable();
        assert_eq!(
            vec![
                (0, 0, HEADER_LEN as u64),
                (
                    0,
                    object.size - RECOVERY_TAIL_WINDOW as u64,
                    RECOVERY_TAIL_WINDOW as u64
                )
            ],
            reads
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_short_read() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_object(&object_store, 0, region(1), &[1]).await;
        let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        let mut object = io.list().await.unwrap().remove(0);
        object.size += 1;
        let error = fetch_footer(&io, &object).await.unwrap_err();
        assert_invalid_object(&error, &object.path, "fewer bytes than the listed");
    }

    async fn read_entries(
        store: &ObjectStoreLogStore,
        region_id: RegionId,
        start: EntryId,
    ) -> Vec<Entry> {
        store
            .read(&provider(region_id), start)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    fn expected_entries(region_id: RegionId, entries: &[(EntryId, &str)]) -> Vec<Entry> {
        entries
            .iter()
            .map(|(entry_id, data)| {
                Entry::Naive(NaiveEntry {
                    provider: provider(region_id),
                    region_id,
                    entry_id: *entry_id,
                    data: data.as_bytes().to_vec(),
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn test_store_reads_only_region_segments_in_entry_order() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let mut expected_ranges = HashMap::<_, Vec<_>>::new();
        for (seq, first) in [(1, 10), (3, 30), (7, 70)] {
            let records = [region(2), region(1)]
                .into_iter()
                .flat_map(|region_id| {
                    (first..first + 3).map(move |entry_id| Record {
                        region_id,
                        entry_id,
                        payload: Bytes::from(format!("r{}-e{entry_id}", region_id.region_number())),
                    })
                })
                .collect::<Vec<_>>();
            put_records(&object_store, seq, &records).await;
            let bytes = object_store
                .read(&object_path(&object_store, seq))
                .await
                .unwrap()
                .to_vec();
            for entry in footer_of(&bytes).1 {
                expected_ranges.entry(entry.region_id).or_default().push((
                    seq,
                    entry.segment_offset,
                    entry.segment_len,
                ));
            }
        }
        let (io, reads) = RecordingIo::over(object_store);
        let store = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap();
        for number in [1, 2] {
            reads.lock().unwrap().clear();
            let entries = read_entries(&store, region(number), 11).await;
            let ids = [11, 12, 30, 31, 32, 70, 71, 72];
            let expected = ids
                .into_iter()
                .map(|entry_id| {
                    Entry::Naive(NaiveEntry {
                        provider: provider(region(number)),
                        region_id: region(number),
                        entry_id,
                        data: format!("r{number}-e{entry_id}").into_bytes(),
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(expected, entries);
            // Segments are fetched concurrently, so in no particular order.
            let mut reads = reads.lock().unwrap().clone();
            reads.sort_unstable();
            assert_eq!(expected_ranges[&region(number)], reads);
        }
        reads.lock().unwrap().clear();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&store, region(1), 73).await
        );
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&store, region(3), 0).await
        );
        assert!(reads.lock().unwrap().is_empty());
        assert_eq!(
            vec![provider(region(1)), provider(region(2))],
            store.list_namespaces().await.unwrap()
        );
        store.create_namespace(&provider(region(3))).await.unwrap();
        store.delete_namespace(&provider(region(1))).await.unwrap();
        assert_eq!(
            vec![provider(region(1)), provider(region(2))],
            store.list_namespaces().await.unwrap()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_obsolete_hides_entries_from_read_only() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_object(&object_store, 0, region(1), &[10, 11, 12]).await;
        put_object(&object_store, 1, region(1), &[20]).await;
        put_object(&object_store, 2, region(2), &[10]).await;
        let store = open(object_store.clone(), &eager()).await;
        let p = provider(region(1));
        let all = expected_entries(
            region(1),
            &[(10, "e10"), (11, "e11"), (12, "e12"), (20, "e20")],
        );
        store.obsolete(&p, region(1), 9).await.unwrap();
        assert_eq!(all, read_entries(&store, region(1), 0).await);
        store.obsolete(&p, region(1), 11).await.unwrap();
        let remaining = expected_entries(region(1), &[(12, "e12"), (20, "e20")]);
        assert_eq!(remaining, read_entries(&store, region(1), 0).await);
        assert_eq!(
            expected_entries(region(1), &[(20, "e20")]),
            read_entries(&store, region(1), 20).await
        );
        assert_eq!(20, latest(&store, region(1)));
        store.obsolete(&p, region(1), 10).await.unwrap();
        assert_eq!(remaining, read_entries(&store, region(1), 0).await);
        store.stop().await.unwrap();

        // A watermark is kept in memory only.
        let reopened = open(object_store.clone(), &eager()).await;
        assert_eq!(all, read_entries(&reopened, region(1), 0).await);
        reopened.obsolete(&p, region(1), 20).await.unwrap();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&reopened, region(1), 0).await
        );
        assert_eq!(20, latest(&reopened, region(1)));

        // A release in full survives the restart, and a later watermark does
        // not move it down.
        reopened.obsolete_all(&p, region(1)).await.unwrap();
        reopened.obsolete(&p, region(1), 0).await.unwrap();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&reopened, region(1), 0).await
        );
        assert_eq!(20, latest(&reopened, region(1)));
        reopened.stop().await.unwrap();
        let reopened = open(object_store, &eager()).await;
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&reopened, region(1), 0).await
        );
        reopened.obsolete(&p, region(1), 0).await.unwrap();
        assert_eq!(
            Vec::<Entry>::new(),
            read_entries(&reopened, region(1), 0).await
        );
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&reopened, region(2), 0).await
        );
        reopened.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_entry_and_watermarks_reject_another_region() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let p = provider(region(2));
        let errors = [
            store
                .entry(b"payload".to_vec(), 7, region(1), &p)
                .unwrap_err(),
            store.obsolete(&p, region(1), 7).await.unwrap_err(),
            store.obsolete_all(&p, region(1)).await.unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(&error, Error::MismatchedWalRegion { region_id, reason, .. }
                if *region_id == region(1) && reason == &format!("provider belongs to region {}", region(2)))
            );
            assert_eq!(
                common_error::status_code::StatusCode::InvalidArguments,
                error.status_code()
            );
        }
        assert!(store.obsolete_entry_ids.lock().unwrap().is_empty());
        assert_eq!(
            expected_entries(region(2), &[(7, "payload")]),
            vec![store.entry(b"payload".to_vec(), 7, region(2), &p).unwrap()]
        );
        assert!(store.list_namespaces().await.unwrap().is_empty());
        store.stop().await.unwrap();
    }

    /// The range reads of object `object_seq` among `reads`, in the order
    /// they were issued.
    fn range_reads_of(reads: &RangeReads, object_seq: u64) -> Vec<(u64, u64, u64)> {
        reads
            .lock()
            .unwrap()
            .iter()
            .filter(|read| read.0 == object_seq)
            .copied()
            .collect()
    }

    /// Writes object 0 with one entry of region 1 and one of region 2 and
    /// object 1 with an entry of region 2, then flips a byte in the segment of
    /// region 2 in object 0. Returns the key of object 0 and its footer.
    async fn write_corrupted_segment(object_store: &ObjectStore) -> (String, Vec<FooterEntry>) {
        put_records(
            object_store,
            0,
            &[
                Record {
                    region_id: region(1),
                    entry_id: 1,
                    payload: Bytes::from_static(b"a1"),
                },
                Record {
                    region_id: region(2),
                    entry_id: 1,
                    payload: Bytes::from_static(b"b1"),
                },
            ],
        )
        .await;
        put_object(object_store, 1, region(2), &[10]).await;
        let path = object_path(object_store, 0);
        let bytes = object_store.read(&path).await.unwrap().to_vec();
        let footer = footer_of(&bytes).1;
        let corrupt = &footer[1];
        assert_eq!(region(2), corrupt.region_id);
        corrupt_object(object_store, &path, |bytes| {
            bytes[(corrupt.segment_offset + corrupt.segment_len - 1) as usize] ^= 1;
        })
        .await;
        (path, footer)
    }

    #[tokio::test]
    async fn test_store_skips_a_corrupted_segment_and_records_a_hole() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (path, footer) = write_corrupted_segment(&object_store).await;
        let (io, reads) = RecordingIo::over(object_store);
        let store = open_over(io, &eager()).await;
        assert_eq!(CorruptedSegmentAction::Skip, store.on_corrupted_segment);
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

        // Recovery indexes the object; a read of the other region and one of
        // the later object of the damaged region are unaffected.
        assert_eq!(
            expected_entries(region(1), &[(1, "a1")]),
            read_entries(&store, region(1), 0).await
        );
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 10).await
        );
        assert!(store.wal_holes(&provider(region(2))).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );

        // A read across the damaged segment fetches it twice, continues with
        // the next object and records the hole.
        reads.lock().unwrap().clear();
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 0).await
        );
        let corrupt = (0, footer[1].segment_offset, footer[1].segment_len);
        assert_eq!(vec![corrupt, corrupt], range_reads_of(&reads, 0));
        let hole = WalHole {
            path,
            object_seq: 0,
            min_entry_id: 1,
            max_entry_id: 1,
        };
        assert_eq!(
            vec![hole.clone()],
            store.wal_holes(&provider(region(2))).unwrap()
        );
        assert!(store.wal_holes(&provider(region(1))).unwrap().is_empty());
        assert_eq!(
            skipped + 1,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );

        // Every read across the segment skips it again; the hole is one.
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 0).await
        );
        assert_eq!(vec![hole], store.wal_holes(&provider(region(2))).unwrap());
        assert_eq!(
            skipped + 2,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_corrupted_segment_fails_the_read_that_decodes_it() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (path, footer) = write_corrupted_segment(&object_store).await;
        let (io, reads) = RecordingIo::over(object_store);
        let config = on_corruption(CorruptedSegmentAction::Fail, eager());
        let store = open_over(io, &config).await;
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();
        assert_eq!(1, latest(&store, region(1)));
        assert_eq!(10, latest(&store, region(2)));
        reads.lock().unwrap().clear();
        assert_eq!(
            expected_entries(region(1), &[(1, "a1")]),
            read_entries(&store, region(1), 0).await
        );
        assert_eq!(
            vec![(0, footer[0].segment_offset, footer[0].segment_len)],
            *reads.lock().unwrap()
        );
        assert_eq!(
            expected_entries(region(2), &[(10, "e10")]),
            read_entries(&store, region(2), 10).await
        );
        reads.lock().unwrap().clear();
        let error = store
            .read(&provider(region(2)), 0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_invalid_object(
            &error,
            &path,
            &format!("segment of region {} checksum mismatch", region(2)),
        );
        let corrupt = (0, footer[1].segment_offset, footer[1].segment_len);
        assert_eq!(vec![corrupt, corrupt], range_reads_of(&reads, 0));
        assert!(store.wal_holes(&provider(region(2))).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_reads_a_segment_whose_first_download_was_damaged() {
        let _serialized = SERIALIZED.lock().await;
        for on_corrupted_segment in [CorruptedSegmentAction::Skip, CorruptedSegmentAction::Fail] {
            let object_store = memory_store();
            put_object(&object_store, 0, region(1), &[1, 2]).await;
            let (io, reads) = RecordingIo::over(object_store);
            let config = on_corruption(on_corrupted_segment, eager());
            let store = open_over(io.clone(), &config).await;
            let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

            // The first download of the segment is damaged, the second is
            // not: the read sees the entries and records no hole.
            reads.lock().unwrap().clear();
            io.damage_next_range_read.store(true, Ordering::Relaxed);
            assert_eq!(
                expected_entries(region(1), &[(1, "e1"), (2, "e2")]),
                read_entries(&store, region(1), 0).await
            );
            assert_eq!(2, reads.lock().unwrap().len());
            assert!(store.wal_holes(&provider(region(1))).unwrap().is_empty());
            assert_eq!(
                skipped,
                METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
            );
            store.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_failed_segment_fetch_fails_the_read() {
        let _serialized = SERIALIZED.lock().await;
        for on_corrupted_segment in [CorruptedSegmentAction::Skip, CorruptedSegmentAction::Fail] {
            let object_store = memory_store();
            put_object(&object_store, 0, region(1), &[1]).await;
            let path = object_path(&object_store, 0);
            let (io, _) = RecordingIo::over(object_store.clone());
            let config = on_corruption(on_corrupted_segment, eager());
            let store = open_over(io, &config).await;
            let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

            object_store.delete(&path).await.unwrap();
            let error = store
                .read(&provider(region(1)), 0)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { path: actual, .. } if *actual == path),
                "unexpected error: {error:?}"
            );
            assert!(store.wal_holes(&provider(region(1))).unwrap().is_empty());
            assert_eq!(
                skipped,
                METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
            );
            store.stop().await.unwrap();
        }
    }

    /// The entries of region 1 that [`put_object_per_entry`] wrote to the
    /// objects `object_seqs`.
    fn object_entries(object_seqs: impl IntoIterator<Item = u64>) -> Vec<Entry> {
        object_seqs
            .into_iter()
            .map(|object_seq| {
                let entry_id = object_seq + 1;
                Entry::Naive(NaiveEntry {
                    provider: provider(region(1)),
                    region_id: region(1),
                    entry_id,
                    data: format!("e{entry_id}").into_bytes(),
                })
            })
            .collect()
    }

    /// Writes objects 0 to `objects - 1`, object `n` with entry `n + 1` of
    /// region 1.
    async fn put_object_per_entry(object_store: &ObjectStore, objects: u64) {
        for object_seq in 0..objects {
            put_object(object_store, object_seq, region(1), &[object_seq + 1]).await;
        }
    }

    /// Opens a store over [`put_object_per_entry`] objects, releasing every
    /// read of recovery; range reads park from then on.
    async fn open_parking_range_reads(
        objects: u64,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<ParkedIo>,
        mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) {
        let object_store = memory_store();
        put_object_per_entry(&object_store, objects).await;
        let (io, mut parked) = ParkedIo::over(object_store);
        let mut opening = {
            let io = io.clone();
            tokio::spawn(async move {
                ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
            })
        };
        let store = loop {
            tokio::select! {
                opened = &mut opening => break opened.unwrap().unwrap(),
                Some((_, release)) = parked.recv() => release.send(true).unwrap(),
            }
        };
        io.range_reads_parked.store(true, Ordering::SeqCst);
        io.max_in_flight.store(0, Ordering::SeqCst);
        (store, io, parked)
    }

    /// Polls until `settled` holds.
    async fn wait_until(settled: impl Fn() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !settled() {
            assert!(Instant::now() < deadline, "the condition never held");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn assert_nothing_parks(
        parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
    ) {
        assert!(
            timeout(Duration::from_millis(100), parked.recv())
                .await
                .is_err()
        );
    }

    async fn next_batch(stream: &mut SendableEntryStream<'static, Entry, Error>) -> Vec<Entry> {
        timeout(WAIT, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn test_store_read_prefetches_up_to_the_depth_in_order() {
        let _serialized = SERIALIZED.lock().await;
        let depth = READ_PREFETCH_DEPTH as u64;
        let objects = depth + 4;
        let (store, io, mut parked) = open_parking_range_reads(objects).await;
        let reads = METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count();
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();

        // The first poll starts the fetches of the first `depth` objects.
        assert!(stream.next().now_or_never().is_none());
        let mut wave = parked_operations(&mut parked, READ_PREFETCH_DEPTH).await;
        assert_eq!(
            (0..depth).collect::<BTreeSet<_>>(),
            wave.keys().copied().collect()
        );
        assert_nothing_parks(&mut parked).await;

        // Later objects complete first: they wait for object 0 and still
        // count toward the depth.
        for object_seq in (1..depth).rev() {
            wave.remove(&object_seq).unwrap().send(true).unwrap();
        }
        wait_until(|| io.in_flight.load(Ordering::SeqCst) == 1).await;
        assert!(stream.next().now_or_never().is_none());
        assert_nothing_parks(&mut parked).await;
        wave.remove(&0).unwrap().send(true).unwrap();
        assert_eq!(object_entries([0]), next_batch(&mut stream).await);

        // No fetch starts while the caller holds the entries of object 0;
        // each later poll starts the fetch of the next object.
        assert_nothing_parks(&mut parked).await;
        let rest = tokio::spawn(stream.try_collect::<Vec<_>>());
        let refills = parked_operations(&mut parked, (objects - depth) as usize).await;
        assert_eq!(
            (depth..objects).collect::<BTreeSet<_>>(),
            refills.keys().copied().collect()
        );
        for release in refills.into_values() {
            release.send(true).unwrap();
        }
        let rest = timeout(WAIT, rest).await.unwrap().unwrap().unwrap();
        assert_eq!(
            object_entries(1..objects),
            rest.into_iter().flatten().collect::<Vec<_>>()
        );
        assert_eq!(READ_PREFETCH_DEPTH, io.max_in_flight.load(Ordering::SeqCst));
        assert_eq!(
            reads + 1,
            METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_read_fails_at_a_prefetched_object_in_order() {
        let _serialized = SERIALIZED.lock().await;
        let depth = READ_PREFETCH_DEPTH as u64;
        let objects = depth + 4;
        let failed = 5;
        let (store, io, mut parked) = open_parking_range_reads(objects).await;
        let reads = METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count();
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();
        assert!(stream.next().now_or_never().is_none());
        let mut wave = parked_operations(&mut parked, READ_PREFETCH_DEPTH).await;

        // The fetch of object 5 fails before any earlier fetch completes.
        wave.remove(&failed).unwrap().send(false).unwrap();
        wait_until(|| io.in_flight.load(Ordering::SeqCst) == READ_PREFETCH_DEPTH - 1).await;
        for release in wave.into_values() {
            release.send(true).unwrap();
        }
        let mut refills = Vec::new();
        for object_seq in 0..failed {
            assert_eq!(object_entries([object_seq]), next_batch(&mut stream).await);
            if object_seq > 0 {
                refills.push(next_parked_operation(&mut parked).await);
            }
        }
        let error = timeout(WAIT, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(&error, Error::WalObjectStore { operation: "read", path, .. }
                if *path == io.object_path(failed)),
            "unexpected error: {error:?}"
        );
        assert!(timeout(WAIT, stream.next()).await.unwrap().is_none());

        // The fetches started meanwhile are abandoned.
        assert_eq!(
            (depth..objects).collect::<Vec<_>>(),
            refills
                .iter()
                .map(|(object_seq, _)| *object_seq)
                .collect::<Vec<_>>()
        );
        for (_, mut release) in refills {
            timeout(WAIT, release.closed()).await.unwrap();
        }
        assert_nothing_parks(&mut parked).await;
        assert_eq!(
            reads,
            METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_read_skips_prefetched_collected_and_corrupted_objects() {
        let _serialized = SERIALIZED.lock().await;
        let objects = READ_PREFETCH_DEPTH as u64 + 4;
        let (corrupted, collected) = (3, 9);
        let object_store = memory_store();
        put_object_per_entry(&object_store, objects).await;
        let path = object_path(&object_store, corrupted);
        let bytes = object_store.read(&path).await.unwrap().to_vec();
        let segment = footer_of(&bytes).1.remove(0);
        corrupt_object(&object_store, &path, |bytes| {
            bytes[(segment.segment_offset + segment.segment_len - 1) as usize] ^= 1;
        })
        .await;
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(CorruptedSegmentAction::Skip, store.on_corrupted_segment);
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

        // A collection removes object 9 after the read listed it.
        let stream = store.read(&provider(region(1)), 0).await.unwrap();
        store.catalog.write().unwrap().remove_object(collected);
        object_store
            .delete(&object_path(&object_store, collected))
            .await
            .unwrap();
        let entries = timeout(WAIT, stream.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(
            object_entries((0..objects).filter(|seq| ![corrupted, collected].contains(seq))),
            entries
        );
        assert_eq!(
            vec![WalHole {
                path,
                object_seq: corrupted,
                min_entry_id: corrupted + 1,
                max_entry_id: corrupted + 1,
            }],
            store.wal_holes(&provider(region(1))).unwrap()
        );
        assert_eq!(
            skipped + 1,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_dropped_read_abandons_its_prefetches() {
        let _serialized = SERIALIZED.lock().await;
        let objects = READ_PREFETCH_DEPTH as u64 + 4;
        let (store, _io, mut parked) = open_parking_range_reads(objects).await;
        let reads = METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count();
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();
        assert!(stream.next().now_or_never().is_none());
        let mut wave = parked_operations(&mut parked, READ_PREFETCH_DEPTH).await;
        wave.remove(&0).unwrap().send(true).unwrap();
        assert_eq!(object_entries([0]), next_batch(&mut stream).await);

        drop(stream);
        for (_, mut release) in wave {
            timeout(WAIT, release.closed()).await.unwrap();
        }
        assert_nothing_parks(&mut parked).await;
        assert_eq!(
            reads,
            METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_read_fails_at_a_panicking_fetch() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_object_per_entry(&object_store, 3).await;
        let (io, _) = RecordingIo::over(object_store);
        let store = open_over(io.clone(), &eager()).await;
        *io.panic_range_read_of.lock().unwrap() = Some(1);
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();
        assert_eq!(object_entries([0]), next_batch(&mut stream).await);
        let error = timeout(WAIT, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(&error, Error::WalSegmentFetch { path, .. } if *path == io.object_path(1)),
            "unexpected error: {error:?}"
        );
        assert!(timeout(WAIT, stream.next()).await.unwrap().is_none());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_reads_fetch_ahead_within_a_shared_budget() {
        let _serialized = SERIALIZED.lock().await;
        let depth = READ_PREFETCH_DEPTH as u64;
        let objects = depth + 4;
        let (store, _io, mut parked) = open_parking_range_reads(objects).await;

        // While other reads hold the whole budget, a read fetches only the
        // segment it yields next.
        let held = store
            .read_prefetch_budget
            .clone()
            .try_acquire_many_owned(READ_PREFETCH_BYTES as u32)
            .unwrap();
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();
        assert!(stream.next().now_or_never().is_none());
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        assert_nothing_parks(&mut parked).await;
        release.send(true).unwrap();
        assert_eq!(object_entries([0]), next_batch(&mut stream).await);

        // Once the budget is free, the next poll fetches ahead up to the depth.
        drop(held);
        assert!(stream.next().now_or_never().is_none());
        let wave = parked_operations(&mut parked, READ_PREFETCH_DEPTH).await;
        assert_eq!(
            (1..=depth).collect::<BTreeSet<_>>(),
            wave.keys().copied().collect()
        );
        assert_nothing_parks(&mut parked).await;
        for release in wave.into_values() {
            release.send(true).unwrap();
        }
        let rest = tokio::spawn(stream.try_collect::<Vec<_>>());
        for release in parked_operations(&mut parked, (objects - depth - 1) as usize)
            .await
            .into_values()
        {
            release.send(true).unwrap();
        }
        let rest = timeout(WAIT, rest).await.unwrap().unwrap().unwrap();
        assert_eq!(
            object_entries(1..objects),
            rest.into_iter().flatten().collect::<Vec<_>>()
        );
        assert_eq!(
            READ_PREFETCH_BYTES,
            store.read_prefetch_budget.available_permits()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_dropped_read_keeps_reservations_until_its_fetches_end() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        put_object_per_entry(&object_store, 2).await;
        let (io, _) = RecordingIo::over(object_store);
        let store = open_over(io.clone(), &eager()).await;
        let budget = store.read_prefetch_budget.clone();
        let (began_tx, began_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *io.block_range_read_of.lock().unwrap() = Some((1, began_tx, release_rx));

        // The first poll fetches object 0, and object 1 with its bytes
        // reserved; the read of object 1 blocks its thread.
        let mut stream = store.read(&provider(region(1)), 0).await.unwrap();
        let _ = stream.next().now_or_never();
        began_rx.recv_timeout(WAIT).unwrap();
        assert!(budget.available_permits() < READ_PREFETCH_BYTES);

        // The aborted fetch keeps its bytes until it ends.
        drop(stream);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(budget.available_permits() < READ_PREFETCH_BYTES);
        release_tx.send(()).unwrap();
        wait_until(|| budget.available_permits() == READ_PREFETCH_BYTES).await;
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_assigns_ids_from_the_object_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);

        // Appends admitted into one batch share its object; the regions take
        // their own positions in admission order, and every append learns
        // the last id of each of its regions.
        let first = spawn_append_batch(
            &store,
            vec![
                entry(&store, region_one, "a1"),
                entry(&store, region_two, "b1"),
                entry(&store, region_one, "a2"),
            ],
        );
        store.wait_for_admitted_appends(1).await.unwrap();
        let second = spawn_append_batch(&store, vec![entry(&store, region_two, "b2")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        assert!(!first.is_finished());
        assert_eq!(0, latest(&store, region_one));
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 2)), (region_two, id(1, 1))]),
            first.await.unwrap().unwrap().last_entry_ids
        );
        assert_eq!(
            HashMap::from([(region_two, id(1, 2))]),
            second.await.unwrap().unwrap().last_entry_ids
        );

        // The next object starts every region at position one again.
        let third = spawn_append_batch(
            &store,
            vec![
                entry(&store, region_two, "b3"),
                entry(&store, region_one, "a3"),
            ],
        );
        store.wait_for_admitted_appends(3).await.unwrap();
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(2, 1)), (region_two, id(2, 1))]),
            third.await.unwrap().unwrap().last_entry_ids
        );
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(
                region_one,
                &[(id(1, 1), "a1"), (id(1, 2), "a2"), (id(2, 1), "a3")]
            ),
            read_entries(&store, region_one, 1).await
        );
        assert_eq!(
            expected_entries(region_two, &[(id(1, 2), "b2"), (id(2, 1), "b3")]),
            read_entries(&store, region_two, id(1, 2)).await
        );
        assert_eq!(id(2, 1), latest(&store, region_one));
        assert_eq!(id(2, 1), latest(&store, region_two));
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_seals_when_a_region_exhausts_its_positions() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);
        let entries_of = |count: u64| {
            (0..count)
                .map(|_| entry(&store, region_id, ""))
                .collect::<Vec<_>>()
        };

        // The open batch holds every position of the region.
        let full = spawn_append_batch(&store, entries_of(POSITION_LIMIT - 1));
        store.wait_for_admitted_appends(1).await.unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        // The next entry of the region seals the batch and opens the next
        // object; another region would still have fit.
        let next = spawn_append_batch(&store, vec![entry(&store, region_id, "next")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        let response = timeout(WAIT, full).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, POSITION_LIMIT - 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![1], collected_object_seqs(&store).await);

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
        let response = timeout(WAIT, next).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        let after = spawn_append_batch(&store, vec![entry(&store, region_id, "after")]);
        store.wait_for_admitted_appends(3).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, after).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(3, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(region_id, &[(id(2, 1), "next"), (id(3, 1), "after")]),
            read_entries(&store, region_id, id(2, 1)).await
        );
        store.stop().await.unwrap();
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
            // Object 1 carries the boundary past the start object.
            let expected_seqs = (1..=index as u64 + 1).collect::<Vec<_>>();
            assert_eq!(expected_seqs, collected_object_seqs(&store).await);
        }

        // Ticks with an empty open batch do not create objects.
        tokio::time::sleep(interval * 5).await;
        let seqs = collected_object_seqs(&store).await;
        assert_eq!(vec![1, 2, 3], seqs);
        for object_seq in seqs {
            let bytes = store.io.get(object_seq).await.unwrap();
            let decoded = decode_object(&bytes).unwrap();
            assert_eq!(1, decoded.records.len(), "object {object_seq} is empty");
        }
        assert_eq!(
            expected_entries(
                region(1),
                &[(id(1, 1), "a1"), (id(2, 1), "a2"), (id(3, 1), "a3")]
            ),
            read_entries(&store, region(1), 1).await
        );
    }

    #[tokio::test]
    async fn test_store_rejects_entries_of_another_region() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);

        let entry = Entry::Naive(NaiveEntry {
            provider: provider(region_id),
            region_id: region(2),
            entry_id: 0,
            data: Vec::new(),
        });
        let error = store.append_batch(vec![entry]).await.unwrap_err();
        assert!(
            matches!(error, Error::MismatchedWalRegion { .. }),
            "unexpected error: {error:?}"
        );
        let incomplete = Entry::MultiplePart(MultiplePartEntry {
            provider: provider(region_id),
            region_id,
            entry_id: 0,
            headers: vec![MultiplePartHeader::First],
            parts: vec![b"a1".to_vec()],
        });
        let error = store.append_batch(vec![incomplete]).await.unwrap_err();
        assert!(
            matches!(error, Error::IncompleteWalEntry { region_id: actual, .. } if actual == region_id),
            "unexpected error: {error:?}"
        );
        assert_eq!(
            common_error::status_code::StatusCode::InvalidArguments,
            error.status_code()
        );
        assert!(
            store
                .append_batch(Vec::new())
                .await
                .unwrap()
                .last_entry_ids
                .is_empty()
        );
        // Nothing was admitted.
        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_pipelines_creates_and_acknowledges_in_sequence_order() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 2).await;

        // At most the limit of creates run at a time; the rest wait for a slot.
        let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        assert_eq!(
            (1..=MAX_IN_FLIGHT_CREATES as u64).collect::<BTreeSet<_>>(),
            releases.keys().copied().collect::<BTreeSet<_>>()
        );
        round_trip_actor(&store).await;
        assert!(parked.try_recv().is_err());
        assert!(appends.iter().all(|append| !append.is_finished()));

        // Objects 3 and 2 become durable before object 1 and free a slot
        // each, but nothing is acknowledged ahead of object 1.
        for (released, expected_next) in [(3, 5), (2, 6)] {
            releases.remove(&released).unwrap().send(true).unwrap();
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(expected_next, object_seq);
            releases.insert(object_seq, release);
        }
        round_trip_actor(&store).await;
        assert_eq!(vec![0, 2, 3], object_seqs(io.as_ref()).await);
        assert!(appends.iter().all(|append| !append.is_finished()));
        assert_eq!(0, latest(&store, region_id));

        // Object 1 releases the acknowledgements of objects 1 to 3.
        releases.remove(&1).unwrap().send(true).unwrap();
        let mut appends = appends.into_iter();
        for object_seq in 1..4 {
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
        assert_eq!(id(3, 1), latest(&store, region_id));

        // Object 6 before object 5: the append of object 6 waits for it.
        releases.remove(&4).unwrap().send(true).unwrap();
        releases.remove(&6).unwrap().send(true).unwrap();
        let fourth = timeout(WAIT, appends.next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(4, 1))]),
            fourth.last_entry_ids
        );
        let appends = appends.collect::<Vec<_>>();
        round_trip_actor(&store).await;
        assert!(appends.iter().all(|append| !append.is_finished()));
        releases.remove(&5).unwrap().send(true).unwrap();
        for (append, object_seq) in appends.into_iter().zip(5..) {
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
        assert_eq!(vec![1, 2, 3, 4, 5, 6], collected_object_seqs(&store).await);
        // Every batch extends the batch sealed before it, whichever was
        // durable first.
        for object_seq in 1..=6 {
            let header = decode_header(&io.get(object_seq).await.unwrap()).unwrap();
            assert_eq!(
                Some(object_seq - 1),
                header.prev.map(|link| link.object_seq)
            );
        }
        assert_eq!(
            expected_entries(
                region_id,
                &[
                    (id(1, 1), "a1"),
                    (id(2, 1), "a2"),
                    (id(3, 1), "a3"),
                    (id(4, 1), "a4"),
                    (id(5, 1), "a5"),
                    (id(6, 1), "a6")
                ]
            ),
            read_entries(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_repeats_a_transient_failure_without_failing_the_writers() {
        let _serialized = SERIALIZED.lock().await;
        let (store, _, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        // Every slot is taken; the last batch waits for one.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        round_trip_actor(&store).await;
        assert!(parked.try_recv().is_err());

        // Object 1 fails while objects 2 to 4 are created and object 5 waits
        // for a slot: no writer fails, and object 1 is attempted again under
        // its sequence while object 5 takes the free slot.
        releases.remove(&1).unwrap().send(false).unwrap();
        for release in releases.into_values() {
            release.send(true).unwrap();
        }
        let retries = parked_operations(&mut parked, 2).await;
        assert_eq!(BTreeSet::from([1, 5]), retries.keys().copied().collect());
        // Objects 2 to 4 are durable only once object 1 is.
        assert!(appends.iter().all(|append| !append.is_finished()));
        assert_eq!(0, latest(&store, region_id));
        for release in retries.into_values() {
            release.send(true).unwrap();
        }
        for (append, object_seq) in appends.into_iter().zip(1..) {
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(id(5, 1), latest(&store, region_id));
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_permanent_failure_before_a_durable_object_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let foreign = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        // Another writer of the same epoch took sequence 1.
        put_foreign(&object_store, 1, 1).await;
        let appends = spawn_appends(&store, region_id, 2).await;
        let mut releases = parked_operations(&mut parked, 2).await;

        // Object 2 is created; object 1 conflicts with the foreign object.
        releases.remove(&2).unwrap().send(true).unwrap();
        timeout(WAIT, async {
            while object_seqs(&foreign).await != [0, 1, 2] {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        round_trip_actor(&store).await;
        assert!(appends.iter().all(|append| !append.is_finished()));
        releases.remove(&1).unwrap().send(true).unwrap();
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
        // The entries of the store were handed out and are lost, which stop
        // reports.
        let error = store.stop().await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_conflict_with_an_earlier_epoch_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        // An object of an earlier epoch appears under sequence 1, which this
        // store assigned: its entries were handed out and can neither be
        // written there nor be given up.
        put_foreign(&object_store, 1, 0).await;
        let appended = spawn_appends(&store, region_id, 1).await;
        next_parked_operation(&mut parked)
            .await
            .1
            .send(true)
            .unwrap();
        let error = timeout(WAIT, appended.into_iter().next().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
        assert!(store.latest_entry_id(&provider(region_id)).is_err());
        let error = store.stop().await.unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_store_obsolete_raises_the_sequence_floor() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_one = region(1);
        let region_two = region(2);
        let mut admitted = 0;
        let mut spawn_append = |region_id, data: &str| {
            admitted += 1;
            (
                spawn_append_batch(&store, vec![entry(&store, region_id, data)]),
                admitted,
            )
        };
        let (first, count) = spawn_append(region_one, "a1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, first).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1))]),
            response.last_entry_ids
        );
        let (second, count) = spawn_append(region_one, "a2");
        store.wait_for_admitted_appends(count).await.unwrap();

        // A durable watermark names an object below the next sequence and
        // changes nothing: the open batch goes on under sequence 2.
        store
            .obsolete(&provider(region_one), region_one, id(1, 1))
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
                Error::WalObjectSequenceUnsettled { object_seq: 2, .. }
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(RetryHint::Retryable, error.retry_hint());
        assert!(
            store
                .obsolete_entry_ids
                .lock()
                .unwrap()
                .get(&region_two)
                .is_none()
        );

        // Once the batch is durable the sequence moves above that object, so
        // the region's next id is greater than the watermark. An object
        // without entries marks the floor at object 4 before any id above it
        // is handed out, and `obsolete` returns once it is durable.
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, second).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(2, 1))]),
            response.last_entry_ids
        );
        let response = timeout(WAIT, third).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(2, 2))]),
            response.last_entry_ids
        );
        // Object 2 carries the boundary past the watermark of region one.
        assert_eq!(vec![2], collected_object_seqs(&store).await);
        store
            .obsolete(&provider(region_two), region_two, id(3, 7))
            .await
            .unwrap();
        assert_eq!(vec![2, 4], object_seqs(store.io.as_ref()).await);
        // The watermark is clamped to the region's durable id, as no entry
        // of the region is durable here.
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_two)
        );
        let (fourth, count) = spawn_append(region_two, "b1");
        store.wait_for_admitted_appends(count).await.unwrap();
        store.seal_open_batch().await.unwrap();
        let response = timeout(WAIT, fourth).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(5, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![2, 4, 5], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(region_two, &[(id(5, 1), "b1")]),
            read_entries(&store, region_two, 1).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_floor_waits_for_an_empty_pipeline() {
        let _serialized = SERIALIZED.lock().await;
        let (store, _, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_one = region(1);
        let region_two = region(2);
        let pending = spawn_append_batch(&store, vec![entry(&store, region_one, "a1")]);
        let (_, release) = next_parked_operation(&mut parked).await;

        // Object 1 is in flight: the floor cannot move, since the sealed
        // batches must take consecutive sequences, and nothing is recorded.
        let error = store
            .obsolete(&provider(region_two), region_two, id(5, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 2, .. }
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
        release.send(true).unwrap();
        timeout(WAIT, pending).await.unwrap().unwrap().unwrap();

        // Once it is durable the floor moves: object 6 marks it, and the
        // region's next id lies above it.
        let raised = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .obsolete(&provider(region_two), region_two, id(5, 1))
                    .await
            })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(6, object_seq);
        assert!(!raised.is_finished());
        release.send(true).unwrap();
        timeout(WAIT, raised).await.unwrap().unwrap().unwrap();
        let write = spawn_append_batch(&store, vec![entry(&store, region_two, "b1")]);
        next_parked_operation(&mut parked)
            .await
            .1
            .send(true)
            .unwrap();
        let response = timeout(WAIT, write).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_two, id(7, 1))]),
            response.last_entry_ids
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_obsolete_queued_behind_stop_records_the_watermark() {
        let _serialized = SERIALIZED.lock().await;
        let (store, mut command_rx) = store_without_actor();
        let region_id = region(1);
        let provider_one = provider(region_id);

        // The actor exits with the command still queued and never answers
        // it: nothing is assigned an id any more, so the watermark alone
        // holds, clamped to the durable id of the region, none here.
        let (result, ()) = tokio::join!(store.obsolete(&provider_one, region_id, 1), async {
            let command = timeout(WAIT, command_rx.recv()).await.unwrap();
            assert!(matches!(
                command,
                Some(Command::Obsolete { entry_id: 1, .. })
            ));
        });
        result.unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );

        // The same holds for a call that finds the actor gone.
        drop(command_rx);
        store
            .obsolete(&provider(region(2)), region(2), id(1, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region(2))
        );
    }

    #[tokio::test]
    async fn test_store_hooks_hold_and_fail_creates() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        store.hold_creates();
        let held = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        round_trip_actor(&store).await;
        assert!(!held.is_finished());
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        store.release_creates();
        timeout(WAIT, held).await.unwrap().unwrap().unwrap();
        assert_eq!(vec![1], collected_object_seqs(&store).await);

        // A failed create is repeated until the hook lets it write.
        store.fail_creates();
        let failing = spawn_append_batch(&store, vec![entry(&store, region_id, "a2")]);
        timeout(WAIT, store.wait_for_failed_creates(2))
            .await
            .unwrap()
            .unwrap();
        assert!(!failing.is_finished());
        assert_eq!(vec![1], object_seqs(store.io.as_ref()).await);
        store.allow_creates();
        timeout(WAIT, failing).await.unwrap().unwrap().unwrap();
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);

        store.raise_stopped_flag();
        assert_stopped(&append(&store, region_id, "a3").await.unwrap_err());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_hook_fails_the_next_create_after_it_writes() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);

        // Object 1 is stored, but its create reports a failure: the create
        // is repeated with the same bytes, which the identical object
        // accepts.
        store.fail_next_create_after_write();
        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        assert_eq!(1, *store.failed_creates.borrow());
        assert_eq!(id(1, 1), latest(&store, region_id));
        let response = append(&store, region_id, "a2").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_hooks_observe_parked_creates_waits_and_crash() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(eager())).await;
        let region_id = region(1);

        // The append is acknowledged and its create parks; a wait for its
        // entry has to wait.
        store.hold_creates();
        append(&store, region_id, "a1").await.unwrap();
        timeout(WAIT, store.wait_for_parked_creates(1))
            .await
            .unwrap()
            .unwrap();
        let wait = {
            let store = store.clone();
            tokio::spawn(async move { store.wait_durable(&provider(region_id), id(1, 1)).await })
        };
        timeout(WAIT, store.wait_for_durability_waits(1))
            .await
            .unwrap()
            .unwrap();
        assert!(!wait.is_finished());

        // The crash fails the waiter and the store, and the parked create
        // never writes its object.
        store.crash().await;
        assert_stopped(&timeout(WAIT, wait).await.unwrap().unwrap().unwrap_err());
        assert_stopped(&append(&store, region_id, "a2").await.unwrap_err());
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
    }

    #[tokio::test]
    async fn test_store_create_held_when_the_store_is_dropped_never_runs() {
        let _serialized = SERIALIZED.lock().await;
        let (io, _) = RecordingIo::over(memory_store());
        let store = open_over(io.clone(), &eager()).await;
        store.hold_creates();
        let append = spawn_append_batch(&store, vec![entry(&store, region(1), "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        append.abort();
        let _ = append.await;
        // A command sender outlives the store, so the actor keeps running and
        // the parked create completes on the closed hold channel instead of
        // being dropped with the actor.
        let command_tx = store.command_tx.clone();
        let terminal_error = store.terminal_error.clone();
        drop(store);
        timeout(WAIT, async {
            while terminal(&terminal_error).is_none() && object_seqs(io.as_ref()).await == [0] {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        let error = terminal(&terminal_error).unwrap();
        assert!(
            matches!(error.as_ref(), Error::ObjectStoreWalStopped { .. }),
            "unexpected error: {error:?}"
        );
        drop(command_tx);
    }

    /// How a store gives up a batch whose ids it handed out.
    #[derive(Debug, Clone, Copy)]
    enum GiveUp {
        /// The create meets an object of an earlier epoch, which poisons the
        /// store.
        Conflict,
        /// The create fails after `stop` began, which drops the backlog.
        DroppedBacklog,
    }

    /// Spawns a wait of `kind` until the region is durable through
    /// `entry_id`.
    fn spawn_wait(
        store: &Arc<ObjectStoreLogStore>,
        region_id: RegionId,
        entry_id: EntryId,
        kind: WaitKind,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let store = store.clone();
        tokio::spawn(async move {
            let provider = provider(region_id);
            match kind {
                WaitKind::Barrier => store.wait_durable(&provider, entry_id).await,
                WaitKind::Write => store.wait_write_durable(&provider, entry_id).await,
            }
        })
    }

    /// No wait for an id the store gave up answers Ok, whether it was
    /// registered before or after the store gave the batch up, and although a
    /// later batch of the region was created: only poisoning and a stop that
    /// drops the backlog give up a batch whose ids were handed out, and
    /// neither indexes a later batch.
    #[tokio::test]
    async fn test_store_never_certifies_an_id_it_gave_up() {
        let _serialized = SERIALIZED.lock().await;
        for give_up in [GiveUp::Conflict, GiveUp::DroppedBacklog] {
            for config in [manual(), enqueued(manual())] {
                let object_store = memory_store();
                let (store, _io, mut parked) =
                    open_parking_creates(object_store.clone(), &config).await;
                let region_id = region(1);
                let mut seals = Vec::new();
                let mut releases = HashMap::new();
                for data in ["a1", "a2"] {
                    store
                        .append_batch(vec![entry(&store, region_id, data)])
                        .await
                        .unwrap();
                    let sealing = store.clone();
                    seals.push(tokio::spawn(async move { sealing.seal_open_batch().await }));
                    let (object_seq, release) = next_parked_operation(&mut parked).await;
                    releases.insert(object_seq, release);
                }
                let mut waits = Vec::new();
                for entry_id in [id(1, 1), id(2, 1)] {
                    for kind in [WaitKind::Barrier, WaitKind::Write] {
                        waits.push(spawn_wait(&store, region_id, entry_id, kind));
                    }
                }
                timeout(WAIT, store.wait_for_durability_waits(4))
                    .await
                    .unwrap()
                    .unwrap();

                // Object 2 is created first, and is not indexed while object
                // 1 is pending.
                releases.remove(&2).unwrap().send(true).unwrap();
                timeout(WAIT, async {
                    while !object_seqs(store.io.as_ref()).await.contains(&2) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(0, latest(&store, region_id));
                let release = releases.remove(&1).unwrap();
                match give_up {
                    GiveUp::Conflict => {
                        put_foreign(&object_store, 1, 0).await;
                        release.send(true).unwrap();
                    }
                    GiveUp::DroppedBacklog => {
                        store.begin_stop().await;
                        release.send(false).unwrap();
                    }
                }
                for entry_id in [id(1, 1), id(2, 1)] {
                    for kind in [WaitKind::Barrier, WaitKind::Write] {
                        waits.push(spawn_wait(&store, region_id, entry_id, kind));
                    }
                }
                for wait in waits {
                    timeout(WAIT, wait).await.unwrap().unwrap().unwrap_err();
                }
                for seal in seals {
                    timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
                }
                // A poisoned store answers no more calls.
                if matches!(give_up, GiveUp::DroppedBacklog) {
                    assert_eq!(0, latest(&store, region_id));
                }
                timeout(WAIT, store.stop()).await.unwrap().unwrap_err();
            }
        }
    }

    #[tokio::test]
    async fn test_store_poison_drops_the_open_batch() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &manual()).await;
        let region_id = region(1);

        // Object 1 conflicts while a2 waits in the open batch under sequence
        // 2: both fail.
        let sealed = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        let (_, release) = next_parked_operation(&mut parked).await;
        let open = spawn_append_batch(&store, vec![entry(&store, region_id, "a2")]);
        store.wait_for_admitted_appends(2).await.unwrap();
        put_foreign(&object_store, 1, 1).await;
        release.send(true).unwrap();
        for append in [sealed, open] {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        store.stop().await.unwrap_err();
    }

    #[tokio::test]
    async fn test_store_poisons_once_the_last_sequence_is_taken() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let last = OBJECT_SEQ_LIMIT - 1;
        // The start object of the store takes the sequence before the last,
        // above the sequences the writer of the listed object can have
        // handed out.
        let listed = last - 2 - UNLISTED_SEQUENCES;
        put_object(&object_store, listed, region(1), &[id(listed, 1)]).await;
        let store = open(object_store, &eager()).await;
        let next = entry(&store, region(1), "next");

        // The batch at the last sequence is created and acknowledged, and the
        // store is poisoned as soon as it takes that sequence.
        let response = append(&store, region(1), "last").await.unwrap();
        assert_eq!(
            HashMap::from([(region(1), id(last, 1))]),
            response.last_entry_ids
        );
        for error in [
            store.latest_entry_id(&provider(region(1))).unwrap_err(),
            store.append_batch(vec![next]).await.unwrap_err(),
        ] {
            assert!(
                matches!(
                    unwrap_shared(&error),
                    Error::WalObjectSequenceExhausted { last_object_seq, .. } if *last_object_seq == last
                ),
                "unexpected error: {error:?}"
            );
        }
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_holds_appends_back_while_the_sealed_batches_are_full() {
        let _serialized = SERIALIZED.lock().await;
        let (store, _, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        let admitted = || *store.admitted_appends.borrow();
        let stalled = METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get();
        let stalled_waits = METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.get_sample_count();

        // Every create is parked: once no two more sealed batches fit, the
        // next append is held back and counted as stalled, and further
        // appends wait in the channel; none is admitted.
        let mut appends = spawn_appends(&store, region_id, MAX_SEALED_BATCHES - 1).await;
        for index in 0..3 {
            let entries = vec![entry(&store, region_id, &format!("w{index}"))];
            appends.push(spawn_append_batch(&store, entries));
        }
        let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        wait_until(|| METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get() == stalled + 1).await;
        round_trip_actor(&store).await;
        assert_eq!(MAX_SEALED_BATCHES - 1, admitted());

        // A durable object frees a place, and the held-back append is
        // admitted; the next one is held back in its turn.
        releases.remove(&1).unwrap().send(true).unwrap();
        timeout(WAIT, store.wait_for_admitted_appends(MAX_SEALED_BATCHES))
            .await
            .unwrap()
            .unwrap();
        wait_until(|| METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get() == stalled + 2).await;
        round_trip_actor(&store).await;
        assert_eq!(MAX_SEALED_BATCHES, admitted());
        assert_eq!(
            stalled_waits + 1,
            METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.get_sample_count()
        );

        // Stop is handled while appends are held back: the appends still
        // waiting learn of the stop, and the backlog of every admitted append
        // is uploaded.
        let stop = begin_spawned_stop(&store).await;
        for release in releases.into_values() {
            release.send(true).unwrap();
        }
        for release in parked_operations(&mut parked, MAX_SEALED_BATCHES - MAX_IN_FLIGHT_CREATES)
            .await
            .into_values()
        {
            release.send(true).unwrap();
        }
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        let mut acknowledged = 0;
        for append in appends {
            match timeout(WAIT, append).await.unwrap().unwrap() {
                Ok(_) => acknowledged += 1,
                Err(error) => assert_stopped(&error),
            }
        }
        assert_eq!(MAX_SEALED_BATCHES, acknowledged);
    }

    /// Starts `stop` and waits until it has set the stopped flag.
    async fn begin_spawned_stop(
        store: &Arc<ObjectStoreLogStore>,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        while !store.stopped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        stop
    }

    #[tokio::test]
    async fn test_store_stop_during_failed_flush_reports_stopped() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let pending = spawn_append_batch(&store, vec![entry(&store, region(1), "a1")]);
        let (_, release) = next_parked_operation(&mut parked).await;
        let stop = begin_spawned_stop(&store).await;
        assert!(!stop.is_finished());

        // The create fails after stop began: the backlog is dropped, its
        // writer learns of the stop and the stop reports the failure.
        release.send(false).unwrap();
        let error = timeout(WAIT, stop).await.unwrap().unwrap().unwrap_err();
        assert!(
            matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        assert_stopped(&timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err());
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert_stopped(&append(&store, region(1), "a2").await.unwrap_err());
    }

    #[tokio::test]
    async fn test_store_stop_with_several_creates_in_flight() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
        let region_id = region(1);
        // Four creates are in flight, the fifth batch waits for a slot.
        let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
        let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        assert!(parked.try_recv().is_err());

        let stop = begin_spawned_stop(&store).await;
        assert!(!stop.is_finished());
        // Nothing is assigned an id after stop began, so a watermark needs no
        // floor even while creates are in flight; it is clamped to the
        // region's durable id, none here.
        store
            .obsolete(&provider(region(2)), region(2), id(9, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region(2))
        );

        // The creates in flight run to completion in any order, and the
        // batch that waited for a slot is uploaded too: stop uploads the
        // whole backlog.
        for object_seq in [3, 1, 4, 2] {
            releases.remove(&object_seq).unwrap().send(true).unwrap();
        }
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(5, object_seq);
        release.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        for (append, object_seq) in appends.into_iter().zip(1..) {
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq, 1))]),
                response.last_entry_ids
            );
        }
        assert!(parked.try_recv().is_err());
        assert_eq!(vec![0, 1, 2, 3, 4, 5], object_seqs(io.as_ref()).await);
        assert_eq!(id(5, 1), latest(&store, region_id));
        timeout(WAIT, store.command_tx.closed()).await.unwrap();
    }

    #[tokio::test]
    async fn test_store_stop_during_conflicting_flush_reports_the_conflict_and_poisons() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (store, _, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let region_id = region(1);
        let pending = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        let (_, release) = next_parked_operation(&mut parked).await;
        let stop = begin_spawned_stop(&store).await;
        // Another writer of the same epoch takes the sequence before the
        // create runs.
        put_foreign(&object_store, 1, 1).await;

        release.send(true).unwrap();
        assert_stopped(&timeout(WAIT, pending).await.unwrap().unwrap().unwrap_err());
        for error in [
            timeout(WAIT, stop).await.unwrap().unwrap().unwrap_err(),
            store.latest_entry_id(&provider(region_id)).unwrap_err(),
            store.read(&provider(region_id), 1).await.err().unwrap(),
        ] {
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
        }
    }

    fn injected_failure<T>(operation: &'static str, path: String) -> Result<T> {
        Err(
            object_store::Error::new(object_store::ErrorKind::Unexpected, "injected failure")
                .set_temporary(),
        )
        .context(WalObjectStoreSnafu { operation, path })
    }

    fn chain_header(object_seq: u64, epoch: u64, prev: Option<(u64, u64)>) -> Header {
        Header {
            object_seq,
            epoch,
            prev: prev.map(|(object_seq, epoch)| ChainLink { object_seq, epoch }),
            reclaim_boundary: 0,
        }
    }

    fn chain_of(headers: &[Header]) -> Vec<u64> {
        select_chain(
            &headers
                .iter()
                .map(|header| (header.object_seq, header))
                .collect(),
        )
    }

    #[test]
    fn test_store_selects_the_chain_of_the_latest_complete_object() {
        assert!(chain_of(&[]).is_empty());
        // A linear chain is replayed whole.
        assert_eq!(
            vec![0, 1, 2],
            chain_of(&[
                chain_header(0, 1, None),
                chain_header(1, 1, Some((0, 1))),
                chain_header(2, 1, Some((1, 1))),
            ])
        );
        // Object 1 was reported as failed and object 2 links past it.
        assert_eq!(
            vec![0, 2],
            chain_of(&[
                chain_header(0, 1, None),
                chain_header(1, 1, Some((0, 1))),
                chain_header(2, 1, Some((0, 1))),
            ])
        );
        // Object 3 extends object 2, which never landed, so object 1 is the
        // tip although object 3 has a higher sequence.
        assert_eq!(
            vec![0, 1],
            chain_of(&[
                chain_header(0, 1, None),
                chain_header(1, 1, Some((0, 1))),
                chain_header(3, 1, Some((2, 1))),
            ])
        );
        // Object 1 links to an object of epoch 2 at sequence 0, where an
        // object of epoch 1 is.
        assert_eq!(
            vec![0],
            chain_of(&[chain_header(0, 1, None), chain_header(1, 2, Some((0, 2))),])
        );
        // Instance A left object 2 behind after instance B, a later epoch,
        // acknowledged object 1: the later epoch ends the chain.
        assert_eq!(
            vec![0, 1],
            chain_of(&[
                chain_header(0, 1, None),
                chain_header(1, 2, Some((0, 1))),
                chain_header(2, 1, Some((0, 1))),
            ])
        );
        // A missing predecessor breaks the link, whether or not it lies below
        // every present object, so an object that lands late below it cannot
        // change which links hold.
        assert!(
            chain_of(&[
                chain_header(3, 1, Some((2, 1))),
                chain_header(4, 1, Some((3, 1))),
            ])
            .is_empty()
        );
        assert!(
            chain_of(&[
                chain_header(2, 1, Some((1, 1))),
                chain_header(4, 2, Some((3, 1))),
                chain_header(5, 2, Some((4, 2))),
            ])
            .is_empty()
        );
        assert_eq!(
            vec![1],
            chain_of(&[chain_header(1, 1, None), chain_header(3, 1, Some((2, 1))),])
        );
        // A predecessor at or above the object itself never holds.
        assert!(chain_of(&[chain_header(5, 1, Some((5, 1)))]).is_empty());

        // A link below the reclaim boundary of the object it is judged for
        // holds whether or not the predecessor is present; above it a
        // missing predecessor still breaks the link.
        let bounded = |object_seq, prev, reclaim_boundary| Header {
            reclaim_boundary,
            ..chain_header(object_seq, 1, prev)
        };
        assert_eq!(
            vec![3, 4],
            chain_of(&[bounded(3, Some((2, 1)), 3), bounded(4, Some((3, 1)), 3)])
        );
        assert!(chain_of(&[bounded(4, Some((3, 1)), 3)]).is_empty());
        // Object 3 is incomplete with its own boundary, but object 4 only
        // needs it to be present.
        assert_eq!(
            vec![3, 4],
            chain_of(&[bounded(3, Some((2, 1)), 0), bounded(4, Some((3, 1)), 3)])
        );
        assert_eq!(
            vec![4],
            chain_of(&[bounded(3, Some((2, 1)), 0), bounded(4, Some((3, 1)), 4)])
        );
    }

    #[tokio::test]
    async fn test_store_recovery_indexes_only_the_chain() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let record = |entry_id: EntryId, data: &'static str| Record {
            region_id: region(1),
            entry_id,
            payload: Bytes::from_static(data.as_bytes()),
        };
        put_fixture(&io, 0, &[record(1, "a")]).await;
        let first = fixture_header(&io, 1).await;
        // Object 1 was reported as failed; object 2 reused its row sequences
        // and links past it.
        put_object_with_header(&io, first.clone(), &[record(id(1, 1), "old")]).await;
        let second = Header {
            object_seq: 2,
            ..first
        };
        put_object_with_header(&io, second, &[record(id(2, 1), "new")]).await;

        // Every open starts above the sequences an earlier instance can have
        // handed out and its epoch one above the sequence of its start
        // object, and the first restart's start object extends the chain the
        // second restart replays.
        for (start_seq, prev) in [(11, 2), (20, 11)] {
            let store = open(object_store.clone(), &eager()).await;
            assert_eq!(
                expected_entries(region(1), &[(1, "a"), (id(2, 1), "new")]),
                read_entries(&store, region(1), 0).await
            );
            assert_eq!(id(2, 1), latest(&store, region(1)));
            store.stop().await.unwrap();
            let start = decode_header(&io.get(start_seq).await.unwrap()).unwrap();
            assert_eq!(start_seq + 1, start.epoch);
            assert_eq!(Some(prev), start.prev.map(|prev| prev.object_seq));
        }
        assert_eq!(vec![0, 1, 2, 11, 20], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_recovery_never_reuses_the_sequence_of_an_orphan() {
        let _serialized = SERIALIZED.lock().await;
        let io = ObjectStoreIo::new(memory_store(), PREFIX).unwrap();
        put_fixture(&io, 0, &[]).await;
        // Object 2 extends object 1, which never landed.
        let orphan = Header {
            prev: Some(ChainLink {
                object_seq: 1,
                epoch: 1,
            }),
            ..fixture_header(&io, 2).await
        };
        put_object_with_header(&io, orphan, &[]).await;
        let recovered = recover(&io).await.unwrap();
        assert_eq!(Some(0), recovered.tip.map(|tip| tip.object_seq));
        assert_eq!(3, recovered.next_object_seq);
    }

    #[tokio::test]
    async fn test_store_recovery_rejects_objects_without_a_complete_chain() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        // Object 1 extends object 0, which is gone.
        put_object_with_header(&io, chain_header(1, 1, Some((0, 1))), &[]).await;
        let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::CorruptedWalObject { reason, .. }
                if reason.contains("no object among 1 present objects completes a chain")),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![1], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_recovery_keeps_the_later_epoch_over_a_late_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let record = |entry_id: EntryId, data: &'static str| Record {
            region_id: region(1),
            entry_id,
            payload: Bytes::from_static(data.as_bytes()),
        };
        put_object_with_header(&io, chain_header(0, 1, None), &[record(1, "a")]).await;
        // Instance B acknowledged object 1 before the create instance A issued
        // for object 2 landed.
        put_object_with_header(
            &io,
            chain_header(1, 2, Some((0, 1))),
            &[record(id(1, 1), "b")],
        )
        .await;
        put_object_with_header(
            &io,
            chain_header(2, 1, Some((0, 1))),
            &[record(id(2, 1), "late")],
        )
        .await;

        let store = open(object_store, &eager()).await;
        assert_eq!(
            expected_entries(region(1), &[(1, "a"), (id(1, 1), "b")]),
            read_entries(&store, region(1), 0).await
        );
        store.stop().await.unwrap();
        let start = decode_header(&io.get(11).await.unwrap()).unwrap();
        assert_eq!(12, start.epoch);
        assert_eq!(
            Some(ChainLink {
                object_seq: 1,
                epoch: 2,
            }),
            start.prev
        );
    }

    #[tokio::test]
    async fn test_store_open_starts_the_first_epoch_on_an_empty_prefix() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        store.stop().await.unwrap();
        let io = ObjectStoreIo::new(object_store, PREFIX).unwrap();
        let start = decode_header(&io.get(0).await.unwrap()).unwrap();
        assert_eq!(1, start.epoch);
        assert_eq!(None, start.prev);
        assert_eq!(vec![0], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_start_object_moves_past_an_earlier_epoch_only() {
        let _serialized = SERIALIZED.lock().await;
        // The start object takes sequence 9, above the sequences the writer
        // of object 0 can have handed out, and epoch 10.
        for (epoch, moves) in [(9, true), (10, false)] {
            let object_store = memory_store();
            let fixtures = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
            put_fixture(&fixtures, 0, &[]).await;
            // An object lands at the sequence of the start object after
            // recovery listed the prefix.
            let late = encode_object(chain_header(9, epoch, None), &[])
                .unwrap()
                .bytes;
            let io = Arc::new(RacingIo {
                inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
                late: Mutex::new(Some((9, late))),
            });
            let result = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await;
            if moves {
                let store = result.unwrap();
                let start = decode_header(&fixtures.get(10).await.unwrap()).unwrap();
                assert_eq!(11, start.epoch);
                assert_eq!(Some(0), start.prev.map(|prev| prev.object_seq));
                // Object 11 carries the boundary past the late object, which
                // recovery never listed.
                append(&store, region(1), "a").await.unwrap();
                assert_eq!(vec![11], collected_object_seqs(&store).await);
                store.stop().await.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, Error::WalObjectConflict { path, .. } if path == &fixtures.object_path(9)),
                    "unexpected error: {error:?}"
                );
                assert_eq!(vec![0, 9], object_seqs(&fixtures).await);
            }
        }
    }

    #[tokio::test]
    async fn test_store_open_fails_when_its_start_object_is_already_present() {
        let _serialized = SERIALIZED.lock().await;
        // On an empty prefix and after object 0 of epoch 1, another open that
        // recovered the same objects wrote the very start object this open
        // writes.
        for present in [false, true] {
            let object_store = memory_store();
            let fixtures = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
            let (start_seq, epoch, tip) = if present {
                put_fixture(&fixtures, 0, &[]).await;
                (9, 10, Some((0, 1)))
            } else {
                (0, 1, None)
            };
            // A prefix without segments is reclaimed up to the start object.
            let header = Header {
                reclaim_boundary: start_seq,
                ..chain_header(start_seq, epoch, tip)
            };
            let same = encode_object(header, &[]).unwrap().bytes;
            let io = Arc::new(RacingIo {
                inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
                late: Mutex::new(Some((start_seq, same))),
            });
            let error = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::UnconfirmedWalEpochStart { epoch: actual, path, .. }
                    if *actual == epoch && path == &fixtures.object_path(start_seq)),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());

            // The next open counts that object and starts a later epoch,
            // above the sequences its writer can have handed out.
            open(object_store, &eager()).await.stop().await.unwrap();
            let next_start = start_seq + 1 + UNLISTED_SEQUENCES;
            let start = decode_header(&fixtures.get(next_start).await.unwrap()).unwrap();
            assert_eq!(next_start + 1, start.epoch);
            assert_eq!(
                Some(ChainLink {
                    object_seq: start_seq,
                    epoch,
                }),
                start.prev
            );
        }
    }

    #[tokio::test]
    async fn test_store_opens_racing_across_a_late_object_take_distinct_epochs() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        put_fixture(&io, 0, &[]).await;
        // Open A recovers object 0 and pauses before its start object.
        let recovered = recover(&io).await.unwrap();
        // A late object of epoch 1 lands at sequence 2, across the gap at
        // sequence 1, and open B, which lists it, completes.
        put_object_with_header(&io, chain_header(2, 1, Some((0, 1))), &[]).await;
        open(object_store, &eager()).await.stop().await.unwrap();
        let b = decode_header(&io.get(11).await.unwrap()).unwrap();
        assert_eq!(12, b.epoch);

        // Open A resumes at the sequence it recovered: the epoch follows from
        // the sequence it claims, so it differs from the epoch of open B.
        let a = start_epoch(
            &io,
            recovered.next_object_seq,
            recovered.tip,
            recovered.reclaim_boundary,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            ChainLink {
                object_seq: 1,
                epoch: 2,
            },
            a
        );
    }

    #[tokio::test]
    async fn test_store_open_rejects_an_epoch_above_the_next_sequence() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        // No instance that starts at or below sequence 0 writes epoch 5.
        put_object_with_header(&io, chain_header(0, 5, None), &[]).await;
        let error = ObjectStoreLogStore::try_new(object_store, &eager(), 1, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::CorruptedWalObject { reason, .. }
                if reason.contains("carries epoch 5, above the next sequence 1")),
            "unexpected error: {error:?}"
        );
        assert_eq!(vec![0], object_seqs(&io).await);
    }

    #[tokio::test]
    async fn test_store_open_fails_when_its_start_object_lands_with_an_unknown_outcome() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let fixtures = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        put_fixture(&fixtures, 0, &[]).await;
        // The start object is stored, but its create reports an error.
        let io = Arc::new(LostResponseIo {
            inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
            lose_next: AtomicBool::new(true),
        });
        let error = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::WalObjectStore { .. }),
            "unexpected error: {error:?}"
        );
        // The open does not move to another sequence.
        assert_eq!(vec![0, 9], object_seqs(&fixtures).await);
        let stored = decode_header(&fixtures.get(9).await.unwrap()).unwrap();
        assert_eq!(10, stored.epoch);

        // The next open counts the stored start object and claims a later
        // epoch.
        open(object_store, &eager()).await.stop().await.unwrap();
        let start = decode_header(&fixtures.get(18).await.unwrap()).unwrap();
        assert_eq!(19, start.epoch);
        assert_eq!(
            Some(ChainLink {
                object_seq: 9,
                epoch: 10,
            }),
            start.prev
        );
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
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);
        assert_eq!(0, latest(&store, region_id));
        assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
        // A read of the region waits for its entries to become durable.
        let read = {
            let store = store.clone();
            tokio::spawn(async move { read_entries(&store, region_id, 1).await })
        };
        let response = append(&store, region_id, "a2").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 2))]),
            response.last_entry_ids
        );

        assert!(!read.is_finished());

        store.seal_open_batch().await.unwrap();
        assert_eq!(vec![1], collected_object_seqs(&store).await);
        assert_eq!(id(1, 2), latest(&store, region_id));
        assert_eq!(
            id(1, 2),
            store.durable_entry_id(&provider(region_id)).unwrap()
        );
        let expected = expected_entries(region_id, &[(id(1, 1), "a1"), (id(1, 2), "a2")]);
        assert_eq!(expected, timeout(WAIT, read).await.unwrap().unwrap());
        assert_eq!(expected, read_entries(&store, region_id, 1).await);
    }

    #[tokio::test]
    async fn test_store_enqueued_durable_id_advances_after_create_and_indexing() {
        let _serialized = SERIALIZED.lock().await;
        let (store, _, mut parked) = open_parking_creates(memory_store(), &enqueued(eager())).await;
        let region_one = region(1);
        let region_two = region(2);

        let response = timeout(WAIT, append(&store, region_one, "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            HashMap::from([(region_one, id(1, 1))]),
            response.last_entry_ids
        );
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(1, object_seq);
        assert_eq!(0, store.durable_entry_id(&provider(region_one)).unwrap());
        let wait = |entry_id| {
            let store = store.clone();
            tokio::spawn(async move { store.wait_durable(&provider(region_one), entry_id).await })
        };
        // An id the store never handed out waits for the ids of the region
        // that were handed out below it.
        let waits = [wait(id(1, 1)), wait(id(1, 7))];
        // The other region has nothing to wait for.
        timeout(WAIT, store.wait_durable(&provider(region_two), id(1, 1)))
            .await
            .unwrap()
            .unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(waits.iter().all(|wait| !wait.is_finished()));

        release.send(true).unwrap();
        for wait in waits {
            timeout(WAIT, wait).await.unwrap().unwrap().unwrap();
        }
        assert_eq!(
            id(1, 1),
            store.durable_entry_id(&provider(region_one)).unwrap()
        );
        assert_eq!(0, store.durable_entry_id(&provider(region_two)).unwrap());
        assert_eq!(vec![1], collected_object_seqs(&store).await);
    }

    #[tokio::test]
    async fn test_store_enqueued_wait_ignores_later_pending_entries_of_the_region() {
        let _serialized = SERIALIZED.lock().await;
        let (store, _io, mut parked) =
            open_parking_creates(memory_store(), &enqueued(eager())).await;
        let region_a = region(1);
        let region_b = region(2);
        // Object 1 holds region A's id(1, 1) and object 2 region B's id(2, 1).
        for (region_id, data) in [(region_a, "a1"), (region_b, "b1")] {
            let response = append(&store, region_id, data).await.unwrap();
            let (_, release) = next_parked_operation(&mut parked).await;
            release.send(true).unwrap();
            let entry_id = response.last_entry_ids[&region_id];
            timeout(WAIT, store.wait_durable(&provider(region_id), entry_id))
                .await
                .unwrap()
                .unwrap();
        }
        let response = append(&store, region_a, "a2").await.unwrap();
        assert_eq!(
            HashMap::from([(region_a, id(3, 1))]),
            response.last_entry_ids
        );
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(3, object_seq);

        // Every entry of region A up to id(2, 1) is durable, so the wait does
        // not depend on the parked create of object 3.
        timeout(WAIT, store.wait_durable(&provider(region_a), id(2, 1)))
            .await
            .unwrap()
            .unwrap();
        // A wait for id(3, 1) itself is answered only once object 3 is
        // indexed; the round trip orders the check after the actor handled it.
        let (response_tx, mut response_rx) = oneshot::channel();
        store
            .command_tx
            .send(Command::WaitDurable {
                region_id: region_a,
                kind: WaitKind::Barrier,
                entry_id: id(3, 1),
                response: response_tx,
            })
            .await
            .unwrap();
        round_trip_actor(&store).await;
        assert!(matches!(
            response_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release.send(true).unwrap();
        timeout(WAIT, response_rx).await.unwrap().unwrap().unwrap();
    }

    /// Once a caller asks the store to drain, the open batch is sealed at
    /// once rather than on the next tick, and every later append is refused.
    #[tokio::test]
    async fn test_store_drain_seals_the_open_batch_and_refuses_later_appends() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);

        append(&store, region_id, "a1").await.unwrap();
        assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
        store.begin_drain();
        timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_stopped(&append(&store, region_id, "a2").await.unwrap_err());
        assert_eq!(id(1, 1), latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_enqueued_obsolete_never_passes_the_durable_id() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        store
            .obsolete(&provider(region_id), region_id, id(1, 2))
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        store.seal_open_batch().await.unwrap();
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a1"), (id(1, 2), "a2")]),
            read_entries(&store, region_id, 1).await
        );
        store
            .obsolete(&provider(region_id), region_id, id(1, 2))
            .await
            .unwrap();
        assert_eq!(
            Some(&id(1, 2)),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        assert!(read_entries(&store, region_id, 1).await.is_empty());
    }

    /// Appends one entry in the enqueued mode with `config`, whose create
    /// parks, then appends a second one that the backlog threshold must
    /// stall. Returns the store, the parked creates, the stalled append and
    /// the release of the first create.
    async fn stall_second_append(
        config: ObjectStoreWalConfig,
    ) -> (
        Arc<ObjectStoreLogStore>,
        Arc<ParkedIo>,
        mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        tokio::task::JoinHandle<Result<AppendBatchResponse>>,
        oneshot::Sender<bool>,
    ) {
        let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
        let region_id = region(1);
        let response = timeout(WAIT, append(&store, region_id, "a1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        if config.max_unpersisted_age < Duration::from_secs(1) {
            tokio::time::sleep(config.max_unpersisted_age * 2).await;
        }

        let stalled = spawn_append_batch(&store, vec![entry(&store, region_id, "a2")]);
        // The stall seals the open batch so that an upload is in flight.
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(1, object_seq);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!stalled.is_finished());
        assert!(parked.try_recv().is_err());
        (store, io, parked, stalled, release)
    }

    #[tokio::test]
    async fn test_store_enqueued_backlog_bytes_stall_admission_until_an_upload_completes() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            max_unpersisted_bytes: ReadableSize(1),
            ..enqueued(manual())
        };
        let (store, _, mut parked, stalled, release) = stall_second_append(config).await;
        let region_id = region(1);
        // Two more appends queue behind the stalled one.
        let third = spawn_append_batch(&store, vec![entry(&store, region_id, "a3")]);
        let fourth = spawn_append_batch(&store, vec![entry(&store, region_id, "a4")]);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!third.is_finished() && !fourth.is_finished());
        assert!(parked.try_recv().is_err());

        // The upload releases the second append, whose entry reaches the
        // threshold again; the stall seals it so that the next upload can
        // release the third, and so on.
        release.send(true).unwrap();
        let response = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![1], collected_object_seqs(&store).await);
        assert_eq!(id(1, 1), latest(&store, region_id));
        for (append, object_seq) in [(third, 2), (fourth, 3)] {
            let (parked_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(object_seq, parked_seq);
            release.send(true).unwrap();
            let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(object_seq + 1, 1))]),
                response.last_entry_ids
            );
        }
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        // The fourth entry waits in the open batch, and a read of the region
        // waits for it.
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(4, object_seq);
        release.send(true).unwrap();
        timeout(WAIT, seal).await.unwrap().unwrap().unwrap();
        assert_eq!(
            expected_entries(
                region_id,
                &[
                    (id(1, 1), "a1"),
                    (id(2, 1), "a2"),
                    (id(3, 1), "a3"),
                    (id(4, 1), "a4")
                ]
            ),
            read_entries(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_backlog_age_stalls_admission_until_an_upload_completes() {
        let _serialized = SERIALIZED.lock().await;
        // The upload is released well within the age, which also bounds how
        // long the sealed batch may stay not durable.
        let config = ObjectStoreWalConfig {
            max_unpersisted_age: Duration::from_millis(500),
            ..enqueued(manual())
        };
        let (store, _, _parked, stalled, release) = stall_second_append(config).await;
        let region_id = region(1);

        release.send(true).unwrap();
        let response = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
            response.last_entry_ids
        );
        assert_eq!(vec![1], collected_object_seqs(&store).await);
        assert_eq!(id(1, 1), latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_fails_a_stalled_append_and_uploads_the_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            max_unpersisted_bytes: ReadableSize(1),
            ..enqueued(manual())
        };
        let (store, io, mut parked, stalled, release) = stall_second_append(config).await;

        let stop = {
            let store = store.clone();
            tokio::spawn(async move { store.stop().await })
        };
        assert_stopped(&timeout(WAIT, stalled).await.unwrap().unwrap().unwrap_err());
        release.send(true).unwrap();
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
        assert!(parked.try_recv().is_err());
    }

    /// A drain refuses the append held back behind the backlog and every
    /// later one with the stopped error, and the backlog still uploads.
    #[tokio::test]
    async fn test_store_enqueued_drain_refuses_a_stalled_append_and_uploads_the_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let config = ObjectStoreWalConfig {
            max_unpersisted_bytes: ReadableSize(1),
            ..enqueued(manual())
        };
        let (store, _, _parked, stalled, release) = stall_second_append(config).await;
        let region_id = region(1);

        store.begin_drain();
        assert_stopped(&timeout(WAIT, stalled).await.unwrap().unwrap().unwrap_err());
        assert_stopped(&append(&store, region_id, "a3").await.unwrap_err());
        release.send(true).unwrap();
        timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a1")]),
            read_entries(&store, region_id, 1).await
        );
    }

    #[tokio::test]
    async fn test_store_enqueued_repeats_a_create_that_failed_transiently() {
        let _serialized = SERIALIZED.lock().await;
        let (io, _) = RecordingIo::over(memory_store());
        let store = open_over(io.clone(), &enqueued(eager())).await;
        let region_id = region(1);

        // The create stores the object but reports a failure; the repeat
        // writes the same bytes under the same sequence.
        io.fail_after_next_put.store(true, Ordering::Relaxed);
        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![1], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a1")]),
            read_entries(&store, region_id, 1).await
        );

        // An object store with a retry layer reports a temporary error that
        // outlasted its retries as persistent; the create is repeated too,
        // and appends are still admitted meanwhile.
        let rejected = METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get();
        io.fail_next_put_persistently();
        append(&store, region_id, "a2").await.unwrap();
        append(&store, region_id, "a3").await.unwrap();
        timeout(WAIT, store.wait_durable(&provider(region_id), id(3, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        assert_eq!(
            rejected,
            METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get()
        );
    }

    #[tokio::test]
    async fn test_store_classifies_object_store_errors_by_kind() {
        let refusing = RefusingStore::new();
        let io = ObjectStoreIo::new(refusing.store.clone(), PREFIX).unwrap();
        let content = Bytes::from_static(b"wal");
        // Behind the retry layer every refusal is persistent, whatever its
        // kind, so only the kind tells what a repeat can settle: denied
        // access, a missing S3 bucket, or a write to a missing bucket of
        // another service cannot; an HTTP 400 or a 5xx that outlasted the
        // retries may pass.
        for (kind, requires) in [
            (ErrorKind::PermissionDenied, true),
            (ErrorKind::ConfigInvalid, true),
            (ErrorKind::NotFound, true),
            (ErrorKind::Unexpected, false),
        ] {
            refusing.refuse_writes(Some(kind));
            let error = io.put_if_absent(1, content.clone()).await.unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { operation: "write", error, .. }
                    if error.kind() == kind && error.is_persistent()),
                "unexpected error for {kind:?}: {error:?}"
            );
            assert_eq!(requires, requires_intervention(&error), "{kind:?}");
        }

        // The object exists, so the create is refused by its condition and
        // the read that reconciles it decides: denied access cannot pass, an
        // object that is gone is created by the repeat.
        refusing.refuse_writes(None);
        io.put_if_absent(1, content.clone()).await.unwrap();
        for (kind, requires) in [
            (ErrorKind::PermissionDenied, true),
            (ErrorKind::NotFound, false),
        ] {
            refusing.refuse_reads(Some(kind));
            let error = io
                .put_if_absent(1, Bytes::from_static(b"other"))
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { operation: "read", error, .. }
                    if error.kind() == kind && error.is_persistent()),
                "unexpected error for {kind:?}: {error:?}"
            );
            assert_eq!(requires, requires_intervention(&error), "{kind:?}");
        }
        assert!(!requires_intervention(
            &WalObjectConflictSnafu {
                path: io.object_path(1)
            }
            .build()
        ));
    }

    #[test]
    fn test_store_failing_delay_doubles_up_to_the_cap() {
        let now = Instant::now();
        let mut failing = Failing {
            error: Arc::new(ObjectStoreWalStoppedSnafu.build()),
            since: now,
            delay: CREATE_RETRY_DELAY,
            probe_at: None,
            failed_creates: 1,
            rejected_appends: 0,
            logged_at: now,
        };
        let mut delays = Vec::new();
        for _ in 0..12 {
            let delay = failing.delay;
            let scheduled_at = Instant::now();
            failing.schedule();
            assert!(failing.probe_at.unwrap() >= scheduled_at + delay);
            delays.push(delay.as_millis());
        }
        assert_eq!(
            vec![
                100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000, 30000, 30000
            ],
            delays
        );
    }

    /// Opens an `enqueued` store over a refusing object store, makes it
    /// refuse writes with `kind` and appends one entry, whose create fails,
    /// and waits until the store is failing.
    async fn open_failing(kind: ErrorKind) -> (Arc<ObjectStoreLogStore>, RefusingStore) {
        let refusing = RefusingStore::new();
        let store = open(refusing.store.clone(), &enqueued(eager())).await;
        let region_id = region(1);
        refusing.refuse_writes(Some(kind));
        let response = append(&store, region_id, "a1").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        wait_until(|| failing_stores() == 1).await;
        (store, refusing)
    }

    #[tokio::test]
    async fn test_store_enqueued_failure_requiring_intervention_keeps_the_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let region_id = region(1);
        // Denied access and a missing bucket, as S3 reports them.
        for kind in [ErrorKind::PermissionDenied, ErrorKind::ConfigInvalid] {
            let rejected = METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get();
            let (store, refusing) = open_failing(kind).await;
            let admitted = *store.admitted_appends.borrow();

            // A later append is rejected at once and takes no entry id; the
            // acknowledged entry waits in the backlog and the store is not
            // poisoned.
            let error = append(&store, region_id, "a2").await.unwrap_err();
            assert_rejected(&error, kind);
            assert_eq!(admitted, *store.admitted_appends.borrow());
            assert_eq!(
                rejected + 1,
                METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get()
            );
            let wait = {
                let store = store.clone();
                tokio::spawn(
                    async move { store.wait_durable(&provider(region_id), id(1, 1)).await },
                )
            };
            timeout(WAIT, store.wait_for_durability_waits(1))
                .await
                .unwrap()
                .unwrap();
            assert!(!wait.is_finished());
            assert!(terminal(&store.terminal_error).is_none());
            assert_eq!(0, latest(&store, region_id));
            assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

            // Once the object store accepts writes again, the backlog is
            // uploaded under its sequence and admission resumes.
            refusing.refuse_writes(None);
            timeout(WAIT, wait).await.unwrap().unwrap().unwrap();
            assert_eq!(0, failing_stores());
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1")]),
                read_entries(&store, region_id, 1).await
            );
            let response = append(&store, region_id, "a3").await.unwrap();
            assert_eq!(
                HashMap::from([(region_id, id(2, 1))]),
                response.last_entry_ids
            );
            timeout(WAIT, store.wait_durable(&provider(region_id), id(2, 1)))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
            store.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_enqueued_create_retries_back_off() {
        let _serialized = SERIALIZED.lock().await;
        let (store, refusing) = open_failing(ErrorKind::PermissionDenied).await;
        let refused = || refusing.refused_writes.lock().unwrap().clone();
        wait_until(|| refused().len() >= 5).await;

        // Each attempt starts at least the doubling delay after the previous
        // one started, so the 1.6 s from the first refusal hold at most five
        // attempts, where a repeat every 100 ms would make sixteen.
        let refused = refused();
        for (index, delay) in [100, 200, 400, 800].into_iter().enumerate() {
            let gap = refused[index + 1] - refused[index];
            assert!(
                gap >= Duration::from_millis(delay),
                "attempt {} started {gap:?} after the one before",
                index + 1
            );
        }
        let window = refused[0] + Duration::from_millis(1600);
        assert!(refused.iter().filter(|at| **at < window).count() <= 5);
        assert_eq!(1, failing_stores());

        refusing.refuse_writes(None);
        timeout(WAIT, store.wait_durable(&provider(region(1)), id(1, 1)))
            .await
            .unwrap()
            .unwrap();
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_enqueued_failing_store_attempts_the_lowest_batch_alone() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) =
            open_parking_creates(memory_store(), &enqueued(eager())).await;
        let region_id = region(1);
        for append in spawn_appends(&store, region_id, 3).await {
            timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        }
        let mut releases = parked_operations(&mut parked, 3).await;
        io.creates_refused.store(true, Ordering::SeqCst);
        for object_seq in [2, 1, 3] {
            releases.remove(&object_seq).unwrap().send(false).unwrap();
        }

        for _ in 0..2 {
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            assert_eq!(1, io.in_flight.load(Ordering::SeqCst));
            release.send(false).unwrap();
            assert!(parked.try_recv().is_err());
        }

        // The attempt that succeeds ends the failing state: the other
        // batches are created together and everything is acknowledged in
        // order.
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(1, object_seq);
        release.send(true).unwrap();
        for release in parked_operations(&mut parked, 2).await.into_values() {
            release.send(true).unwrap();
        }
        timeout(WAIT, store.wait_durable(&provider(region_id), id(3, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(0, failing_stores());
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(
                region_id,
                &[(id(1, 1), "a1"), (id(2, 1), "a2"), (id(3, 1), "a3")]
            ),
            read_entries(&store, region_id, 1).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_enqueued_rejects_the_appends_held_back_by_a_refused_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) =
            open_parking_creates(memory_store(), &enqueued(eager())).await;
        let region_id = region(1);
        let stalled = METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get();
        let stalled_waits = METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.get_sample_count();
        let rejected = METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get();

        // The sealed batches are full: the next append is held back and the
        // ones after it wait in their channel.
        for append in spawn_appends(&store, region_id, MAX_SEALED_BATCHES - 1).await {
            timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        }
        let held = (0..3)
            .map(|index| {
                spawn_append_batch(&store, vec![entry(&store, region_id, &format!("h{index}"))])
            })
            .collect::<Vec<_>>();
        let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
        wait_until(|| METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get() == stalled + 1).await;
        assert!(held.iter().all(|append| !append.is_finished()));

        // The object store denies access: every append waiting is rejected
        // instead of hanging, and so is a later one.
        io.creates_refused.store(true, Ordering::SeqCst);
        for release in releases.drain().map(|(_, release)| release) {
            release.send(false).unwrap();
        }
        for append in held {
            let error = timeout(WAIT, append).await.unwrap().unwrap().unwrap_err();
            assert_rejected(&error, ErrorKind::PermissionDenied);
        }
        assert_rejected(
            &append(&store, region_id, "a8").await.unwrap_err(),
            ErrorKind::PermissionDenied,
        );
        assert_eq!(
            rejected + 4,
            METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get()
        );
        assert_eq!(
            stalled_waits + 1,
            METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.get_sample_count()
        );

        // Once a create succeeds the backlog drains and the next append
        // takes the sequence after it.
        io.creates_refused.store(false, Ordering::SeqCst);
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(1, object_seq);
        release.send(true).unwrap();
        for _ in 2..MAX_SEALED_BATCHES {
            next_parked_operation(&mut parked)
                .await
                .1
                .send(true)
                .unwrap();
        }
        let last = id(MAX_SEALED_BATCHES as u64 - 1, 1);
        timeout(WAIT, store.wait_durable(&provider(region_id), last))
            .await
            .unwrap()
            .unwrap();
        let appended = spawn_append_batch(&store, vec![entry(&store, region_id, "a9")]);
        next_parked_operation(&mut parked)
            .await
            .1
            .send(true)
            .unwrap();
        let response = timeout(WAIT, appended).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(MAX_SEALED_BATCHES as u64, 1))]),
            response.last_entry_ids
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_uploads_a_refused_backlog_at_once() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) =
            open_parking_creates(memory_store(), &enqueued(eager())).await;
        let region_id = region(1);
        for append in spawn_appends(&store, region_id, 3).await {
            timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        }
        io.creates_refused.store(true, Ordering::SeqCst);
        for release in parked_operations(&mut parked, 3).await.into_values() {
            release.send(false).unwrap();
        }
        wait_until(|| failing_stores() == 1).await;

        // Stop does not wait for the next attempt of the failing store: it
        // starts the creates of the whole backlog together, which succeed
        // now.
        let stop = begin_spawned_stop(&store).await;
        let releases = parked_operations(&mut parked, 3).await;
        for object_seq in 1..=3 {
            assert!(releases.contains_key(&object_seq));
        }
        for release in releases.into_values() {
            release.send(true).unwrap();
        }
        timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
        assert_eq!(0, failing_stores());
        assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);
        assert_eq!(id(3, 1), latest(&store, region_id));
    }

    #[tokio::test]
    async fn test_store_enqueued_conflict_poisons() {
        let _serialized = SERIALIZED.lock().await;
        // A conflict of any epoch poisons the store without reading the
        // existing object, since the acknowledged entries cannot move to
        // another sequence.
        for foreign_epoch in [0, 2] {
            let object_store = memory_store();
            let (io, reads) = RecordingIo::over(object_store.clone());
            let store = open_over(io.clone(), &enqueued(eager())).await;
            let region_id = region(1);
            put_foreign(&object_store, 1, foreign_epoch).await;
            let assert_poisoned = |error: Error| {
                assert!(
                    matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                    "unexpected error: {error:?}"
                );
            };

            // The append was acknowledged; the failure surfaces afterwards.
            let second = entry(&store, region_id, "a2");
            append(&store, region_id, "a1").await.unwrap();
            let error = timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
                .await
                .unwrap()
                .unwrap_err();
            assert_poisoned(error);
            assert_poisoned(store.append_batch(vec![second]).await.unwrap_err());
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            // The acknowledged entry was dropped, which stop reports.
            assert_poisoned(store.stop().await.unwrap_err());
            assert!(reads.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_reports_a_backlog_lost_before_the_stop_command() {
        let _serialized = SERIALIZED.lock().await;
        let (store, io, mut parked) =
            open_parking_creates(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        let seal = {
            let store = store.clone();
            tokio::spawn(async move { store.seal_open_batch().await })
        };
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(1, object_seq);

        // Stop began, but the actor has not received the stop command when
        // the create fails: the backlog is dropped and the failure is kept
        // for the stop that follows.
        store.raise_stopped_flag();
        release.send(false).unwrap();
        assert_stopped(&timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err());
        // The lost entry cannot be certified as durable before the stop
        // command is handled; a durable id still is.
        let error = timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
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
        assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        assert!(parked.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_store_enqueued_stop_uploads_the_backlog() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();
        assert_eq!(vec![0], object_seqs(store.io.as_ref()).await);

        store.stop().await.unwrap();
        assert_eq!(vec![0, 1], object_seqs(store.io.as_ref()).await);
        assert_eq!(id(1, 2), latest(&store, region_id));
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a1"), (id(1, 2), "a2")]),
            read_entries(&store, region_id, 1).await
        );

        // A backlog that cannot be uploaded is dropped and reported by stop,
        // whether the failure is temporary, persistent or permanent; none
        // poisons the store.
        for failure in ["temporary", "persistent", "permanent"] {
            let (io, _) = RecordingIo::over(memory_store());
            let store = open_over(io.clone(), &enqueued(manual())).await;
            append(&store, region_id, "a1").await.unwrap();
            match failure {
                "temporary" => store.fail_creates(),
                "persistent" => io.fail_next_put_persistently(),
                _ => io.fail_next_put_permanently(),
            }
            let error = store.stop().await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert!(terminal(&store.terminal_error).is_none());
            store.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_enqueued_issued_id_needs_no_floor_before_it_is_durable() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &enqueued(manual())).await;
        let region_id = region(1);
        append(&store, region_id, "a1").await.unwrap();

        // The open batch holds id(1, 1) under the next sequence. The id was
        // handed out, so it is accepted without a floor, with the watermark
        // capped to what is durable; an id the store never handed out under
        // that sequence is refused.
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        assert_eq!(
            Some(&0),
            store.obsolete_entry_ids.lock().unwrap().get(&region_id)
        );
        let error = store
            .obsolete(&provider(region_id), region_id, id(1, 2))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::WalObjectSequenceUnsettled { object_seq: 1, .. }
            ),
            "unexpected error: {error:?}"
        );

        store.seal_open_batch().await.unwrap();
        assert_eq!(
            expected_entries(region_id, &[(id(1, 1), "a1")]),
            read_entries(&store, region_id, 0).await
        );
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        assert!(read_entries(&store, region_id, 0).await.is_empty());
        let response = append(&store, region_id, "a2").await.unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(2, 1))]),
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
        // stays at zero, but its ids must still start above the watermark:
        // object 6 marks the floor.
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
            HashMap::from([(region_id, id(7, 1))]),
            response.last_entry_ids
        );
        store.seal_open_batch().await.unwrap();
        timeout(WAIT, store.wait_durable(&provider(region_id), id(7, 1)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id(7, 1), latest(&store, region_id));
        store.stop().await.unwrap();

        // Replay from the watermark sees the entry.
        let store = open(object_store, &enqueued(manual())).await;
        assert_eq!(
            expected_entries(region_id, &[(id(7, 1), "r1")]),
            read_entries(&store, region_id, id(5, 1) + 1).await
        );
    }

    #[tokio::test]
    async fn test_store_stop_uploads_the_open_batch_of_pending_waits() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &manual()).await;
        let region_id = region(1);
        let append = spawn_append_batch(&store, vec![entry(&store, region_id, "a1")]);
        store.wait_for_admitted_appends(1).await.unwrap();
        let wait = {
            let store = store.clone();
            tokio::spawn(async move { store.wait_durable(&provider(region_id), id(1, 1)).await })
        };
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!wait.is_finished());

        // Stop seals the open batch and uploads it before it returns.
        store.stop().await.unwrap();
        let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        assert_eq!(
            HashMap::from([(region_id, id(1, 1))]),
            response.last_entry_ids
        );
        timeout(WAIT, wait).await.unwrap().unwrap().unwrap();
        assert_eq!(id(1, 1), latest(&store, region_id));
    }

    /// The reclaim boundary in the header of the object `object_seq`.
    async fn reclaim_boundary_of(io: &dyn WalObjectIo, object_seq: u64) -> u64 {
        decode_header(&io.get(object_seq).await.unwrap())
            .unwrap()
            .reclaim_boundary
    }

    #[tokio::test]
    async fn test_store_stamps_the_reclaim_boundary_and_collects_below_it() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let io = store.io.clone();
        let (one, two, three) = (region(1), region(2), region(3));
        let deleted = METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get();

        // Object 1 carries the boundary past the indexed start object, which
        // is collected; region one then holds the boundary at object 1.
        append(&store, one, "a1").await.unwrap();
        append(&store, two, "b1").await.unwrap();
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
        assert_eq!(1, reclaim_boundary_of(io.as_ref(), 1).await);
        assert_eq!(1, reclaim_boundary_of(io.as_ref(), 2).await);

        // A watermark moves the boundary; the next indexed object makes it
        // durable.
        store.obsolete(&provider(one), one, id(1, 1)).await.unwrap();
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
        append(&store, one, "a2").await.unwrap();
        assert_eq!(2, reclaim_boundary_of(io.as_ref(), 3).await);
        assert_eq!(vec![2, 3], collected_object_seqs(&store).await);

        // Region two flushed through object 2 and region one released: the
        // object of the release record, 4, carries the sequence after the
        // last indexed object, and so does the next one.
        store.obsolete(&provider(two), two, id(2, 1)).await.unwrap();
        store.obsolete_all(&provider(one), one).await.unwrap();
        assert_eq!(4, reclaim_boundary_of(io.as_ref(), 4).await);
        assert_eq!(vec![4], collected_object_seqs(&store).await);
        append(&store, two, "b2").await.unwrap();
        assert_eq!(5, reclaim_boundary_of(io.as_ref(), 5).await);
        assert_eq!(vec![5], collected_object_seqs(&store).await);

        // A region that appears later does not move the boundary back.
        append(&store, three, "c1").await.unwrap();
        assert_eq!(5, reclaim_boundary_of(io.as_ref(), 6).await);
        assert_eq!(vec![5, 6], collected_object_seqs(&store).await);
        // Other stores of the process count their deletes as well.
        assert!(METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get() >= deleted + 5);
        assert_eq!(
            expected_entries(two, &[(id(5, 1), "b2")]),
            read_entries(&store, two, 0).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_attempts_a_failed_delete_again_at_the_next_collection() {
        let _serialized = SERIALIZED.lock().await;
        let (io, mut parked) = ParkedIo::parking_creates(memory_store());
        let store = open_over(io.clone(), &eager()).await;
        io.deletes_parked.store(true, Ordering::SeqCst);
        let region_id = region(1);
        let failed = METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get();
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        release.send(false).unwrap();

        // The object stays indexed, and an indexing that leaves the durable
        // boundary where it is starts no collection.
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert_eq!(
            failed + 1,
            METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get()
        );
        assert!(store.catalog.read().unwrap().contains_object(0));
        append(&store, region_id, "a3").await.unwrap();
        assert!(parked.try_recv().is_err());
        assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);

        // The next `obsolete` starts a collection, which deletes it again.
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        let (object_seq, release) = next_parked_operation(&mut parked).await;
        assert_eq!(0, object_seq);
        release.send(true).unwrap();
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        assert!(!store.catalog.read().unwrap().contains_object(0));
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_collection_ends_with_stop_and_poison() {
        let _serialized = SERIALIZED.lock().await;
        for poison in [false, true] {
            let object_store = memory_store();
            let (io, mut parked) = ParkedIo::parking_creates(object_store.clone());
            let store = open_over(io.clone(), &eager()).await;
            io.deletes_parked.store(true, Ordering::SeqCst);
            let region_id = region(1);
            for index in 1..=5 {
                append(&store, region_id, &format!("a{index}"))
                    .await
                    .unwrap();
            }
            store
                .obsolete(&provider(region_id), region_id, id(5, 1))
                .await
                .unwrap();
            append(&store, region_id, "a6").await.unwrap();
            // Objects 0 to 5 are below the durable boundary; four are deleted
            // at a time.
            let releases = parked_operations(&mut parked, MAX_IN_FLIGHT_DELETES).await;
            assert!(parked.try_recv().is_err());

            let mut stop = None;
            if poison {
                put_foreign(&object_store, 7, 100).await;
                append(&store, region_id, "a7").await.unwrap_err();
            } else {
                // The first poll queues the stop command; the actor refuses
                // the release of `obsolete_all` queued after it only once it
                // handled the stop.
                let mut stopping = Box::pin(store.stop());
                assert!((&mut stopping).now_or_never().is_none());
                assert_stopped(
                    &store
                        .obsolete_all(&provider(region_id), region_id)
                        .await
                        .unwrap_err(),
                );
                assert!((&mut stopping).now_or_never().is_none());
                stop = Some(stopping);
            }
            for release in releases.into_values() {
                release.send(true).unwrap();
            }
            match stop {
                Some(stop) => timeout(WAIT, stop).await.unwrap().unwrap(),
                None => timeout(WAIT, store.wait_for_garbage_collection())
                    .await
                    .unwrap(),
            }

            // The deletes in flight completed and no other started.
            assert!(parked.try_recv().is_err());
            assert_eq!(
                MAX_IN_FLIGHT_DELETES,
                io.max_in_flight.load(Ordering::SeqCst)
            );
            let mut expected = vec![4, 5, 6];
            if poison {
                expected.push(7);
            }
            assert_eq!(expected, object_seqs(io.as_ref()).await);
        }
    }

    #[tokio::test]
    async fn test_store_drops_the_holes_of_a_collected_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        write_corrupted_segment(&object_store).await;
        let store = open(object_store, &eager()).await;
        read_entries(&store, region(2), 0).await;
        assert_eq!(1, store.wal_holes(&provider(region(2))).unwrap().len());

        store
            .obsolete(&provider(region(1)), region(1), 1)
            .await
            .unwrap();
        store
            .obsolete(&provider(region(2)), region(2), 10)
            .await
            .unwrap();
        append(&store, region(1), "a").await.unwrap();
        assert_eq!(vec![11], collected_object_seqs(&store).await);
        assert!(store.wal_holes(&provider(region(2))).unwrap().is_empty());
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_records_no_hole_for_a_collected_object() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (_, footer) = write_corrupted_segment(&object_store).await;
        let store = open(object_store, &eager()).await;
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();

        // A read met the damaged segment of object 0, which a collection
        // removed before the read recorded the hole.
        store.catalog.write().unwrap().remove_object(0);
        let error = CorruptedWalObjectSnafu { reason: "damaged" }.build();
        record_skipped_segment(
            &store.catalog,
            &store.wal_holes,
            store.io.as_ref(),
            0,
            &footer[1],
            &error,
        );
        assert!(store.wal_holes(&provider(region(2))).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
        store.stop().await.unwrap();
    }

    async fn recover_listed(io: &ObjectStoreIo, listed: Vec<ListedObject>) -> Result<Recovered> {
        finish_recovery(fetch_footers(io, listed, RECOVERY_CONCURRENCY).await?)
    }

    #[tokio::test]
    async fn test_store_recovery_tolerates_objects_collected_after_the_listing() {
        let _serialized = SERIALIZED.lock().await;
        let io = ObjectStoreIo::new(memory_store(), PREFIX).unwrap();
        let bounded = |object_seq, prev, reclaim_boundary| Header {
            reclaim_boundary,
            ..chain_header(object_seq, 1, prev)
        };
        put_object_with_header(&io, chain_header(0, 1, None), &[]).await;
        put_object_with_header(&io, chain_header(1, 1, Some((0, 1))), &[]).await;
        put_object_with_header(&io, bounded(2, Some((1, 1)), 2), &[]).await;
        put_object_with_header(&io, bounded(3, Some((2, 1)), 2), &[]).await;
        let listed = io.list().await.unwrap();

        // A delete of an earlier instance removes object 1, below the
        // boundary of the tip, after the listing.
        io.delete(1).await.unwrap();
        let recovered = recover_listed(&io, listed.clone()).await.unwrap();
        assert_eq!(Some(3), recovered.tip.map(|tip| tip.object_seq));
        assert_eq!(2, recovered.reclaim_boundary);
        assert_eq!(vec![0], recovered.unindexed);
        assert_eq!(4, recovered.next_object_seq);

        // Object 2 is needed above the boundary: its disappearance fails
        // recovery.
        io.delete(2).await.unwrap();
        let error = recover_listed(&io, listed).await.unwrap_err();
        assert!(is_not_found(&error), "unexpected error: {error:?}");
    }

    #[tokio::test]
    async fn test_store_read_skips_an_object_collected_after_it_was_listed() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let region_id = region(1);
        let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();
        append(&store, region_id, "a1").await.unwrap();
        append(&store, region_id, "a2").await.unwrap();

        // The read lists objects 1 and 2 before a collection deletes object 1.
        let stream = store.read(&provider(region_id), 0).await.unwrap();
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        append(&store, region_id, "a3").await.unwrap();
        assert_eq!(vec![2, 3], collected_object_seqs(&store).await);

        let entries = stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(expected_entries(region_id, &[(id(2, 1), "a2")]), entries);
        assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
        assert_eq!(
            skipped,
            METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_read_waits_for_an_undecided_delete() {
        let _serialized = SERIALIZED.lock().await;
        for deleted in [true, false] {
            let object_store = memory_store();
            let (io, mut parked) = ParkedIo::parking_creates(object_store.clone());
            let store = open_over(io.clone(), &eager()).await;
            io.deletes_parked.store(true, Ordering::SeqCst);
            let region_id = region(1);
            let skipped = METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get();
            append(&store, region_id, "a1").await.unwrap();
            next_parked_operation(&mut parked)
                .await
                .1
                .send(true)
                .unwrap();
            append(&store, region_id, "a2").await.unwrap();

            // The read lists objects 1 and 2. The delete of object 1 removes
            // it, but its outcome is not reported yet.
            let stream = store.read(&provider(region_id), 0).await.unwrap();
            store
                .obsolete(&provider(region_id), region_id, id(1, 1))
                .await
                .unwrap();
            append(&store, region_id, "a3").await.unwrap();
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            object_store
                .delete(&object_path(&object_store, 1))
                .await
                .unwrap();
            let read = tokio::spawn(stream.try_collect::<Vec<_>>());
            let waiting = || {
                store
                    .deleting
                    .lock()
                    .get(&1)
                    .map_or(0, |settled| settled.receiver_count())
            };
            timeout(WAIT, async {
                while waiting() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!read.is_finished());

            // A delete that succeeded skips the object; one that failed
            // leaves it indexed, and the read fails on the fetch.
            release.send(deleted).unwrap();
            let result = timeout(WAIT, read).await.unwrap().unwrap();
            if deleted {
                let entries = result.unwrap().into_iter().flatten().collect::<Vec<_>>();
                assert_eq!(expected_entries(region_id, &[(id(2, 1), "a2")]), entries);
            } else {
                let error = result.unwrap_err();
                assert!(is_not_found(&error), "unexpected error: {error:?}");
            }
            assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
            assert_eq!(
                skipped,
                METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get()
            );
            store.stop().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_store_recovers_from_the_reclaim_boundary_of_the_tip() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let region_id = region(1);
        let store = open(object_store.clone(), &eager()).await;
        append(&store, region_id, "a1").await.unwrap();
        store
            .obsolete(&provider(region_id), region_id, id(1, 1))
            .await
            .unwrap();
        append(&store, region_id, "a2").await.unwrap();
        assert_eq!(vec![2], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        // Object 2 links to object 1, below its boundary. The start object,
        // 11, carries the recovered boundary.
        let store = open(object_store.clone(), &eager()).await;
        assert_eq!(2, reclaim_boundary_of(&io, 11).await);
        assert_eq!(
            expected_entries(region_id, &[(id(2, 1), "a2")]),
            read_entries(&store, region_id, 0).await
        );
        store
            .obsolete(&provider(region_id), region_id, id(2, 1))
            .await
            .unwrap();
        append(&store, region_id, "a3").await.unwrap();
        assert_eq!(vec![12], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        let store = open(object_store, &eager()).await;
        assert_eq!(12, reclaim_boundary_of(&io, 21).await);
        assert_eq!(
            expected_entries(region_id, &[(id(12, 1), "a3")]),
            read_entries(&store, region_id, 0).await
        );
        assert_eq!(vec![12, 21], object_seqs(&io).await);
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_collects_a_late_object_of_an_earlier_epoch_below_the_boundary() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let region_id = region(1);
        let record = |entry_id, data: &'static str| Record {
            region_id,
            entry_id,
            payload: Bytes::from_static(data.as_bytes()),
        };
        let bounded = |object_seq, epoch, prev, reclaim_boundary| Header {
            reclaim_boundary,
            ..chain_header(object_seq, epoch, prev)
        };
        put_object_with_header(&io, chain_header(0, 1, None), &[]).await;
        put_object_with_header(
            &io,
            chain_header(1, 1, Some((0, 1))),
            &[record(id(1, 1), "a")],
        )
        .await;
        // Epoch 4 started at object 3, and object 4 carries boundary 4.
        // Object 2 of epoch 1 landed late; its link holds, but its epoch is
        // lower.
        put_object_with_header(
            &io,
            bounded(2, 1, Some((1, 1)), 2),
            &[record(id(2, 1), "late")],
        )
        .await;
        put_object_with_header(&io, chain_header(3, 4, Some((1, 1))), &[]).await;
        put_object_with_header(
            &io,
            bounded(4, 4, Some((3, 4)), 4),
            &[record(id(4, 1), "b")],
        )
        .await;
        let recovered = recover(&io).await.unwrap();
        assert_eq!(Some(4), recovered.tip.map(|tip| tip.object_seq));
        assert_eq!(4, recovered.reclaim_boundary);
        assert_eq!(vec![0, 1, 2, 3], recovered.unindexed);

        // The start object makes boundary 4 durable, and the first
        // collection deletes everything below it.
        let store = open(object_store, &eager()).await;
        assert_eq!(
            expected_entries(region_id, &[(id(4, 1), "b")]),
            read_entries(&store, region_id, 0).await
        );
        store
            .obsolete(&provider(region_id), region_id, id(4, 1))
            .await
            .unwrap();
        assert_eq!(vec![4, 13], collected_object_seqs(&store).await);
        store.stop().await.unwrap();
    }

    /// An earlier instance can have handed out every sequence up to
    /// [`UNLISTED_SEQUENCES`] above the newest object a recovery lists. Its
    /// late object at any of them, listed by the recovery or landing after
    /// the start object, never meets a sequence this instance assigns, and
    /// becomes history only if the recovery found its link complete.
    #[tokio::test]
    async fn test_store_starts_above_the_sequences_an_earlier_instance_handed_out() {
        let _serialized = SERIALIZED.lock().await;
        let region_id = region(1);
        let record = |entry_id, data: &str| Record {
            region_id,
            entry_id,
            payload: Bytes::from(data.as_bytes().to_vec()),
        };
        for late_seq in 2..=1 + UNLISTED_SEQUENCES {
            for listed in [true, false] {
                let object_store = memory_store();
                let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
                put_object_with_header(&io, chain_header(0, 1, None), &[]).await;
                put_object_with_header(
                    &io,
                    chain_header(1, 1, Some((0, 1))),
                    &[record(id(1, 1), "a1")],
                )
                .await;
                // The late object links to its pipeline predecessor, which
                // never landed unless it is object 1.
                let late = || chain_header(late_seq, 1, Some((late_seq - 1, 1)));
                let late_record = [record(id(late_seq, 1), "late")];
                if listed {
                    put_object_with_header(&io, late(), &late_record).await;
                }
                let store = open(object_store.clone(), &eager()).await;
                let newest = if listed { late_seq } else { 1 };
                let start_seq = newest + 1 + UNLISTED_SEQUENCES;
                if !listed {
                    put_object_with_header(&io, late(), &late_record).await;
                }
                // The store writes past every sequence below its start
                // object without meeting the late object.
                let writes = UNLISTED_SEQUENCES + 2;
                for index in 1..=writes {
                    append(&store, region_id, &format!("b{index}"))
                        .await
                        .unwrap();
                }
                assert_eq!(id(start_seq + writes, 1), latest(&store, region_id));
                store.stop().await.unwrap();

                let mut expected = vec![(id(1, 1), "a1".to_string())];
                if listed && late_seq == 2 {
                    expected.push((id(2, 1), "late".to_string()));
                }
                expected.extend(
                    (1..=writes).map(|index| (id(start_seq + index, 1), format!("b{index}"))),
                );
                let expected = expected
                    .iter()
                    .map(|(entry_id, data)| (*entry_id, data.as_str()))
                    .collect::<Vec<_>>();
                let store = open(object_store.clone(), &eager()).await;
                assert_eq!(
                    expected_entries(region_id, &expected),
                    read_entries(&store, region_id, 0).await,
                    "late object {late_seq}, listed: {listed}"
                );
                // Once the region is flushed through its last entry, the next
                // object carries a boundary past every older object, and the
                // late object is collected with the rest.
                store
                    .obsolete(&provider(region_id), region_id, id(start_seq + writes, 1))
                    .await
                    .unwrap();
                append(&store, region(2), "c1").await.unwrap();
                let left = collected_object_seqs(&store).await;
                assert!(!left.contains(&late_seq), "{left:?}");
                store.stop().await.unwrap();
            }
        }
    }

    /// An instance holds at most [`UNLISTED_SEQUENCES`] sealed batches that
    /// are not durable, releases included: here seven of appends and one of a
    /// release, which leave room for nothing more. Their objects land after
    /// the next instance opened on the objects listed before, and none
    /// meets a sequence of that instance.
    #[tokio::test]
    async fn test_store_earlier_instance_with_every_batch_outstanding() {
        let _serialized = SERIALIZED.lock().await;
        assert_eq!(MAX_SEALED_BATCHES as u64, UNLISTED_SEQUENCES);
        let object_store = memory_store();
        // Every append seals its own batch.
        let (earlier, io, mut parked) = open_parking_creates(object_store.clone(), &eager()).await;
        let writer = region(1);
        let dropped = region(2);
        let mut appends = spawn_appends(&earlier, dropped, 1).await;
        appends.extend(spawn_appends(&earlier, writer, MAX_SEALED_BATCHES - 2).await);
        let release = spawn_release(&earlier, dropped);
        let held_back = spawn_append_batch(&earlier, vec![entry(&earlier, writer, "held")]);
        timeout(WAIT, earlier.wait_for_held_back_appends(1))
            .await
            .unwrap()
            .unwrap();
        let mut creates = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;

        // The next instance lists only the start object of the earlier one.
        let later = open(object_store.clone(), &eager()).await;
        let start_seq = 1 + UNLISTED_SEQUENCES;
        // Every sealed object of the earlier instance lands now, the last one
        // with the release record.
        for object_seq in 1..=UNLISTED_SEQUENCES {
            while !creates.contains_key(&object_seq) {
                let (parked_seq, release) = next_parked_operation(&mut parked).await;
                creates.insert(parked_seq, release);
            }
            creates.remove(&object_seq).unwrap().send(true).unwrap();
        }
        timeout(WAIT, release).await.unwrap().unwrap().unwrap();
        for append in appends {
            timeout(WAIT, append).await.unwrap().unwrap().unwrap();
        }
        assert_eq!(
            vec![Release {
                region_id: dropped,
                entry_id: id(1, 1),
            }],
            releases_of(io.as_ref(), UNLISTED_SEQUENCES).await
        );
        // The held-back append now takes the start sequence of the next
        // instance, whose create would meet that start object; the earlier
        // instance dies before it lands.
        while !creates.contains_key(&start_seq) {
            let (parked_seq, release) = next_parked_operation(&mut parked).await;
            creates.insert(parked_seq, release);
        }
        assert_eq!(vec![start_seq], creates.keys().copied().collect::<Vec<_>>());
        earlier.crash().await;
        timeout(WAIT, held_back)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        append(&later, writer, "b1").await.unwrap();
        append(&later, writer, "b2").await.unwrap();
        assert_eq!(id(start_seq + 2, 1), latest(&later, writer));
        later.stop().await.unwrap();

        // A recovery replays the later instance's history alone.
        let store = open(object_store, &eager()).await;
        assert_eq!(
            expected_entries(
                writer,
                &[(id(start_seq + 1, 1), "b1"), (id(start_seq + 2, 1), "b2")]
            ),
            read_entries(&store, writer, 0).await
        );
        store.stop().await.unwrap();
    }

    /// A late object of an earlier instance that lands at a skipped sequence
    /// after a collection deleted that absent sequence is collected by a
    /// later collection, however long after the open it lands.
    #[tokio::test]
    async fn test_store_collects_a_late_object_landing_after_its_absent_delete() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        put_object_with_header(&io, chain_header(0, 1, None), &[]).await;
        let store = open(object_store.clone(), &eager()).await;
        let start_seq = 1 + UNLISTED_SEQUENCES;
        let writer = region(1);
        let other = region(2);

        // Once region 1 is flushed through its entry, the next object carries
        // a boundary past every older object, the absent skipped ones
        // included.
        append(&store, writer, "a1").await.unwrap();
        store
            .obsolete(&provider(writer), writer, id(start_seq + 1, 1))
            .await
            .unwrap();
        append(&store, other, "b1").await.unwrap();
        assert_eq!(vec![start_seq + 2], collected_object_seqs(&store).await);

        // Objects of the earlier instance land at skipped sequences, one after
        // the other, each once the interval of the rechecks reached its
        // longest, and a collection after the next recheck deletes each.
        for late_seq in [5, 3, 8] {
            tokio::time::sleep(MAX_LATE_OBJECT_RECHECK * 2).await;
            put_object_with_header(
                &io,
                chain_header(late_seq, 1, Some((late_seq - 1, 1))),
                &[Record {
                    region_id: writer,
                    entry_id: id(late_seq, 1),
                    payload: Bytes::from_static(b"late"),
                }],
            )
            .await;
            tokio::time::sleep(MAX_LATE_OBJECT_RECHECK * 2).await;
            store.obsolete(&provider(other), other, 0).await.unwrap();
            assert_eq!(vec![start_seq + 2], collected_object_seqs(&store).await);
        }
        store.stop().await.unwrap();
    }

    /// The sequences an instance handed out stay within
    /// [`UNLISTED_SEQUENCES`] of its newest durable object also with an open
    /// batch that holds ids beside seven sealed batches, which an append that
    /// exhausts the positions of the open batch leaves. The next instance
    /// starts above that open batch, whose object lands late without meeting
    /// any sequence of that instance.
    #[tokio::test]
    async fn test_store_earlier_instance_with_an_open_batch_beside_seven_sealed() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (earlier, _io, mut parked) =
            open_parking_creates(object_store.clone(), &manual()).await;
        let writer = region(1);
        let mut seals = Vec::new();
        for index in 1..MAX_SEALED_BATCHES - 1 {
            earlier
                .append_batch(vec![entry(&earlier, writer, &format!("a{index}"))])
                .await
                .unwrap();
            let sealing = earlier.clone();
            seals.push(tokio::spawn(async move { sealing.seal_open_batch().await }));
            timeout(WAIT, async {
                while earlier.sealed_batches().await.unwrap() < index {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        // The open batch holds the ids of object 7; an append that would
        // exhaust its positions seals it and takes ids of object 8.
        earlier
            .append_batch(vec![entry(&earlier, writer, "a7")])
            .await
            .unwrap();
        let exhausting = (1..POSITION_LIMIT)
            .map(|_| entry(&earlier, writer, ""))
            .collect::<Vec<_>>();
        let response = earlier.append_batch(exhausting).await.unwrap();
        assert_eq!(
            Some(&id(UNLISTED_SEQUENCES, POSITION_LIMIT - 1)),
            response.last_entry_ids.get(&writer)
        );
        assert_eq!(
            MAX_SEALED_BATCHES - 1,
            earlier.sealed_batches().await.unwrap()
        );

        // The next instance lists the start object of the earlier one only.
        let later = open(object_store.clone(), &eager()).await;
        let start_seq = 1 + UNLISTED_SEQUENCES;
        // Every object of the earlier instance lands now, the open batch's
        // last.
        let mut creates = HashMap::new();
        seals.push({
            let sealing = earlier.clone();
            tokio::spawn(async move { sealing.seal_open_batch().await })
        });
        for object_seq in 1..=UNLISTED_SEQUENCES {
            while !creates.contains_key(&object_seq) {
                let (parked_seq, release) = next_parked_operation(&mut parked).await;
                creates.insert(parked_seq, release);
            }
            creates.remove(&object_seq).unwrap().send(true).unwrap();
        }
        for seal in seals {
            timeout(WAIT, seal).await.unwrap().unwrap().unwrap();
        }
        earlier.stop().await.unwrap();

        append(&later, writer, "b1").await.unwrap();
        assert_eq!(id(start_seq + 1, 1), latest(&later, writer));
        later.stop().await.unwrap();
        let store = open(object_store, &eager()).await;
        assert_eq!(
            expected_entries(writer, &[(id(start_seq + 1, 1), "b1")]),
            read_entries(&store, writer, 0).await
        );
        store.stop().await.unwrap();
    }

    /// A create that never completes does not hold the stop up: once
    /// `max_unpersisted_age` passed since the stop began, the stop drops the
    /// backlog and reports it, and every caller waiting for it is answered.
    #[tokio::test]
    async fn test_store_stop_abandons_a_create_that_never_completes() {
        let _serialized = SERIALIZED.lock().await;
        for ack_mode in [AckMode::Durable, AckMode::Enqueued] {
            let config = ObjectStoreWalConfig {
                ack_mode,
                max_unpersisted_age: Duration::from_millis(200),
                ..manual()
            };
            let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
            let region_id = region(1);
            store
                .append_batch(vec![entry(&store, region_id, "a1")])
                .await
                .unwrap();
            let seal = {
                let store = store.clone();
                tokio::spawn(async move { store.seal_open_batch().await })
            };
            let (object_seq, _never) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            let waits = [WaitKind::Barrier, WaitKind::Write]
                .map(|kind| spawn_wait(&store, region_id, id(1, 1), kind));

            let error = timeout(WAIT, store.stop()).await.unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectUnconfirmed { .. }),
                "unexpected error: {error:?}"
            );
            for wait in waits {
                timeout(WAIT, wait).await.unwrap().unwrap().unwrap_err();
            }
            timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
        }
    }

    fn spawn_release(
        store: &Arc<ObjectStoreLogStore>,
        region_id: RegionId,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let store = store.clone();
        tokio::spawn(async move { store.release_region(&provider(region_id), region_id).await })
    }

    fn release(region_id: RegionId, entry_id: EntryId) -> Release {
        Release {
            region_id,
            entry_id,
        }
    }

    /// The release records of the object `object_seq`.
    async fn releases_of(io: &dyn WalObjectIo, object_seq: u64) -> Vec<Release> {
        let decoded = decode_object(&io.get(object_seq).await.unwrap()).unwrap();
        let record = decoded
            .records
            .iter()
            .find(|record| record.region_id == RELEASE_REGION_ID)
            .unwrap();
        assert_eq!(entry_id(object_seq, 1), record.entry_id);
        decode_releases(&record.payload).unwrap()
    }

    fn watermark(store: &ObjectStoreLogStore, region_id: RegionId) -> Option<EntryId> {
        store
            .obsolete_entry_ids
            .lock()
            .unwrap()
            .get(&region_id)
            .copied()
    }

    #[tokio::test]
    async fn test_store_release_writes_a_record_of_the_largest_issued_id() {
        let _serialized = SERIALIZED.lock().await;
        let store = open(memory_store(), &eager()).await;
        let (one, two) = (region(1), region(2));
        store
            .append_batch(vec![entry(&store, one, "a1"), entry(&store, two, "b1")])
            .await
            .unwrap();

        // The release seals object 2 without any entry; its record releases
        // region one through the largest id handed out for it, and it carries
        // the boundary region two holds at object 1.
        store.release_region(&provider(one), one).await.unwrap();
        assert_eq!(
            vec![release(one, id(1, 1))],
            releases_of(store.io.as_ref(), 2).await
        );
        assert_eq!(1, reclaim_boundary_of(store.io.as_ref(), 2).await);
        assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
        assert!(read_entries(&store, one, 0).await.is_empty());
        // The record is the segment of a reserved region, which names no
        // namespace.
        assert_eq!(
            vec![provider(one), provider(two)],
            store.list_namespaces().await.unwrap()
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_recovery_seeds_watermarks_from_release_records() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (one, two) = (region(1), region(2));
        let store = open(object_store.clone(), &eager()).await;
        // Object 1 holds entries of both regions and object 2 of region one.
        // The release of region one is the last object written, and region
        // two holds the boundary at object 1.
        store
            .append_batch(vec![entry(&store, one, "a1"), entry(&store, two, "b1")])
            .await
            .unwrap();
        append(&store, one, "a2").await.unwrap();
        store.release_region(&provider(one), one).await.unwrap();
        assert_eq!(1, reclaim_boundary_of(store.io.as_ref(), 3).await);
        store.stop().await.unwrap();

        // Recovery seeds the watermark of region one, which nothing opens
        // again, and a later watermark never moves it down.
        let store = open(object_store, &eager()).await;
        assert_eq!(vec![1, 2, 3, 12], collected_object_seqs(&store).await);
        assert_eq!(Some(id(2, 1)), watermark(&store, one));
        assert!(read_entries(&store, one, 0).await.is_empty());
        store.obsolete(&provider(one), one, id(1, 1)).await.unwrap();
        assert_eq!(Some(id(2, 1)), watermark(&store, one));

        // Once region two is flushed through object 1, no object holds a
        // live segment: object 13 carries the boundary past all of them.
        store.obsolete(&provider(two), two, id(1, 1)).await.unwrap();
        append(&store, two, "b2").await.unwrap();
        assert_eq!(13, reclaim_boundary_of(store.io.as_ref(), 13).await);
        assert_eq!(vec![13], collected_object_seqs(&store).await);
        assert_eq!(
            expected_entries(two, &[(id(13, 1), "b2")]),
            read_entries(&store, two, 0).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_carries_the_record_of_a_failed_release() {
        let _serialized = SERIALIZED.lock().await;
        let region_id = region(1);
        // The release fails while the object store refuses its object, whose
        // record stays in the backlog: a create that succeeds later, or the
        // last attempt of a stop, makes it durable, and a stop whose attempt
        // fails reports it lost.
        for config in [eager(), enqueued(eager())] {
            for last_attempt in ["probe", "stop succeeds", "stop fails"] {
                let object_store = memory_store();
                let (store, io, mut parked) =
                    open_parking_creates(object_store.clone(), &config).await;
                let appended = spawn_appends(&store, region_id, 1).await;
                next_parked_operation(&mut parked)
                    .await
                    .1
                    .send(true)
                    .unwrap();
                timeout(WAIT, appended.into_iter().next().unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                io.creates_refused.store(true, Ordering::SeqCst);
                let released = {
                    let store = store.clone();
                    tokio::spawn(async move {
                        store.release_region(&provider(region_id), region_id).await
                    })
                };
                let (object_seq, refused) = next_parked_operation(&mut parked).await;
                assert_eq!(2, object_seq);
                refused.send(false).unwrap();
                let error = timeout(WAIT, released).await.unwrap().unwrap().unwrap_err();
                assert_rejected(&error, ErrorKind::PermissionDenied);

                let stopped = match last_attempt {
                    "probe" => {
                        io.creates_refused.store(false, Ordering::SeqCst);
                        next_parked_operation(&mut parked)
                            .await
                            .1
                            .send(true)
                            .unwrap();
                        wait_until(|| store.catalog.read().unwrap().contains_object(2)).await;
                        assert_eq!(
                            vec![release(region_id, id(1, 1))],
                            releases_of(io.as_ref(), 2).await
                        );
                        store.stop().await
                    }
                    attempt => {
                        let stop = {
                            let store = store.clone();
                            tokio::spawn(async move { store.stop().await })
                        };
                        let succeeds = attempt == "stop succeeds";
                        io.creates_refused.store(!succeeds, Ordering::SeqCst);
                        next_parked_operation(&mut parked)
                            .await
                            .1
                            .send(succeeds)
                            .unwrap();
                        timeout(WAIT, stop).await.unwrap().unwrap()
                    }
                };
                let store = open(object_store, &config).await;
                if last_attempt == "stop fails" {
                    stopped.unwrap_err();
                    assert_eq!(None, watermark(&store, region_id));
                } else {
                    stopped.unwrap();
                    assert_eq!(Some(id(1, 1)), watermark(&store, region_id));
                }
                store.stop().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_store_releases_a_region_without_segments() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let store = open(object_store.clone(), &eager()).await;
        let io = store.io.clone();
        let (one, two, three) = (region(1), region(2), region(3));
        append(&store, two, "b1").await.unwrap();

        // A region the store holds no entry of is released through zero and
        // a release in full reaches the largest id, each in an object of its
        // own; neither moves the boundary region two holds.
        store.release_region(&provider(one), one).await.unwrap();
        store.obsolete_all(&provider(three), three).await.unwrap();
        assert_eq!(vec![release(one, 0)], releases_of(io.as_ref(), 2).await);
        assert_eq!(
            vec![release(three, EntryId::MAX)],
            releases_of(io.as_ref(), 3).await
        );
        assert_eq!(1, reclaim_boundary_of(io.as_ref(), 3).await);
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);

        // Region one written again stays readable across a restart.
        append(&store, one, "a1").await.unwrap();
        store.stop().await.unwrap();
        let store = open(object_store, &eager()).await;
        assert_eq!(Some(0), watermark(&store, one));
        assert_eq!(Some(EntryId::MAX), watermark(&store, three));
        assert_eq!(
            expected_entries(one, &[(id(4, 1), "a1")]),
            read_entries(&store, one, 0).await
        );
        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_store_release_record_lives_as_long_as_the_segments_it_covers() {
        let _serialized = SERIALIZED.lock().await;
        let object_store = memory_store();
        let (one, two) = (region(1), region(2));
        let store = open(object_store.clone(), &eager()).await;
        // Object 1 holds entries of both regions, object 2 the release of
        // region one and object 3 an entry of region two, which holds the
        // boundary at object 1.
        store
            .append_batch(vec![entry(&store, one, "a1"), entry(&store, two, "b1")])
            .await
            .unwrap();
        store.release_region(&provider(one), one).await.unwrap();
        append(&store, two, "b2").await.unwrap();
        assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
        store.stop().await.unwrap();

        // The record outlives a restart together with the segment it covers.
        let (io, mut parked) = ParkedIo::parking_creates(object_store.clone());
        let store = open_over(io.clone(), &eager()).await;
        assert_eq!(Some(id(1, 1)), watermark(&store, one));
        assert!(read_entries(&store, one, 0).await.is_empty());

        // Region two flushed through object 1 lets object 13, after start
        // object 12, carry boundary 3, below which the record and the
        // segment go in any order: the record is deleted, the delete of the
        // segment fails.
        io.deletes_parked.store(true, Ordering::SeqCst);
        store.obsolete(&provider(two), two, id(1, 1)).await.unwrap();
        append(&store, two, "b3").await.unwrap();
        assert_eq!(3, reclaim_boundary_of(io.as_ref(), 13).await);
        let mut deletes = parked_operations(&mut parked, 2).await;
        deletes.remove(&2).unwrap().send(true).unwrap();
        deletes.remove(&1).unwrap().send(false).unwrap();
        timeout(WAIT, store.wait_for_garbage_collection())
            .await
            .unwrap();
        assert_eq!(vec![1, 3, 12, 13], object_seqs(io.as_ref()).await);
        store.stop().await.unwrap();

        // Recovery indexes nothing below boundary 3, which the start object
        // carries forward, so the segment the record covered is never live
        // again, and the first collection deletes it.
        let plain = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
        let recovered = recover(&plain).await.unwrap();
        assert_eq!(3, recovered.reclaim_boundary);
        assert_eq!(vec![1], recovered.unindexed);
        assert!(recovered.released_entry_ids.is_empty());
        let store = open(object_store, &eager()).await;
        assert_eq!(3, reclaim_boundary_of(&plain, 22).await);
        assert_eq!(None, watermark(&store, one));
        store.obsolete(&provider(two), two, id(3, 1)).await.unwrap();
        assert_eq!(vec![3, 12, 13, 22], collected_object_seqs(&store).await);
        store.stop().await.unwrap();
    }

    /// Object access whose next create stores the object but reports a
    /// transient failure, as a create whose response is lost.
    struct LostResponseIo {
        inner: ObjectStoreIo,
        lose_next: AtomicBool,
    }

    #[async_trait::async_trait]
    impl WalObjectIo for LostResponseIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if !self.lose_next.swap(false, Ordering::SeqCst) {
                return Ok(result);
            }
            Err(
                object_store::Error::new(object_store::ErrorKind::Unexpected, "lost response")
                    .set_temporary(),
            )
            .context(WalObjectStoreSnafu {
                operation: "write",
                path: self.object_path(object_seq),
            })
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
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

        fn storage(&self) -> String {
            self.inner.storage()
        }
    }

    /// Object access whose first create lets another object land at a given
    /// sequence first, as a create issued before recovery would.
    struct RacingIo {
        inner: ObjectStoreIo,
        late: Mutex<Option<(u64, Bytes)>>,
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RacingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            let late = self.late.lock().unwrap().take();
            if let Some((late_seq, late)) = late {
                self.inner.put_if_absent(late_seq, late).await?;
            }
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
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

        fn storage(&self) -> String {
            self.inner.storage()
        }
    }

    /// Object access whose whole-object reads, or conditional creates, and
    /// on request deletes and range reads, park until the test releases
    /// them, counting how many are in flight. A release of false fails the
    /// operation with a transient error before it reaches the object store,
    /// or a create with denied access once `creates_refused` is set.
    struct ParkedIo {
        inner: ObjectStoreIo,
        parked: mpsc::UnboundedSender<(u64, oneshot::Sender<bool>)>,
        park_creates: bool,
        creates_refused: AtomicBool,
        /// Whether creates park yet; set once the store opened.
        creates_parked: AtomicBool,
        /// Whether deletes park.
        deletes_parked: AtomicBool,
        range_reads_parked: AtomicBool,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl ParkedIo {
        fn over(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            Self::new(object_store, false)
        }

        fn parking_creates(
            object_store: ObjectStore,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            Self::new(object_store, true)
        }

        fn new(
            object_store: ObjectStore,
            park_creates: bool,
        ) -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
        ) {
            let (parked, parked_rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                    parked,
                    park_creates,
                    creates_refused: AtomicBool::new(false),
                    creates_parked: AtomicBool::new(false),
                    deletes_parked: AtomicBool::new(false),
                    range_reads_parked: AtomicBool::new(false),
                    in_flight: AtomicUsize::new(0),
                    max_in_flight: AtomicUsize::new(0),
                }),
                parked_rx,
            )
        }

        /// Parks until the test releases `object_seq` and returns whether the
        /// operation proceeds.
        async fn park(&self, object_seq: u64) -> bool {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            let (release, released) = oneshot::channel();
            self.parked.send((object_seq, release)).unwrap();
            let proceed = released.await.unwrap();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            proceed
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for ParkedIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            if self.park_creates
                && self.creates_parked.load(Ordering::SeqCst)
                && !self.park(object_seq).await
            {
                if self.creates_refused.load(Ordering::SeqCst) {
                    return Err(object_store::Error::new(
                        ErrorKind::PermissionDenied,
                        "access denied",
                    )
                    .set_persistent())
                    .context(WalObjectStoreSnafu {
                        operation: "write",
                        path: self.object_path(object_seq),
                    });
                }
                return injected_failure("write", self.object_path(object_seq));
            }
            self.inner.put_if_absent(object_seq, content).await
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            if !self.park_creates && !self.park(object_seq).await {
                return injected_failure("read", self.object_path(object_seq));
            }
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            if self.range_reads_parked.load(Ordering::SeqCst) && !self.park(object_seq).await {
                return injected_failure("read", self.object_path(object_seq));
            }
            self.inner.get_range(object_seq, offset, len).await
        }

        async fn delete(&self, object_seq: u64) -> Result<()> {
            if self.deletes_parked.load(Ordering::SeqCst) && !self.park(object_seq).await {
                return injected_failure("delete", self.object_path(object_seq));
            }
            self.inner.delete(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }

        fn storage(&self) -> String {
            self.inner.storage()
        }
    }

    type RangeReads = Arc<Mutex<Vec<(u64, u64, u64)>>>;

    /// Object access that records every range read as (sequence, offset,
    /// length) and, on request, reports the next conditional create as failed
    /// after it wrote the object, fails it with a given error before it
    /// writes, stores the same content before it writes, or damages the bytes
    /// the next range read returns; it also fails the next listing, or the
    /// next range read of an object at an offset, with a transient error,
    /// panics in the next range read of a given object, and blocks its thread
    /// in the next range read of a given object until the test releases it.
    struct RecordingIo {
        inner: ObjectStoreIo,
        reads: RangeReads,
        fail_after_next_put: AtomicBool,
        fail_next_put: Mutex<Option<object_store::Error>>,
        damage_next_range_read: AtomicBool,
        duplicate_next_put: AtomicBool,
        fail_next_list: AtomicBool,
        fail_range_read_at: Mutex<Option<(u64, u64)>>,
        panic_range_read_of: Mutex<Option<u64>>,
        block_range_read_of: Mutex<Option<BlockedRangeRead>>,
    }

    /// The object whose range read blocks, the sender that reports the read
    /// began and the receiver that releases it.
    type BlockedRangeRead = (
        u64,
        std::sync::mpsc::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    );

    impl RecordingIo {
        fn over(object_store: ObjectStore) -> (Arc<Self>, RangeReads) {
            let reads = RangeReads::default();
            let io = Self {
                inner: ObjectStoreIo::new(object_store, PREFIX).unwrap(),
                reads: reads.clone(),
                fail_after_next_put: AtomicBool::new(false),
                fail_next_put: Mutex::new(None),
                damage_next_range_read: AtomicBool::new(false),
                duplicate_next_put: AtomicBool::new(false),
                fail_next_list: AtomicBool::new(false),
                fail_range_read_at: Mutex::new(None),
                panic_range_read_of: Mutex::new(None),
                block_range_read_of: Mutex::new(None),
            };
            (Arc::new(io), reads)
        }

        fn fail_next_put_permanently(&self) {
            let error = object_store::Error::new(
                object_store::ErrorKind::PermissionDenied,
                "injected failure",
            );
            *self.fail_next_put.lock().unwrap() = Some(error);
        }

        /// Fails the next create as a retry layer reports a temporary error
        /// that outlasted its retries.
        fn fail_next_put_persistently(&self) {
            let error = object_store::Error::new(object_store::ErrorKind::Unexpected, "injected")
                .set_temporary()
                .set_persistent();
            *self.fail_next_put.lock().unwrap() = Some(error);
        }
    }

    #[async_trait::async_trait]
    impl WalObjectIo for RecordingIo {
        async fn put_if_absent(&self, object_seq: u64, content: Bytes) -> Result<PutResult> {
            if let Some(error) = self.fail_next_put.lock().unwrap().take() {
                return Err(error).context(WalObjectStoreSnafu {
                    operation: "write",
                    path: self.object_path(object_seq),
                });
            }
            if self.duplicate_next_put.swap(false, Ordering::Relaxed) {
                self.inner
                    .put_if_absent(object_seq, content.clone())
                    .await?;
            }
            let result = self.inner.put_if_absent(object_seq, content).await?;
            if self.fail_after_next_put.swap(false, Ordering::Relaxed) {
                return injected_failure("write", self.object_path(object_seq));
            }
            Ok(result)
        }

        async fn get(&self, object_seq: u64) -> Result<Bytes> {
            self.inner.get(object_seq).await
        }

        async fn get_range(&self, object_seq: u64, offset: u64, len: u64) -> Result<Bytes> {
            self.reads.lock().unwrap().push((object_seq, offset, len));
            if self
                .fail_range_read_at
                .lock()
                .unwrap()
                .take_if(|at| *at == (object_seq, offset))
                .is_some()
            {
                return injected_failure("read", self.object_path(object_seq));
            }
            let panics = self
                .panic_range_read_of
                .lock()
                .unwrap()
                .take_if(|of| *of == object_seq)
                .is_some();
            assert!(!panics, "injected panic");
            let blocked = self
                .block_range_read_of
                .lock()
                .unwrap()
                .take_if(|(of, ..)| *of == object_seq);
            if let Some((_, began, release)) = blocked {
                began.send(()).unwrap();
                // Blocking the thread keeps an abort from cancelling the read.
                let _ = release.recv();
            }
            let bytes = self.inner.get_range(object_seq, offset, len).await?;
            if self.damage_next_range_read.swap(false, Ordering::Relaxed) {
                let mut damaged = bytes.to_vec();
                if let Some(last) = damaged.last_mut() {
                    *last ^= 1;
                }
                return Ok(Bytes::from(damaged));
            }
            Ok(bytes)
        }

        async fn delete(&self, object_seq: u64) -> Result<()> {
            self.inner.delete(object_seq).await
        }

        async fn list(&self) -> Result<Vec<ListedObject>> {
            if self.fail_next_list.swap(false, Ordering::Relaxed) {
                return injected_failure("list", PREFIX.to_string());
            }
            self.inner.list().await
        }

        fn object_path(&self, object_seq: u64) -> String {
            self.inner.object_path(object_seq)
        }

        fn storage(&self) -> String {
            self.inner.storage()
        }
    }

    /// Drives every outcome of a conditional create and every failure of the
    /// object store access through the store, one case per fault, and checks
    /// that every row of the *Failure matrix* has one.
    mod fault_matrix {
        use object_store::layers::mock::{self, MockLayerBuilder, oio};
        use prometheus::Histogram;

        use super::*;

        /// A row of the *Failure matrix*, named by its *Situation* cell.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Row {
            /// create succeeds, or identical retry
            Created,
            /// transient I/O error
            Transient,
            /// the lowest pending object is not durable within
            /// `max_unpersisted_age` of being sealed
            Unconfirmed,
            /// I/O error that requires intervention
            Intervention,
            /// any I/O error after `stop` began
            ErrorAfterStop,
            /// any conflicting object
            Conflict,
            /// encoding or catalog error
            CatalogError,
            /// create succeeds at the last representable sequence
            LastSequence,
            /// the start object meets an object of an earlier epoch
            StartMovesPast,
            /// the start object meets an object of the same or a later epoch,
            /// or its create has an unknown outcome
            StartFails,
        }

        /// Every row of the matrix, in the order the RFC lists them.
        const ROWS: [Row; 10] = [
            Row::Created,
            Row::Transient,
            Row::Unconfirmed,
            Row::Intervention,
            Row::ErrorAfterStop,
            Row::Conflict,
            Row::CatalogError,
            Row::LastSequence,
            Row::StartMovesPast,
            Row::StartFails,
        ];

        /// One failure of the object store access and the row it exercises,
        /// when the failure is an outcome of a conditional create.
        struct Case {
            fault: &'static str,
            row: Option<Row>,
            run: fn() -> BoxFuture<'static, ()>,
        }

        /// Declares [`Counters`] from one sampling expression per field.
        macro_rules! counters {
            ($($(#[$doc:meta])* $field:ident: $sample:expr,)*) => {
                /// The metrics the cases assert on, sampled around a case.
                #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
                struct Counters {
                    $($(#[$doc])* $field: u64,)*
                }

                impl Counters {
                    fn sample() -> Self {
                        Self { $($field: $sample,)* }
                    }

                    /// The metrics recorded since `before`.
                    fn since(before: Self) -> Self {
                        let now = Self::sample();
                        Self { $($field: now.$field - before.$field,)* }
                    }
                }
            };
        }

        counters! {
            created_objects: METRIC_OBJECT_STORE_WAL_CREATED_OBJECTS_TOTAL.get(),
            /// Samples of the seal-to-durable histogram.
            seal_to_durable: METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS.get_sample_count(),
            create_failures: METRIC_OBJECT_STORE_WAL_CREATE_FAILURES_TOTAL.get(),
            create_conflicts: METRIC_OBJECT_STORE_WAL_CREATE_CONFLICTS_TOTAL.get(),
            poisoned: METRIC_OBJECT_STORE_WAL_POISONED_TOTAL.get(),
            /// Samples of the object size and entry histograms, one per seal.
            sealed_objects: sealed_objects(),
            /// The sums of those two histograms.
            object_bytes: sum_of(&METRIC_OBJECT_STORE_WAL_OBJECT_BYTES),
            object_entries: sum_of(&METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES),
            /// Samples of the `durable` acknowledgement histogram.
            acknowledged_appends: METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS.get_sample_count(),
            /// Objects recovery read footers from, and recoveries timed.
            recovered_objects: METRIC_OBJECT_STORE_WAL_RECOVERED_OBJECTS_TOTAL.get(),
            recoveries: METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS.get_sample_count(),
            /// Read streams driven to their end.
            reads: METRIC_OBJECT_STORE_WAL_READ_SECONDS.get_sample_count(),
            deleted_objects: METRIC_OBJECT_STORE_WAL_DELETED_OBJECTS_TOTAL.get(),
            failed_deletes: METRIC_OBJECT_STORE_WAL_FAILED_DELETES_TOTAL.get(),
            release_records: METRIC_OBJECT_STORE_WAL_RELEASE_RECORDS_TOTAL.get(),
            /// No read of the matrix meets a corrupted segment.
            skipped_segments: METRIC_OBJECT_STORE_WAL_SKIPPED_SEGMENTS_TOTAL.get(),
            stalled_appends: METRIC_OBJECT_STORE_WAL_STALLED_APPENDS_TOTAL.get(),
            stalled_waits: METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS.get_sample_count(),
            rejected_appends: METRIC_OBJECT_STORE_WAL_REJECTED_APPENDS_TOTAL.get(),
        }

        /// Samples of the two object shape histograms, which a sealed batch
        /// feeds together; a batch that fed only one of them fails the case.
        fn sealed_objects() -> u64 {
            let bytes = METRIC_OBJECT_STORE_WAL_OBJECT_BYTES.get_sample_count();
            let entries = METRIC_OBJECT_STORE_WAL_OBJECT_ENTRIES.get_sample_count();
            assert_eq!(
                bytes, entries,
                "an object was sized but its entries were not counted"
            );
            bytes
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

        /// The length of the object a batch of `entries` encodes into; the
        /// header and the entry ids have a fixed width, so any will do.
        fn object_bytes_of(entries: &[(RegionId, &str)]) -> u64 {
            let mut positions = HashMap::new();
            let records = entries
                .iter()
                .map(|(region_id, data)| {
                    let position = positions.entry(*region_id).or_insert(0);
                    *position += 1;
                    Record {
                        region_id: *region_id,
                        entry_id: id(1, *position),
                        payload: Bytes::from(data.as_bytes().to_vec()),
                    }
                })
                .collect::<Vec<_>>();
            encode_object(chain_header(1, 1, None), &records)
                .unwrap()
                .bytes
                .len() as u64
        }

        /// The length of the object of one two-byte entry, which most cases seal.
        fn one_entry_object() -> u64 {
            object_bytes_of(&[(region(1), "a1")])
        }

        /// The length of an object that holds one release record alone.
        fn release_object() -> u64 {
            let releases = BTreeMap::from([(region(1), 0)]);
            encode_batch(chain_header(1, 1, None), Vec::new(), &releases)
                .unwrap()
                .bytes
                .len() as u64
        }

        /// The sum and the count of a timing histogram before a forced wait.
        fn timer_before(histogram: &Histogram) -> (f64, u64) {
            (histogram.get_sample_sum(), histogram.get_sample_count())
        }

        /// One sample of a timing histogram, the wait the case forced on it,
        /// and what the case itself took around that wait.
        #[derive(Debug, Clone, Copy)]
        struct Timed {
            recorded: f64,
            /// The forced wait as measured, which may be far longer than asked.
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

            /// Asserts two observations of one timer: each lies between its own
            /// wait and its case's elapsed time, and the long wait outlasted the
            /// whole short observation, so a timer that records a constant
            /// fails one of them however slow the machine is.
            fn assert_pair(name: &str, short: Self, long: Self) {
                assert!(
                    long.held > short.elapsed,
                    "the {name} timer was observed around a long wait of {:?} that did not outlast the short observation's {:?}",
                    long.held,
                    short.elapsed
                );
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
            }
        }

        fn indexed_gauges() -> (i64, i64) {
            (
                METRIC_OBJECT_STORE_WAL_INDEXED_OBJECTS.get(),
                METRIC_OBJECT_STORE_WAL_INDEXED_BYTES.get(),
            )
        }

        /// Asserts that both indexed gauges hold exactly the objects `indexed`,
        /// with their sizes as listed. Objects a failed create left behind are
        /// listed but not counted.
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

        /// Asserts every append failed with an error `recognise` accepts.
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

        /// Asserts that a read of the poisoned store fails with the terminal error.
        async fn assert_read_is_terminal(store: &ObjectStoreLogStore, region_id: RegionId) {
            let error = store
                .read(&provider(region_id), 1)
                .await
                .err()
                .expect("a read of a poisoned store must fail");
            let terminal = terminal(&store.terminal_error).expect("the store is not poisoned");
            assert!(
                matches!(&error, Error::ObjectStoreWal { source, .. } if Arc::ptr_eq(source, &terminal)),
                "unexpected error: {error:?}"
            );
        }

        #[tokio::test]
        async fn test_store_fault_matrix() {
            let _serialized = SERIALIZED.lock().await;
            wait_until(|| LIVE_ACTORS.load(Ordering::SeqCst) == 0).await;
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
                    row: Some(Row::Created),
                    run: || Box::pin(create_response_is_lost()),
                },
                Case {
                    fault: "a create that fails transiently with a later object created and one in flight",
                    row: Some(Row::Transient),
                    run: || Box::pin(create_fails_transiently()),
                },
                Case {
                    fault: "a create that never answers past `max_unpersisted_age`",
                    row: Some(Row::Unconfirmed),
                    run: || Box::pin(create_is_unconfirmed_past_the_bound()),
                },
                Case {
                    fault: "creates the object store refuses with an error that requires intervention",
                    row: Some(Row::Intervention),
                    run: || Box::pin(creates_refused()),
                },
                Case {
                    fault: "a create that fails transiently after `stop` began",
                    row: Some(Row::ErrorAfterStop),
                    run: || Box::pin(create_fails_transiently_after_stop_began()),
                },
                Case {
                    fault: "a stop while the object store refuses the backlog",
                    row: Some(Row::ErrorAfterStop),
                    run: || Box::pin(stop_while_the_backlog_is_refused()),
                },
                Case {
                    fault: "a create that finds an object of an earlier epoch",
                    row: Some(Row::Conflict),
                    run: || Box::pin(create_finds_an_earlier_epoch()),
                },
                Case {
                    fault: "a create that finds an object of the same or a later epoch",
                    row: Some(Row::Conflict),
                    run: || Box::pin(create_finds_another_writer()),
                },
                Case {
                    fault: "a create that completes after a conflict poisoned the store",
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
                    fault: "a release whose create succeeds",
                    row: Some(Row::Created),
                    run: || Box::pin(release_create_succeeds()),
                },
                Case {
                    fault: "a release whose create fails transiently",
                    row: Some(Row::Transient),
                    run: || Box::pin(release_create_fails_transiently()),
                },
                Case {
                    fault: "a release whose create the object store refuses with an error that requires intervention",
                    row: Some(Row::Intervention),
                    run: || Box::pin(release_create_refused()),
                },
                Case {
                    fault: "a release while the object store refuses the backlog",
                    row: None,
                    run: || Box::pin(release_while_the_backlog_is_refused()),
                },
                Case {
                    fault: "a release whose create finds an object of the same or a later epoch",
                    row: Some(Row::Conflict),
                    run: || Box::pin(release_create_finds_another_writer()),
                },
                Case {
                    fault: "a start object that meets an object of an earlier epoch",
                    row: Some(Row::StartMovesPast),
                    run: || Box::pin(start_object_meets_an_earlier_epoch()),
                },
                Case {
                    fault: "a start object that meets an object of the same or a later epoch",
                    row: Some(Row::StartFails),
                    run: || Box::pin(start_object_meets_another_writer()),
                },
                Case {
                    fault: "a start object that meets an object with identical content",
                    row: Some(Row::StartFails),
                    run: || Box::pin(start_object_meets_an_identical_object()),
                },
                Case {
                    fault: "a start object whose create has an unknown outcome",
                    row: Some(Row::StartFails),
                    run: || Box::pin(start_object_create_has_an_unknown_outcome()),
                },
                Case {
                    fault: "a listing that fails at recovery",
                    row: None,
                    run: || Box::pin(listing_fails_at_recovery()),
                },
                Case {
                    fault: "a trailer fetch that fails at recovery",
                    row: None,
                    run: || Box::pin(trailer_fetch_fails_at_recovery()),
                },
                Case {
                    fault: "a footer fetch that fails at recovery",
                    row: None,
                    run: || Box::pin(footer_fetch_fails_at_recovery()),
                },
                Case {
                    fault: "a recovery the catalog rejects after every footer was read",
                    row: None,
                    run: || Box::pin(catalog_rejects_the_recovery()),
                },
                Case {
                    fault: "an object gone by the time recovery fetches it",
                    row: None,
                    run: || Box::pin(object_vanishes_before_its_fetch()),
                },
                Case {
                    fault: "a segment fetch that fails at a read",
                    row: None,
                    run: || Box::pin(segment_fetch_fails_at_a_read()),
                },
                Case {
                    fault: "a delete that fails at a collection",
                    row: None,
                    run: || Box::pin(delete_fails_at_a_collection()),
                },
                Case {
                    fault: "a stop with a delete in flight",
                    row: None,
                    run: || Box::pin(stop_waits_for_a_delete_in_flight()),
                },
                Case {
                    fault: "an append past the entry id range of its region",
                    row: None,
                    run: || Box::pin(region_exhausts_its_entry_ids()),
                },
                Case {
                    fault: "creates that succeed after `stop` began",
                    row: None,
                    run: || Box::pin(creates_succeed_after_stop_began()),
                },
                Case {
                    fault: "a create that conflicts after `stop` began",
                    row: None,
                    run: || Box::pin(create_conflicts_after_stop_began()),
                },
                Case {
                    fault: "an append held back by full sealed batches",
                    row: None,
                    run: || Box::pin(append_held_back_by_full_sealed_batches()),
                },
                Case {
                    fault: "a stalled append released by admission, then one released by a stop",
                    row: None,
                    run: || Box::pin(stalled_appends_released_by_admission_and_by_a_stop()),
                },
                Case {
                    fault: "a stalled append released by a terminal error",
                    row: None,
                    run: || Box::pin(stalled_append_released_by_a_terminal_error()),
                },
                Case {
                    fault: "a stalled append released by the teardown of the actor",
                    row: None,
                    run: || Box::pin(stalled_append_released_by_the_teardown()),
                },
                Case {
                    fault: "an object store that answers slowly",
                    row: None,
                    run: || Box::pin(object_store_answers_slowly()),
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

        /// The batches are acknowledged in order and the sequence advances.
        /// Each create is held, the second past the whole first observation,
        /// so both write path timers are observed around disjoint waits.
        async fn create_succeeds() {
            let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
            let before = Counters::sample();
            let (region_id, other) = (region(1), region(2));
            let mut seals = Vec::new();
            let mut acknowledgements = Vec::new();
            let mut hold = Duration::from_millis(20);
            // The second batch holds entries of two regions, so entries and
            // objects differ.
            for batch in [
                vec![(region_id, "a1")],
                vec![(region_id, "a2"), (region_id, "a3"), (other, "b1")],
            ] {
                let seal = timer_before(&METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS);
                let acknowledgement = timer_before(&METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS);
                let started_at = Instant::now();
                let entries = batch
                    .iter()
                    .map(|(region_id, data)| entry(&store, *region_id, data))
                    .collect();
                let appended = spawn_append_batch(&store, entries);
                let (_, release) = next_parked_operation(&mut parked).await;
                let parked_at = Instant::now();
                tokio::time::sleep(hold).await;
                let held = parked_at.elapsed();
                release.send(true).unwrap();
                timeout(WAIT, appended).await.unwrap().unwrap().unwrap();
                let elapsed = started_at.elapsed();
                let timed = |timer, before| Timed::since(timer, before, held, elapsed);
                seals.push(timed(
                    &METRIC_OBJECT_STORE_WAL_SEAL_TO_DURABLE_SECONDS,
                    seal,
                ));
                acknowledgements.push(timed(
                    &METRIC_OBJECT_STORE_WAL_APPEND_ACK_SECONDS,
                    acknowledgement,
                ));
                hold = elapsed + Duration::from_millis(20);
            }
            Timed::assert_pair("seal to durable", seals[0], seals[1]);
            Timed::assert_pair(
                "append acknowledgement",
                acknowledgements[0],
                acknowledgements[1],
            );

            // Object 1 carries the boundary past the start object.
            assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
            assert_eq!(
                expected_entries(
                    region_id,
                    &[(id(1, 1), "a1"), (id(2, 1), "a2"), (id(2, 2), "a3")]
                ),
                read_entries(&store, region_id, 1).await
            );
            assert_indexed(io.as_ref(), &[1, 2]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: one_entry_object()
                        + object_bytes_of(&[(region_id, "a2"), (region_id, "a3"), (other, "b1")]),
                    object_entries: 4,
                    acknowledged_appends: 2,
                    reads: 1,
                    deleted_objects: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// A release seals an object of its own and returns once it is
        /// indexed, in both modes.
        async fn release_create_succeeds() {
            for config in [eager(), enqueued(eager())] {
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                let released = spawn_release(&store, region_id);
                let (object_seq, create) = next_parked_operation(&mut parked).await;
                assert_eq!(1, object_seq);
                assert!(!released.is_finished());
                assert_indexed(io.as_ref(), &[0]).await;
                create.send(true).unwrap();
                timeout(WAIT, released).await.unwrap().unwrap().unwrap();
                // Object 1 carries the boundary past the start object.
                assert_eq!(vec![1], collected_object_seqs(&store).await);
                assert_eq!(
                    vec![release(region_id, 0)],
                    releases_of(io.as_ref(), 1).await
                );
                assert!(read_entries(&store, region_id, 1).await.is_empty());
                assert_indexed(io.as_ref(), &[1]).await;
                assert_eq!(
                    Counters {
                        created_objects: 1,
                        seal_to_durable: 1,
                        sealed_objects: 1,
                        object_bytes: release_object(),
                        reads: 1,
                        deleted_objects: 1,
                        release_records: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
                store.stop().await.unwrap();
            }
        }

        /// The store repeats the create, and the release waits for the
        /// repeat, in both modes.
        async fn release_create_fails_transiently() {
            for config in [eager(), enqueued(eager())] {
                release_create_fails_transiently_with(config).await;
            }
        }

        async fn release_create_fails_transiently_with(config: ObjectStoreWalConfig) {
            let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
            let before = Counters::sample();
            let region_id = region(1);
            let released = spawn_release(&store, region_id);
            let (object_seq, create) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            create.send(false).unwrap();
            let (object_seq, repeat) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            assert!(!released.is_finished());
            assert_eq!(0, failing_stores());
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: release_object(),
                    ..Counters::default()
                },
                Counters::since(before)
            );

            repeat.send(true).unwrap();
            timeout(WAIT, released).await.unwrap().unwrap().unwrap();
            assert_eq!(vec![1], collected_object_seqs(&store).await);
            assert_indexed(io.as_ref(), &[1]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: release_object(),
                    deleted_objects: 1,
                    release_records: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The object store denies access to the object of a release: the
        /// store becomes failing and the release fails at once with the
        /// retryable error; its record stays in the backlog and becomes
        /// durable with the next create that succeeds, in both modes.
        async fn release_create_refused() {
            for config in [eager(), enqueued(eager())] {
                release_create_refused_with(config).await;
            }
        }

        async fn release_create_refused_with(config: ObjectStoreWalConfig) {
            let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
            let before = Counters::sample();
            let region_id = region(1);
            let released = spawn_release(&store, region_id);
            let (object_seq, create) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            io.creates_refused.store(true, Ordering::SeqCst);
            create.send(false).unwrap();
            let error = timeout(WAIT, released).await.unwrap().unwrap().unwrap_err();
            assert_rejected(&error, ErrorKind::PermissionDenied);
            assert_eq!(1, failing_stores());
            assert!(terminal(&store.terminal_error).is_none());
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: release_object(),
                    rejected_appends: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );

            let (object_seq, create) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            create.send(true).unwrap();
            wait_until(|| failing_stores() == 0).await;
            assert_eq!(vec![1], collected_object_seqs(&store).await);
            assert_eq!(
                vec![release(region_id, 0)],
                releases_of(io.as_ref(), 1).await
            );
            assert_indexed(io.as_ref(), &[1]).await;
            assert_eq!(
                Counters {
                    created_objects: 1,
                    seal_to_durable: 1,
                    create_failures: 1,
                    sealed_objects: 1,
                    object_bytes: release_object(),
                    deleted_objects: 1,
                    release_records: 1,
                    rejected_appends: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Releases that arrive while the store is failing fail at once with
        /// the retryable error, and their record is kept coalesced: the
        /// ticks that pass seal nothing behind the backlog. A stop seals the
        /// record with the backlog, and recovery reads it back.
        async fn release_while_the_backlog_is_refused() {
            let object_store = memory_store();
            let (store, io, mut parked) = open_parking_creates(
                object_store.clone(),
                &enqueued(config(MIN_FLUSH_INTERVAL, 1)),
            )
            .await;
            let before = Counters::sample();
            let region_id = region(1);
            append(&store, region_id, "a1").await.unwrap();
            io.creates_refused.store(true, Ordering::SeqCst);
            next_parked_operation(&mut parked)
                .await
                .1
                .send(false)
                .unwrap();
            wait_until(|| failing_stores() == 1).await;
            for _ in 0..3 {
                let error = store
                    .release_region(&provider(region_id), region_id)
                    .await
                    .unwrap_err();
                assert_rejected(&error, ErrorKind::PermissionDenied);
            }
            // The next attempt of the backlog comes after several ticks.
            let (object_seq, probe) = next_parked_operation(&mut parked).await;
            assert_eq!(1, object_seq);
            assert!(parked.try_recv().is_err());
            probe.send(false).unwrap();
            wait_until(|| Counters::since(before).create_failures == 2).await;
            assert_eq!(1, Counters::since(before).sealed_objects);

            let stop = {
                let store = store.clone();
                tokio::spawn(async move { store.stop().await })
            };
            for create in parked_operations(&mut parked, 2).await.into_values() {
                create.send(true).unwrap();
            }
            timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
            assert_eq!(0, failing_stores());
            assert_eq!(
                vec![release(region_id, id(1, 1))],
                releases_of(io.as_ref(), 2).await
            );
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    create_failures: 2,
                    sealed_objects: 2,
                    object_bytes: one_entry_object() + release_object(),
                    object_entries: 1,
                    release_records: 1,
                    rejected_appends: 3,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            let store = open(object_store, &eager()).await;
            assert_eq!(Some(id(1, 1)), watermark(&store, region_id));
            store.stop().await.unwrap();
        }

        /// Another writer holds the sequence: the release fails with the
        /// conflict, which poisons the store, in both modes.
        async fn release_create_finds_another_writer() {
            for config in [eager(), enqueued(eager())] {
                let object_store = memory_store();
                let (store, io, mut parked) =
                    open_parking_creates(object_store.clone(), &config).await;
                put_foreign(&object_store, 1, 2).await;
                let before = Counters::sample();
                let region_id = region(1);
                let released = spawn_release(&store, region_id);
                let (_, create) = next_parked_operation(&mut parked).await;
                create.send(true).unwrap();
                let error = timeout(WAIT, released).await.unwrap().unwrap().unwrap_err();
                let path = io.object_path(1);
                assert!(
                    matches!(unwrap_shared(&error), Error::WalObjectConflict { path: actual, .. } if *actual == path),
                    "unexpected error: {error:?}"
                );
                assert_read_is_terminal(&store, region_id).await;
                assert_indexed(io.as_ref(), &[0]).await;
                assert_eq!(
                    Counters {
                        create_conflicts: 1,
                        poisoned: 1,
                        sealed_objects: 1,
                        object_bytes: release_object(),
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
                assert!(matches!(
                    unwrap_shared(&store.stop().await.unwrap_err()),
                    Error::WalObjectConflict { .. }
                ));
            }
        }

        /// Appends over `io` after `arm` made the create meet the very object
        /// it writes, and asserts it was accepted as an identical retry.
        async fn assert_identical_retry(io: Arc<dyn WalObjectIo>, arm: impl FnOnce()) {
            let store = open_over(io.clone(), &eager()).await;
            let before = Counters::sample();
            let region_id = region(1);
            arm();
            let response = append(&store, region_id, "a1").await.unwrap();
            assert_eq!(Some(&id(1, 1)), response.last_entry_ids.get(&region_id));
            let response = append(&store, region_id, "a2").await.unwrap();
            assert_eq!(Some(&id(2, 1)), response.last_entry_ids.get(&region_id));
            assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1"), (id(2, 1), "a2")]),
                read_entries(&store, region_id, 1).await
            );
            assert_indexed(io.as_ref(), &[1, 2]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    acknowledged_appends: 2,
                    reads: 1,
                    deleted_objects: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        async fn create_finds_an_identical_object() {
            let (io, _) = RecordingIo::over(memory_store());
            let armed = io.clone();
            assert_identical_retry(io.clone(), move || {
                armed.duplicate_next_put.store(true, Ordering::Relaxed)
            })
            .await;
            assert!(!io.duplicate_next_put.load(Ordering::Relaxed));
        }

        /// The object store writes the object and loses the response, and the
        /// read-back that reconciles the create finds the same bytes.
        async fn create_response_is_lost() {
            let lose_next = Arc::new(AtomicBool::new(false));
            let losing = lose_next.clone();
            let object_store = memory_store().layer(
                MockLayerBuilder::default()
                    .writer_factory(Arc::new(move |_, _, inner| {
                        if losing.swap(false, Ordering::SeqCst) {
                            Box::new(LosingWriter(inner))
                        } else {
                            inner
                        }
                    }))
                    .build()
                    .unwrap(),
            );
            let io = Arc::new(ObjectStoreIo::new(object_store, PREFIX).unwrap());
            let armed = lose_next.clone();
            assert_identical_retry(io, move || armed.store(true, Ordering::SeqCst)).await;
            assert!(!lose_next.load(Ordering::SeqCst));
        }

        /// A writer that writes the object and then reports a failure, as a
        /// create whose response is lost.
        struct LosingWriter(oio::Writer);

        impl oio::Write for LosingWriter {
            async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
                self.0.write(buffer).await
            }

            async fn close(&mut self) -> mock::Result<mock::Metadata> {
                self.0.close().await?;
                Err(mock::Error::new(mock::ErrorKind::Unexpected, "lost response").set_temporary())
            }

            async fn abort(&mut self) -> mock::Result<()> {
                self.0.abort().await
            }
        }

        /// Object 2 is created and object 3 in flight when object 1 fails
        /// transiently: nothing fails, the create of object 1 is repeated
        /// under its sequence with the same bytes, and the three objects are
        /// indexed in order once it succeeds, in both modes. Writers of the
        /// `durable` mode are acknowledged then.
        async fn create_fails_transiently() {
            for config in [eager(), enqueued(eager())] {
                let durable = config.ack_mode == AckMode::Durable;
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                let appends = spawn_appends(&store, region_id, 3).await;
                let mut releases = parked_operations(&mut parked, 3).await;
                releases.remove(&2).unwrap().send(true).unwrap();
                wait_until(|| Counters::since(before).created_objects == 1).await;
                releases.remove(&1).unwrap().send(false).unwrap();

                // While the repeat is parked nothing is indexed, and the store
                // is not failing.
                let (object_seq, repeat) = next_parked_operation(&mut parked).await;
                assert_eq!(1, object_seq);
                assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
                assert_eq!(0, failing_stores());
                assert_indexed(io.as_ref(), &[0]).await;
                assert_eq!(
                    Counters {
                        created_objects: 1,
                        seal_to_durable: 1,
                        create_failures: 1,
                        sealed_objects: 3,
                        object_bytes: 3 * one_entry_object(),
                        object_entries: 3,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );

                releases.remove(&3).unwrap().send(true).unwrap();
                repeat.send(true).unwrap();
                for (append, object_seq) in appends.into_iter().zip(1..) {
                    let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
                    assert_eq!(
                        Some(&id(object_seq, 1)),
                        response.last_entry_ids.get(&region_id)
                    );
                }
                timeout(WAIT, store.wait_durable(&provider(region_id), id(3, 1)))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(vec![1, 2, 3], collected_object_seqs(&store).await);
                assert_eq!(
                    expected_entries(
                        region_id,
                        &[(id(1, 1), "a1"), (id(2, 1), "a2"), (id(3, 1), "a3")]
                    ),
                    read_entries(&store, region_id, 1).await
                );
                assert_indexed(io.as_ref(), &[1, 2, 3]).await;
                assert_eq!(
                    Counters {
                        created_objects: 3,
                        seal_to_durable: 3,
                        create_failures: 1,
                        sealed_objects: 3,
                        object_bytes: 3 * one_entry_object(),
                        object_entries: 3,
                        acknowledged_appends: if durable { 3 } else { 0 },
                        reads: 1,
                        deleted_objects: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
                store.stop().await.unwrap();
            }
        }

        /// The create of object 1 never answers: once the object is not
        /// durable within `max_unpersisted_age` of being sealed, the store
        /// becomes failing without waiting for the create. A writer waiting
        /// for the entry fails then, and a later one at once, while a barrier
        /// keeps waiting, and an append is rejected. The create succeeding
        /// ends the state, in both modes.
        async fn create_is_unconfirmed_past_the_bound() {
            for config in [eager(), enqueued(eager())] {
                let durable = config.ack_mode == AckMode::Durable;
                let config = ObjectStoreWalConfig {
                    max_unpersisted_age: Duration::from_millis(200),
                    ..config
                };
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                let response = store
                    .append_batch(vec![entry(&store, region_id, "a1")])
                    .await
                    .unwrap();
                assert_eq!(Some(&id(1, 1)), response.last_entry_ids.get(&region_id));
                let (object_seq, create) = next_parked_operation(&mut parked).await;
                assert_eq!(1, object_seq);
                let wait = |kind| {
                    let store = store.clone();
                    tokio::spawn(async move {
                        match kind {
                            WaitKind::Write => {
                                store
                                    .wait_write_durable(&provider(region_id), id(1, 1))
                                    .await
                            }
                            WaitKind::Barrier => {
                                store.wait_durable(&provider(region_id), id(1, 1)).await
                            }
                        }
                    })
                };
                let (writer, barrier) = (wait(WaitKind::Write), wait(WaitKind::Barrier));
                let unconfirmed = |error: &Error| {
                    assert!(
                        matches!(error, Error::ObjectStoreWalFailing { source, .. }
                            if matches!(&**source, Error::WalObjectUnconfirmed { .. })),
                        "unexpected error: {error:?}"
                    );
                    assert_eq!(RetryHint::Retryable, error.retry_hint());
                };
                unconfirmed(&timeout(WAIT, writer).await.unwrap().unwrap().unwrap_err());
                assert_eq!(1, failing_stores());
                unconfirmed(
                    &store
                        .wait_write_durable(&provider(region_id), id(1, 1))
                        .await
                        .unwrap_err(),
                );
                unconfirmed(
                    &store
                        .append_batch(vec![entry(&store, region_id, "a2")])
                        .await
                        .unwrap_err(),
                );
                assert!(!barrier.is_finished());
                assert_indexed(io.as_ref(), &[0]).await;
                assert_eq!(
                    Counters {
                        sealed_objects: 1,
                        object_bytes: one_entry_object(),
                        object_entries: 1,
                        rejected_appends: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );

                create.send(true).unwrap();
                timeout(WAIT, barrier).await.unwrap().unwrap().unwrap();
                wait_until(|| failing_stores() == 0).await;
                store
                    .wait_write_durable(&provider(region_id), id(1, 1))
                    .await
                    .unwrap();
                assert_eq!(vec![1], collected_object_seqs(&store).await);
                assert_indexed(io.as_ref(), &[1]).await;
                assert_eq!(
                    Counters {
                        created_objects: 1,
                        seal_to_durable: 1,
                        sealed_objects: 1,
                        object_bytes: one_entry_object(),
                        object_entries: 1,
                        acknowledged_appends: u64::from(durable),
                        deleted_objects: 1,
                        rejected_appends: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
                store.stop().await.unwrap();
            }
        }

        /// The create of the backlog fails transiently after `stop` began:
        /// the backlog is dropped and `stop` reports the failure, in both
        /// modes, without poisoning the store.
        async fn create_fails_transiently_after_stop_began() {
            for config in [manual(), enqueued(manual())] {
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                store
                    .append_batch(vec![entry(&store, region_id, "a1")])
                    .await
                    .unwrap();
                let seal = {
                    let store = store.clone();
                    tokio::spawn(async move { store.seal_open_batch().await })
                };
                let (_, release) = next_parked_operation(&mut parked).await;
                store.raise_stopped_flag();
                release.send(false).unwrap();
                timeout(WAIT, seal).await.unwrap().unwrap().unwrap_err();
                let error = store.stop().await.unwrap_err();
                assert!(
                    matches!(unwrap_shared(&error), Error::WalObjectStore { .. }),
                    "unexpected error: {error:?}"
                );
                assert!(terminal(&store.terminal_error).is_none());
                assert_eq!(vec![0], object_seqs(io.as_ref()).await);
                assert!(read_entries(&store, region_id, 1).await.is_empty());
                assert_indexed(io.as_ref(), &[0]).await;
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
            }
        }

        /// Objects 1 to 3 are admitted when the object store starts denying
        /// access. The store keeps them under their sequences, fails a writer
        /// waiting for them and rejects the next append, while a barrier
        /// waits, and attempts object 1 alone; once it is created the others
        /// follow and appends are admitted again, in both modes.
        async fn creates_refused() {
            for config in [eager(), enqueued(eager())] {
                let durable = config.ack_mode == AckMode::Durable;
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                for index in 1..=3 {
                    store
                        .append_batch(vec![entry(&store, region_id, &format!("a{index}"))])
                        .await
                        .unwrap();
                }
                let mut releases = parked_operations(&mut parked, 3).await;
                io.creates_refused.store(true, Ordering::SeqCst);
                for object_seq in 1..=3 {
                    releases.remove(&object_seq).unwrap().send(false).unwrap();
                }
                wait_until(|| Counters::since(before).create_failures == 3).await;
                assert_eq!(1, failing_stores());
                let error = append(&store, region_id, "a4").await.unwrap_err();
                assert_rejected(&error, ErrorKind::PermissionDenied);
                let error = store
                    .wait_write_durable(&provider(region_id), id(1, 1))
                    .await
                    .unwrap_err();
                assert_rejected(&error, ErrorKind::PermissionDenied);
                let barrier = {
                    let store = store.clone();
                    tokio::spawn(
                        async move { store.wait_durable(&provider(region_id), id(3, 1)).await },
                    )
                };
                assert_eq!(0, store.durable_entry_id(&provider(region_id)).unwrap());
                assert!(terminal(&store.terminal_error).is_none());
                assert_indexed(io.as_ref(), &[0]).await;
                assert_eq!(
                    Counters {
                        create_failures: 3,
                        sealed_objects: 3,
                        object_bytes: 3 * one_entry_object(),
                        object_entries: 3,
                        rejected_appends: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );

                let (object_seq, release) = next_parked_operation(&mut parked).await;
                assert_eq!(1, object_seq);
                assert!(parked.try_recv().is_err());
                assert!(!barrier.is_finished());
                release.send(true).unwrap();
                for release in parked_operations(&mut parked, 2).await.into_values() {
                    release.send(true).unwrap();
                }
                timeout(WAIT, barrier).await.unwrap().unwrap().unwrap();
                assert_eq!(0, failing_stores());
                let response = store
                    .append_batch(vec![entry(&store, region_id, "a5")])
                    .await
                    .unwrap();
                assert_eq!(Some(&id(4, 1)), response.last_entry_ids.get(&region_id));
                next_parked_operation(&mut parked)
                    .await
                    .1
                    .send(true)
                    .unwrap();
                timeout(WAIT, store.wait_durable(&provider(region_id), id(4, 1)))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(vec![1, 2, 3, 4], collected_object_seqs(&store).await);
                assert_eq!(
                    expected_entries(
                        region_id,
                        &[
                            (id(1, 1), "a1"),
                            (id(2, 1), "a2"),
                            (id(3, 1), "a3"),
                            (id(4, 1), "a5")
                        ]
                    ),
                    read_entries(&store, region_id, 1).await
                );
                assert_indexed(io.as_ref(), &[1, 2, 3, 4]).await;
                assert_eq!(
                    Counters {
                        created_objects: 4,
                        seal_to_durable: 4,
                        create_failures: 3,
                        sealed_objects: 4,
                        object_bytes: 4 * one_entry_object(),
                        object_entries: 4,
                        acknowledged_appends: if durable { 4 } else { 0 },
                        reads: 1,
                        deleted_objects: 1,
                        rejected_appends: 1,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
                store.stop().await.unwrap();
            }
        }

        /// The object store denies access to objects 1 and 2 and still does
        /// when `stop` begins: stop attempts both together, drops the backlog
        /// when they fail and reports the refusal; the store is not poisoned.
        async fn stop_while_the_backlog_is_refused() {
            for config in [eager(), enqueued(eager())] {
                stop_while_the_backlog_is_refused_with(config).await;
            }
        }

        async fn stop_while_the_backlog_is_refused_with(config: ObjectStoreWalConfig) {
            let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
            let before = Counters::sample();
            let region_id = region(1);
            for index in 1..=2 {
                store
                    .append_batch(vec![entry(&store, region_id, &format!("a{index}"))])
                    .await
                    .unwrap();
            }
            io.creates_refused.store(true, Ordering::SeqCst);
            for release in parked_operations(&mut parked, 2).await.into_values() {
                release.send(false).unwrap();
            }
            wait_until(|| Counters::since(before).create_failures == 2).await;
            assert_eq!(1, failing_stores());
            let wait = {
                let store = store.clone();
                tokio::spawn(
                    async move { store.wait_durable(&provider(region_id), id(1, 1)).await },
                )
            };
            timeout(WAIT, store.wait_for_durability_waits(1))
                .await
                .unwrap()
                .unwrap();

            let stop = {
                let store = store.clone();
                tokio::spawn(async move { store.stop().await })
            };
            for release in parked_operations(&mut parked, 2).await.into_values() {
                release.send(false).unwrap();
            }
            let error = timeout(WAIT, stop).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectStore { error, .. }
                    if error.kind() == ErrorKind::PermissionDenied),
                "unexpected error: {error:?}"
            );
            assert_stopped(&timeout(WAIT, wait).await.unwrap().unwrap().unwrap_err());
            assert!(terminal(&store.terminal_error).is_none());
            assert_eq!(0, failing_stores());
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    create_failures: 4,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        async fn create_finds_an_earlier_epoch() {
            for config in [eager(), enqueued(eager())] {
                let object_store = memory_store();
                let store = open(object_store.clone(), &config).await;
                put_foreign(&object_store, 1, 0).await;
                let before = Counters::sample();
                let region_id = region(1);
                let response = store
                    .append_batch(vec![entry(&store, region_id, "a1")])
                    .await
                    .unwrap();
                assert_eq!(Some(&id(1, 1)), response.last_entry_ids.get(&region_id));
                let error = timeout(WAIT, store.wait_durable(&provider(region_id), id(1, 1)))
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(
                    matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                    "unexpected error: {error:?}"
                );
                assert!(store.latest_entry_id(&provider(region_id)).is_err());
                assert_read_is_terminal(&store, region_id).await;
                assert_indexed(store.io.as_ref(), &[0]).await;
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
        }

        async fn create_finds_another_writer() {
            for epoch in [1, 2] {
                let object_store = memory_store();
                let (store, io, mut parked) =
                    open_parking_creates(object_store.clone(), &eager()).await;
                put_foreign(&object_store, 1, epoch).await;
                let before = Counters::sample();
                let region_id = region(1);
                let appends = spawn_appends(&store, region_id, 2).await;
                let mut releases = parked_operations(&mut parked, 2).await;
                releases.remove(&2).unwrap().send(true).unwrap();
                wait_until(|| Counters::since(before).created_objects == 1).await;
                releases.remove(&1).unwrap().send(true).unwrap();
                let path = io.object_path(1);
                assert_appends_failed(appends, |error| {
                    matches!(error, Error::WalObjectConflict { path: actual, .. } if *actual == path)
                })
                .await;
                assert_eq!(vec![0, 1, 2], object_seqs(io.as_ref()).await);
                assert!(store.latest_entry_id(&provider(region_id)).is_err());
                assert_read_is_terminal(&store, region_id).await;
                assert_indexed(io.as_ref(), &[0]).await;
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
                store.stop().await.unwrap_err();
            }
        }

        /// The create of object 2 completes after the conflict at object 1
        /// poisoned the store; it is counted and timed all the same.
        async fn create_completes_after_the_store_was_poisoned() {
            let object_store = memory_store();
            let (store, io, mut parked) =
                open_parking_creates(object_store.clone(), &eager()).await;
            put_foreign(&object_store, 1, 1).await;
            let before = Counters::sample();
            let region_id = region(1);
            let appends = spawn_appends(&store, region_id, 2).await;
            let mut releases = parked_operations(&mut parked, 2).await;
            releases.remove(&1).unwrap().send(true).unwrap();
            assert_appends_failed(appends, |error| {
                matches!(error, Error::WalObjectConflict { .. })
            })
            .await;
            assert_indexed(io.as_ref(), &[0]).await;
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

            releases.remove(&2).unwrap().send(true).unwrap();
            wait_until(|| Counters::since(before).created_objects == 1).await;
            assert_eq!(vec![0, 1, 2], object_seqs(io.as_ref()).await);
            assert_read_is_terminal(&store, region_id).await;
            assert_indexed(io.as_ref(), &[0]).await;
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
            // The handed-out entries are lost, which stop reports.
            store.stop().await.unwrap_err();
        }

        async fn catalog_rejects_the_created_object() {
            let (store, io, mut parked) = open_parking_creates(memory_store(), &eager()).await;
            let before = Counters::sample();
            let region_id = region(1);
            let appends = spawn_appends(&store, region_id, 2).await;
            let mut releases = parked_operations(&mut parked, 2).await;
            releases.remove(&2).unwrap().send(true).unwrap();
            wait_until(|| Counters::since(before).created_objects == 1).await;
            let occupying = FooterEntry {
                region_id: region(9),
                min_entry_id: id(1, 1),
                max_entry_id: id(1, 1),
                entry_count: 1,
                segment_offset: HEADER_LEN as u64,
                segment_len: 1,
                segment_crc32: 0,
            };
            store
                .catalog
                .write()
                .unwrap()
                .insert_object(1, vec![occupying])
                .unwrap();
            releases.remove(&1).unwrap().send(true).unwrap();
            assert_appends_failed(appends, |error| {
                matches!(error, Error::CorruptedWalObject { .. })
            })
            .await;
            assert_eq!(vec![0, 1, 2], object_seqs(io.as_ref()).await);
            assert!(store.latest_entry_id(&provider(region_id)).is_err());
            assert_read_is_terminal(&store, region_id).await;
            // This store indexed neither object, whatever the case put into
            // its catalog.
            assert_indexed(io.as_ref(), &[0]).await;
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
            // The handed-out entries are lost, which stop reports.
            store.stop().await.unwrap_err();
        }

        async fn create_takes_the_last_object_sequence() {
            let object_store = memory_store();
            let last_object_seq = OBJECT_SEQ_LIMIT - 1;
            let listed = last_object_seq - 2 - UNLISTED_SEQUENCES;
            put_records(&object_store, listed, &[]).await;
            let store = open(object_store, &eager()).await;
            let region_id = region(1);
            // A poisoned store hands out no entry, so the second is built now.
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
            assert_indexed(
                store.io.as_ref(),
                &[listed, last_object_seq - 1, last_object_seq],
            )
            .await;
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

        /// Writes object 0 of epoch 1 with one entry of region 1.
        async fn put_chain_start(object_store: &ObjectStore) -> ObjectStoreIo {
            let fixtures = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
            let record = Record {
                region_id: region(1),
                entry_id: id(0, 1),
                payload: Bytes::from_static(b"a0"),
            };
            put_fixture(&fixtures, 0, &[record]).await;
            fixtures
        }

        /// The sequence of the start object of a store opened over the chain
        /// start: above object 0 and the sequences its writer can have handed
        /// out.
        const START_SEQ: u64 = 1 + UNLISTED_SEQUENCES;

        /// Opens a store whose start object meets `late` at [`START_SEQ`].
        async fn open_meeting(
            object_store: &ObjectStore,
            late: Bytes,
        ) -> Result<Arc<ObjectStoreLogStore>> {
            let io = Arc::new(RacingIo {
                inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
                late: Mutex::new(Some((START_SEQ, late))),
            });
            ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
        }

        async fn start_object_meets_an_earlier_epoch() {
            let object_store = memory_store();
            let fixtures = put_chain_start(&object_store).await;
            let late = encode_object(chain_header(START_SEQ, 1, None), &[])
                .unwrap()
                .bytes;
            let before = Counters::sample();
            let store = open_meeting(&object_store, late).await.unwrap();
            assert_eq!(
                vec![0, START_SEQ, START_SEQ + 1],
                object_seqs(&fixtures).await
            );
            assert_eq!(
                START_SEQ + 2,
                decode_header(&fixtures.get(START_SEQ + 1).await.unwrap())
                    .unwrap()
                    .epoch
            );
            assert_indexed(&fixtures, &[0, START_SEQ + 1]).await;
            assert_eq!(
                expected_entries(region(1), &[(id(0, 1), "a0")]),
                read_entries(&store, region(1), 0).await
            );
            assert_eq!(
                Counters {
                    created_objects: 1,
                    create_conflicts: 1,
                    recovered_objects: 1,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Asserts a construction failure at the start object: no other
        /// sequence was tried, and only the recovery and `create` are counted.
        async fn assert_start_fails(
            fixtures: &ObjectStoreIo,
            before: Counters,
            create: Counters,
            error: Error,
            recognise: impl Fn(&Error) -> bool,
            retry_hint: RetryHint,
        ) {
            assert!(recognise(&error), "unexpected error: {error:?}");
            assert_eq!(retry_hint, error.retry_hint());
            assert_eq!(vec![0, START_SEQ], object_seqs(fixtures).await);
            assert_indexed(fixtures, &[0]).await;
            assert_eq!(
                Counters {
                    recovered_objects: 1,
                    recoveries: 1,
                    ..create
                },
                Counters::since(before)
            );
        }

        async fn start_object_meets_another_writer() {
            // The start object takes epoch `START_SEQ + 1`.
            for epoch in [START_SEQ + 1, START_SEQ + 2] {
                let object_store = memory_store();
                let fixtures = put_chain_start(&object_store).await;
                let late = encode_object(chain_header(START_SEQ, epoch, None), &[])
                    .unwrap()
                    .bytes;
                let before = Counters::sample();
                let error = open_meeting(&object_store, late).await.unwrap_err();
                let path = fixtures.object_path(START_SEQ);
                assert_start_fails(
                    &fixtures,
                    before,
                    Counters {
                        create_conflicts: 1,
                        ..Counters::default()
                    },
                    error,
                    |error| matches!(error, Error::WalObjectConflict { path: actual, .. } if *actual == path),
                    RetryHint::NonRetryable,
                )
                .await;
            }
        }

        async fn start_object_meets_an_identical_object() {
            let object_store = memory_store();
            let fixtures = put_chain_start(&object_store).await;
            let same = encode_object(chain_header(START_SEQ, START_SEQ + 1, Some((0, 1))), &[])
                .unwrap()
                .bytes;
            let before = Counters::sample();
            let error = open_meeting(&object_store, same).await.unwrap_err();
            assert_start_fails(
                &fixtures,
                before,
                Counters {
                    created_objects: 1,
                    ..Counters::default()
                },
                error,
                |error| matches!(error, Error::UnconfirmedWalEpochStart { epoch, .. } if *epoch == START_SEQ + 1),
                RetryHint::Retryable,
            )
            .await;
        }

        async fn start_object_create_has_an_unknown_outcome() {
            let object_store = memory_store();
            let fixtures = put_chain_start(&object_store).await;
            let io = Arc::new(LostResponseIo {
                inner: ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap(),
                lose_next: AtomicBool::new(true),
            });
            let before = Counters::sample();
            let error = ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string())
                .await
                .unwrap_err();
            assert_start_fails(
                &fixtures,
                before,
                Counters {
                    create_failures: 1,
                    ..Counters::default()
                },
                error,
                |error| matches!(error, Error::WalObjectStore { .. }),
                RetryHint::Retryable,
            )
            .await;
        }

        async fn assert_appends_stopped(
            appends: Vec<tokio::task::JoinHandle<Result<AppendBatchResponse>>>,
        ) {
            for append in appends {
                assert_stopped(&timeout(WAIT, append).await.unwrap().unwrap().unwrap_err());
            }
        }

        /// Opens and stops a store on an empty prefix, which sets both
        /// process-wide indexed gauges to its one start object and moves the
        /// recovery and create counters, so a case that abandons a recovery
        /// can tell the gauges standing still from a publication of what its
        /// prefix holds. It runs before the case samples `Counters`.
        async fn initialize_indexed_gauges() -> (i64, i64) {
            let store = open(memory_store(), &eager()).await;
            store.stop().await.unwrap();
            let gauges = indexed_gauges();
            assert_eq!((1, object_len(&[]) as i64), gauges);
            gauges
        }

        /// Opens a store over `io` and asserts that the recovery was
        /// abandoned: `open` fails with an error `recognise` accepts and
        /// `retry_hint`, no start object is written so the prefix still holds
        /// `present`, both gauges stand still and no metric moves.
        async fn assert_recovery_abandoned(
            io: Arc<dyn WalObjectIo>,
            present: &[u64],
            recognise: impl Fn(&Error) -> bool,
            retry_hint: RetryHint,
        ) {
            let gauges = initialize_indexed_gauges().await;
            let before = Counters::sample();
            let error = ObjectStoreLogStore::open(io.clone(), &eager(), PREFIX.to_string())
                .await
                .unwrap_err();
            assert!(recognise(&error), "unexpected error: {error:?}");
            assert_eq!(retry_hint, error.retry_hint());
            assert_eq!(present, object_seqs(io.as_ref()).await.as_slice());
            assert_eq!(gauges, indexed_gauges());
            assert_eq!(Counters::default(), Counters::since(before));
        }

        /// Writes object `object_seq` holding one entry of region 1 with
        /// `data`, chained to the object below it.
        async fn put_entry(object_store: &ObjectStore, object_seq: u64, data: &str) {
            let record = Record {
                region_id: region(1),
                entry_id: id(object_seq, 1),
                payload: Bytes::from(data.as_bytes().to_vec()),
            };
            put_records(object_store, object_seq, &[record]).await;
        }

        async fn listing_fails_at_recovery() {
            let object_store = memory_store();
            put_entry(&object_store, 0, "a0").await;
            put_entry(&object_store, 1, "a1").await;
            let (io, _) = RecordingIo::over(object_store);
            io.fail_next_list.store(true, Ordering::Relaxed);
            assert_recovery_abandoned(
                io.clone(),
                &[0, 1],
                |error| {
                    matches!(
                        error,
                        Error::WalObjectStore {
                            operation: "list",
                            ..
                        }
                    )
                },
                RetryHint::Retryable,
            )
            .await;
            assert!(!io.fail_next_list.load(Ordering::Relaxed));

            let before = Counters::sample();
            let store = open_over(io.clone(), &eager()).await;
            assert_indexed(io.as_ref(), &[0, 1, 2 + UNLISTED_SEQUENCES]).await;
            assert_eq!(
                expected_entries(region(1), &[(id(0, 1), "a0"), (id(1, 1), "a1")]),
                read_entries(&store, region(1), 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 1,
                    recovered_objects: 2,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        async fn trailer_fetch_fails_at_recovery() {
            fetch_fails_at_recovery(false).await;
        }

        async fn footer_fetch_fails_at_recovery() {
            fetch_fails_at_recovery(true).await;
        }

        /// The range read of the tail window, or of the footer, of the one
        /// object fails; the next open recovers it.
        async fn fetch_fails_at_recovery(footer: bool) {
            let object_store = memory_store();
            let (tail_start, footer_offset) = put_wide_object(&object_store).await;
            let (io, _) = RecordingIo::over(object_store);
            let offset = if footer { footer_offset } else { tail_start };
            *io.fail_range_read_at.lock().unwrap() = Some((0, offset));
            let path = io.object_path(0);
            assert_recovery_abandoned(
                io.clone(),
                &[0],
                |error| {
                    matches!(error, Error::WalObjectStore { operation: "read", path: actual, .. } if *actual == path)
                },
                RetryHint::Retryable,
            )
            .await;
            assert!(io.fail_range_read_at.lock().unwrap().is_none());

            let before = Counters::sample();
            let store = open_over(io.clone(), &eager()).await;
            assert_indexed(io.as_ref(), &[0, START_SEQ]).await;
            assert_eq!(
                expected_entries(region(1), &[(1, "wide")]),
                read_entries(&store, region(1), 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 1,
                    recovered_objects: 1,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The entry ranges of the region do not increase with the sequence,
        /// which the catalog refuses once every footer is read.
        async fn catalog_rejects_the_recovery() {
            let object_store = memory_store();
            put_object(&object_store, 0, region(1), &[id(3, 1), id(3, 2)]).await;
            put_object(&object_store, 1, region(1), &[id(2, 1)]).await;
            let io = Arc::new(ObjectStoreIo::new(object_store, PREFIX).unwrap());
            let path = io.object_path(1);
            assert_recovery_abandoned(
                io.clone(),
                &[0, 1],
                |error| {
                    matches!(error, Error::InvalidWalObject { path: actual, source, .. }
                        if *actual == path && matches!(**source, Error::CorruptedWalObject { .. }))
                },
                RetryHint::NonRetryable,
            )
            .await;
        }

        /// Writes the chain 0 to 3 of epoch 1, one entry of region 1 each,
        /// and `WIDE_REGIONS` more regions per object when `wide`; objects 2
        /// and 3 carry reclaim boundary 2, so object 1 is below the boundary
        /// of the tip and object 2 is not.
        async fn put_bounded_chain(object_store: &ObjectStore, wide: bool) {
            let io = ObjectStoreIo::new(object_store.clone(), PREFIX).unwrap();
            for object_seq in 0..=3 {
                let header = Header {
                    reclaim_boundary: if object_seq >= 2 { 2 } else { 0 },
                    ..chain_header(object_seq, 1, (object_seq > 0).then(|| (object_seq - 1, 1)))
                };
                let mut records = vec![Record {
                    region_id: region(1),
                    entry_id: id(object_seq, 1),
                    payload: Bytes::from(format!("a{object_seq}")),
                }];
                if wide {
                    records.extend((2..=WIDE_REGIONS).map(|number| Record {
                        region_id: region(number),
                        entry_id: id(object_seq, 1),
                        payload: Bytes::from_static(b"wide"),
                    }));
                }
                put_object_with_header(&io, header, &records).await;
            }
        }

        /// The fetch of recovery at which an object is found gone.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum RecoveryFetchStage {
            /// The read of a short object as a whole.
            WholeObject,
            /// The reads of the header and of the tail window of a wide object.
            Header,
            /// The separate read of the footer of a wide object.
            Footer,
        }

        /// Waits for `count` parked operations and returns their releases.
        async fn parked_releases(
            parked: &mut mpsc::UnboundedReceiver<(u64, oneshot::Sender<bool>)>,
            count: usize,
        ) -> Vec<oneshot::Sender<bool>> {
            let mut releases = Vec::with_capacity(count);
            for _ in 0..count {
                releases.push(next_parked_operation(parked).await.1);
            }
            releases
        }

        /// Every fetch of the listed objects parks and one object is deleted
        /// before one of its reads: below the boundary of the tip the object
        /// is ignored, at or above it the construction fails.
        async fn object_vanishes_before_its_fetch() {
            for (vanished, fails) in [(1, false), (2, true)] {
                for stage in [
                    RecoveryFetchStage::WholeObject,
                    RecoveryFetchStage::Header,
                    RecoveryFetchStage::Footer,
                ] {
                    object_vanishes_at(vanished, fails, stage).await;
                }
            }
        }

        async fn object_vanishes_at(vanished: u64, fails: bool, stage: RecoveryFetchStage) {
            let object_store = memory_store();
            put_bounded_chain(&object_store, stage != RecoveryFetchStage::WholeObject).await;
            let (io, mut parked) = ParkedIo::over(object_store.clone());
            io.range_reads_parked.store(true, Ordering::SeqCst);
            let gauges = initialize_indexed_gauges().await;
            let before = Counters::sample();
            let opening = {
                let io = io.clone();
                tokio::spawn(async move {
                    ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
                })
            };
            let path = object_path(&object_store, vanished);
            let delete = || object_store.delete(&path);
            let release_all = |releases: Vec<oneshot::Sender<bool>>| {
                for release in releases {
                    release.send(true).unwrap();
                }
            };
            // A short object parks once, a wide one twice, at its header and
            // at its tail window, and then once more at its footer.
            let whole = stage == RecoveryFetchStage::WholeObject;
            let first = parked_releases(&mut parked, if whole { 4 } else { 8 }).await;
            if stage != RecoveryFetchStage::Footer {
                delete().await.unwrap();
            }
            release_all(first);
            match stage {
                RecoveryFetchStage::WholeObject => {}
                RecoveryFetchStage::Header => release_all(parked_releases(&mut parked, 3).await),
                RecoveryFetchStage::Footer => {
                    let footers = parked_releases(&mut parked, 4).await;
                    delete().await.unwrap();
                    release_all(footers);
                }
            }
            let opened = timeout(WAIT, opening).await.unwrap().unwrap();
            io.range_reads_parked.store(false, Ordering::SeqCst);
            if fails {
                let error = opened.unwrap_err();
                assert!(
                    is_not_found(&error)
                        && matches!(&error, Error::WalObjectStore { path, .. } if *path == io.object_path(2)),
                    "unexpected error: {error:?}"
                );
                assert_eq!(vec![0, 1, 3], object_seqs(io.as_ref()).await);
                assert_eq!(gauges, indexed_gauges());
                assert_eq!(Counters::default(), Counters::since(before));
                return;
            }
            let store = opened.unwrap();
            let start_seq = 4 + UNLISTED_SEQUENCES;
            assert_eq!(vec![0, 2, 3, start_seq], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[2, 3, start_seq]).await;
            assert_eq!(
                expected_entries(region(1), &[(id(2, 1), "a2"), (id(3, 1), "a3")]),
                read_entries(&store, region(1), 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 1,
                    recovered_objects: 3,
                    recoveries: 1,
                    reads: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        async fn segment_fetch_fails_at_a_read() {
            let (io, _) = RecordingIo::over(memory_store());
            let store = open_over(io.clone(), &eager()).await;
            let before = Counters::sample();
            let region_id = region(1);
            append(&store, region_id, "a1").await.unwrap();
            append(&store, region_id, "a2").await.unwrap();
            assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
            let (path, segment) = store
                .segment_location(&provider(region_id), 1)
                .unwrap()
                .unwrap();
            *io.fail_range_read_at.lock().unwrap() = Some((1, segment.start));
            let error = store
                .read(&provider(region_id), 1)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::WalObjectStore { operation: "read", path: actual, .. } if *actual == path),
                "unexpected error: {error:?}"
            );
            assert_eq!(RetryHint::Retryable, error.retry_hint());
            assert!(io.fail_range_read_at.lock().unwrap().is_none());
            assert!(store.wal_holes(&provider(region_id)).unwrap().is_empty());
            assert!(terminal(&store.terminal_error).is_none());
            assert_indexed(io.as_ref(), &[1, 2]).await;
            let written = Counters {
                created_objects: 2,
                seal_to_durable: 2,
                sealed_objects: 2,
                object_bytes: 2 * one_entry_object(),
                object_entries: 2,
                acknowledged_appends: 2,
                deleted_objects: 1,
                ..Counters::default()
            };
            assert_eq!(written, Counters::since(before));

            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1"), (id(2, 1), "a2")]),
                read_entries(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    reads: 1,
                    ..written
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The delete of the start object fails at the collection that
        /// indexing object 1 starts; a watermark that leaves the boundary
        /// where it is starts the next collection, which deletes it.
        async fn delete_fails_at_a_collection() {
            let (io, mut parked) = ParkedIo::parking_creates(memory_store());
            let store = open_over(io.clone(), &eager()).await;
            io.deletes_parked.store(true, Ordering::SeqCst);
            let before = Counters::sample();
            let region_id = region(1);
            append(&store, region_id, "a1").await.unwrap();
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(0, object_seq);
            release.send(false).unwrap();
            timeout(WAIT, store.wait_for_garbage_collection())
                .await
                .unwrap();
            assert!(store.catalog.read().unwrap().contains_object(0));
            assert!(terminal(&store.terminal_error).is_none());
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0, 1]).await;
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1")]),
                read_entries(&store, region_id, 1).await
            );
            let written = Counters {
                created_objects: 1,
                seal_to_durable: 1,
                sealed_objects: 1,
                object_bytes: one_entry_object(),
                object_entries: 1,
                acknowledged_appends: 1,
                reads: 1,
                failed_deletes: 1,
                ..Counters::default()
            };
            assert_eq!(written, Counters::since(before));

            store
                .obsolete(&provider(region_id), region_id, 0)
                .await
                .unwrap();
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(0, object_seq);
            release.send(true).unwrap();
            assert_eq!(vec![1], collected_object_seqs(&store).await);
            assert!(!store.catalog.read().unwrap().contains_object(0));
            assert_indexed(io.as_ref(), &[1]).await;
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1")]),
                read_entries(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    reads: 2,
                    deleted_objects: 1,
                    ..written
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// `stop` returns only after the delete in flight completed, and the
        /// catalog then holds exactly what the prefix holds, whether the
        /// delete succeeded or failed.
        async fn stop_waits_for_a_delete_in_flight() {
            for deleted in [true, false] {
                let (io, mut parked) = ParkedIo::parking_creates(memory_store());
                let store = open_over(io.clone(), &eager()).await;
                io.deletes_parked.store(true, Ordering::SeqCst);
                let before = Counters::sample();
                let region_id = region(1);
                append(&store, region_id, "a1").await.unwrap();
                let (object_seq, release) = next_parked_operation(&mut parked).await;
                assert_eq!(0, object_seq);
                // The first poll queues the stop command; the command queued
                // behind it is answered once the stop was handled, and the
                // delete in flight still holds the answer to the stop back.
                let mut stop = Box::pin(store.stop());
                assert!((&mut stop).now_or_never().is_none());
                round_trip_stopping_actor(&store).await;
                assert!((&mut stop).now_or_never().is_none());
                release.send(deleted).unwrap();
                timeout(WAIT, stop).await.unwrap().unwrap();

                let present = if deleted { vec![1] } else { vec![0, 1] };
                assert_eq!(present, object_seqs(io.as_ref()).await);
                let indexed = store
                    .catalog
                    .read()
                    .unwrap()
                    .objects_in_order()
                    .map(|(object_seq, _)| object_seq)
                    .collect::<Vec<_>>();
                assert_eq!(present, indexed);
                assert_indexed(io.as_ref(), &present).await;
                assert!(terminal(&store.terminal_error).is_none());
                assert_eq!(
                    expected_entries(region_id, &[(id(1, 1), "a1")]),
                    read_entries(&store, region_id, 1).await
                );
                assert_eq!(
                    Counters {
                        created_objects: 1,
                        seal_to_durable: 1,
                        sealed_objects: 1,
                        object_bytes: one_entry_object(),
                        object_entries: 1,
                        acknowledged_appends: 1,
                        reads: 1,
                        deleted_objects: deleted as u64,
                        failed_deletes: (!deleted) as u64,
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
            }
        }

        /// An append that alone runs past the position range of its region
        /// is refused; the open batch it sealed first, which holds another
        /// region too, is acknowledged, and the store keeps serving.
        async fn region_exhausts_its_entry_ids() {
            let store = open(memory_store(), &manual()).await;
            let before = Counters::sample();
            let (region_id, other) = (region(1), region(2));
            let shared = spawn_append_batch(
                &store,
                vec![entry(&store, other, "b1"), entry(&store, region_id, "a1")],
            );
            store.wait_for_admitted_appends(1).await.unwrap();
            let exhausting = (0..POSITION_LIMIT)
                .map(|_| entry(&store, region_id, ""))
                .collect::<Vec<_>>();
            let error = store.append_batch(exhausting).await.unwrap_err();
            assert!(
                matches!(error, Error::WalEntryPositionExhausted { region_id: actual, .. } if actual == region_id),
                "unexpected error: {error:?}"
            );
            let response = timeout(WAIT, shared).await.unwrap().unwrap().unwrap();
            assert_eq!(
                HashMap::from([(other, id(1, 1)), (region_id, id(1, 1))]),
                response.last_entry_ids
            );
            assert!(terminal(&store.terminal_error).is_none());

            let next = spawn_append_batch(&store, vec![entry(&store, region_id, "a2")]);
            store.wait_for_admitted_appends(2).await.unwrap();
            store.seal_open_batch().await.unwrap();
            let response = timeout(WAIT, next).await.unwrap().unwrap().unwrap();
            assert_eq!(Some(&id(2, 1)), response.last_entry_ids.get(&region_id));
            assert_eq!(vec![1, 2], collected_object_seqs(&store).await);
            assert_indexed(store.io.as_ref(), &[1, 2]).await;
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1"), (id(2, 1), "a2")]),
                read_entries(&store, region_id, 1).await
            );
            assert_eq!(
                expected_entries(other, &[(id(1, 1), "b1")]),
                read_entries(&store, other, 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: object_bytes_of(&[(other, "b1"), (region_id, "a1")])
                        + one_entry_object(),
                    object_entries: 3,
                    acknowledged_appends: 2,
                    reads: 2,
                    deleted_objects: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// Round-trips a command through the actor once `stop` began, when
        /// the seal command is refused: the actor refuses the release that
        /// `obsolete_all` of a region without entries requests as well, which
        /// hides nothing of the regions a case reads.
        async fn round_trip_stopping_actor(store: &ObjectStoreLogStore) {
            let region_id = region(9);
            assert_stopped(
                &store
                    .obsolete_all(&provider(region_id), region_id)
                    .await
                    .unwrap_err(),
            );
        }

        /// Four creates are in flight and a fifth batch waits for a slot when
        /// `stop` begins: stop uploads the whole backlog. Objects 4 to 2 and
        /// then the fifth complete while object 1 stays parked, and nothing is
        /// indexed; then object 1 makes all five durable in order, in both
        /// modes.
        async fn creates_succeed_after_stop_began() {
            for config in [eager(), enqueued(eager())] {
                let durable = config.ack_mode == AckMode::Durable;
                let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
                let before = Counters::sample();
                let region_id = region(1);
                let appends = spawn_appends(&store, region_id, MAX_IN_FLIGHT_CREATES + 1).await;
                let mut releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
                let stop = {
                    let store = store.clone();
                    tokio::spawn(async move { store.stop().await })
                };
                wait_until(|| store.stopped.load(Ordering::Acquire)).await;
                for object_seq in (2..=MAX_IN_FLIGHT_CREATES as u64).rev() {
                    releases.remove(&object_seq).unwrap().send(true).unwrap();
                }
                let (object_seq, fifth) = next_parked_operation(&mut parked).await;
                assert_eq!(MAX_IN_FLIGHT_CREATES as u64 + 1, object_seq);
                fifth.send(true).unwrap();
                wait_until(|| {
                    Counters::since(before).created_objects == MAX_IN_FLIGHT_CREATES as u64
                })
                .await;
                assert_eq!(0, latest(&store, region_id));
                assert_indexed(io.as_ref(), &[0]).await;

                releases.remove(&1).unwrap().send(true).unwrap();
                timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
                for (append, object_seq) in appends.into_iter().zip(1..) {
                    let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
                    assert_eq!(
                        Some(&id(object_seq, 1)),
                        response.last_entry_ids.get(&region_id)
                    );
                }

                // Nothing is collected once stop began.
                let sealed = MAX_IN_FLIGHT_CREATES as u64 + 1;
                let objects = (0..=sealed).collect::<Vec<_>>();
                assert_eq!(objects, object_seqs(io.as_ref()).await);
                assert_indexed(io.as_ref(), &objects).await;
                assert!(terminal(&store.terminal_error).is_none());
                assert_eq!(id(sealed, 1), latest(&store, region_id));
                assert_eq!(
                    Counters {
                        created_objects: sealed,
                        seal_to_durable: sealed,
                        sealed_objects: sealed,
                        object_bytes: sealed * one_entry_object(),
                        object_entries: sealed,
                        acknowledged_appends: if durable { sealed } else { 0 },
                        ..Counters::default()
                    },
                    Counters::since(before)
                );
            }
        }

        /// Object 2 is created and object 3 in flight when object 1 meets
        /// another writer after `stop` began: the store is poisoned, every
        /// writer receives the stopped error and `stop` reports the conflict
        /// once the creates in flight completed.
        async fn create_conflicts_after_stop_began() {
            let object_store = memory_store();
            let (store, io, mut parked) =
                open_parking_creates(object_store.clone(), &eager()).await;
            put_foreign(&object_store, 1, 1).await;
            let before = Counters::sample();
            let region_id = region(1);
            let appends = spawn_appends(&store, region_id, 3).await;
            let mut releases = parked_operations(&mut parked, 3).await;
            let stop = {
                let store = store.clone();
                tokio::spawn(async move { store.stop().await })
            };
            wait_until(|| store.stopped.load(Ordering::Acquire)).await;
            releases.remove(&2).unwrap().send(true).unwrap();
            wait_until(|| Counters::since(before).created_objects == 1).await;
            releases.remove(&1).unwrap().send(true).unwrap();
            assert_appends_stopped(appends).await;
            assert!(matches!(
                terminal(&store.terminal_error).as_deref(),
                Some(Error::WalObjectConflict { .. })
            ));
            assert_read_is_terminal(&store, region_id).await;
            assert!(!stop.is_finished());
            assert_indexed(io.as_ref(), &[0]).await;

            releases.remove(&3).unwrap().send(true).unwrap();
            let error = timeout(WAIT, stop).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(vec![0, 1, 2, 3], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    seal_to_durable: 2,
                    create_conflicts: 1,
                    poisoned: 1,
                    sealed_objects: 3,
                    object_bytes: 3 * one_entry_object(),
                    object_entries: 3,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        /// Seven sealed batches leave no room for the two an admission can
        /// seal: the next append is held back and counted as stalled, and it
        /// is admitted once object 1 is durable, which records its wait, in
        /// both modes.
        async fn append_held_back_by_full_sealed_batches() {
            for config in [eager(), enqueued(eager())] {
                append_held_back_by_full_sealed_batches_with(config).await;
            }
        }

        async fn append_held_back_by_full_sealed_batches_with(config: ObjectStoreWalConfig) {
            let durable = config.ack_mode == AckMode::Durable;
            let (store, io, mut parked) = open_parking_creates(memory_store(), &config).await;
            let before = Counters::sample();
            let region_id = region(1);
            let mut appends = spawn_appends(&store, region_id, MAX_SEALED_BATCHES - 1).await;
            appends.push(spawn_append_batch(
                &store,
                vec![entry(&store, region_id, "h1")],
            ));
            let releases = parked_operations(&mut parked, MAX_IN_FLIGHT_CREATES).await;
            wait_until(|| Counters::since(before).stalled_appends == 1).await;
            assert_eq!(0, Counters::since(before).stalled_waits);

            for release in releases.into_values() {
                release.send(true).unwrap();
            }
            for _ in MAX_IN_FLIGHT_CREATES..MAX_SEALED_BATCHES {
                next_parked_operation(&mut parked)
                    .await
                    .1
                    .send(true)
                    .unwrap();
            }
            for (index, append) in appends.into_iter().enumerate() {
                let response = timeout(WAIT, append).await.unwrap().unwrap().unwrap();
                assert_eq!(
                    Some(&id(index as u64 + 1, 1)),
                    response.last_entry_ids.get(&region_id)
                );
            }
            let sealed = MAX_SEALED_BATCHES as u64;
            // An `enqueued` append returned before its object was durable.
            timeout(
                WAIT,
                store.wait_durable(&provider(region_id), id(sealed, 1)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                (1..=sealed).collect::<Vec<_>>(),
                collected_object_seqs(&store).await
            );
            assert_indexed(io.as_ref(), &(1..=sealed).collect::<Vec<_>>()).await;
            assert_eq!(
                Counters {
                    created_objects: sealed,
                    seal_to_durable: sealed,
                    sealed_objects: sealed,
                    object_bytes: sealed * one_entry_object(),
                    object_entries: sealed,
                    acknowledged_appends: if durable { sealed } else { 0 },
                    deleted_objects: 1,
                    stalled_appends: 1,
                    stalled_waits: 1,
                    ..Counters::default()
                },
                Counters::since(before)
            );
            store.stop().await.unwrap();
        }

        /// The `enqueued` configuration under which the second append stalls
        /// behind the create of the first.
        fn stalling() -> ObjectStoreWalConfig {
            ObjectStoreWalConfig {
                max_unpersisted_bytes: ReadableSize(1),
                ..enqueued(manual())
            }
        }

        /// What `stall_second_append` leaves in the metrics.
        fn stalled_once() -> Counters {
            Counters {
                created_objects: 1,
                sealed_objects: 1,
                object_bytes: one_entry_object(),
                object_entries: 1,
                recoveries: 1,
                stalled_appends: 1,
                ..Counters::default()
            }
        }

        /// The first stalled append is admitted once the upload completes;
        /// the entry it admits puts the backlog at the threshold again, so
        /// the next append stalls behind the create it seals, and the stop
        /// releases it with the stopped error. Both waits are timed.
        async fn stalled_appends_released_by_admission_and_by_a_stop() {
            const SHORT: Duration = Duration::from_millis(20);
            let region_id = region(1);
            let before = Counters::sample();
            let stalled_at = Instant::now();
            let (store, io, mut parked, stalled, release) = stall_second_append(stalling()).await;
            assert_eq!(stalled_once(), Counters::since(before));

            let timer = timer_before(&METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS);
            let holding_since = Instant::now();
            tokio::time::sleep(SHORT).await;
            let held = holding_since.elapsed();
            release.send(true).unwrap();
            let response = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap();
            assert_eq!(Some(&id(2, 1)), response.last_entry_ids.get(&region_id));
            let admitted = Timed::since(
                &METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
                timer,
                held,
                stalled_at.elapsed(),
            );
            assert_eq!(vec![1], collected_object_seqs(&store).await);

            let stalled_at = Instant::now();
            let refused = spawn_append_batch(&store, vec![entry(&store, region_id, "a3")]);
            let (object_seq, release) = next_parked_operation(&mut parked).await;
            assert_eq!(2, object_seq);
            let timer = timer_before(&METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS);
            let holding_since = Instant::now();
            tokio::time::sleep(admitted.elapsed + SHORT).await;
            let held = holding_since.elapsed();
            let stop = {
                let store = store.clone();
                tokio::spawn(async move { store.stop().await })
            };
            assert_stopped(&timeout(WAIT, refused).await.unwrap().unwrap().unwrap_err());
            let refused = Timed::since(
                &METRIC_OBJECT_STORE_WAL_STALLED_APPEND_SECONDS,
                timer,
                held,
                stalled_at.elapsed(),
            );
            Timed::assert_pair("stalled append", admitted, refused);

            release.send(true).unwrap();
            timeout(WAIT, stop).await.unwrap().unwrap().unwrap();
            assert_eq!(vec![1, 2], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[1, 2]).await;
            assert!(terminal(&store.terminal_error).is_none());
            assert_eq!(
                expected_entries(region_id, &[(id(1, 1), "a1"), (id(2, 1), "a2")]),
                read_entries(&store, region_id, 1).await
            );
            assert_eq!(
                Counters {
                    created_objects: 3,
                    seal_to_durable: 2,
                    sealed_objects: 2,
                    object_bytes: 2 * one_entry_object(),
                    object_entries: 2,
                    recoveries: 1,
                    reads: 1,
                    deleted_objects: 1,
                    stalled_appends: 2,
                    stalled_waits: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }

        /// The create the stalled append waits for meets another writer,
        /// which poisons the `enqueued` store: the append is released with
        /// the terminal error and its wait is recorded.
        async fn stalled_append_released_by_a_terminal_error() {
            let region_id = region(1);
            let before = Counters::sample();
            let (store, io, _parked, stalled, release) = stall_second_append(stalling()).await;
            assert_eq!(stalled_once(), Counters::since(before));
            let foreign = Header {
                object_seq: 1,
                epoch: 1,
                prev: None,
                reclaim_boundary: 0,
            };
            put_object_with_header(&io.inner, foreign, &[]).await;
            release.send(true).unwrap();
            let error = timeout(WAIT, stalled).await.unwrap().unwrap().unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert_read_is_terminal(&store, region_id).await;
            let error = store.stop().await.unwrap_err();
            assert!(
                matches!(unwrap_shared(&error), Error::WalObjectConflict { .. }),
                "unexpected error: {error:?}"
            );
            assert_eq!(vec![0, 1], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    create_conflicts: 1,
                    poisoned: 1,
                    stalled_waits: 1,
                    ..stalled_once()
                },
                Counters::since(before)
            );
        }

        /// The store is dropped without `stop` while the append is stalled:
        /// the actor's teardown releases it and records its wait, and the
        /// parked create never runs.
        async fn stalled_append_released_by_the_teardown() {
            let before = Counters::sample();
            let (store, io, _parked, stalled, _release) = stall_second_append(stalling()).await;
            assert_eq!(stalled_once(), Counters::since(before));
            assert_indexed(io.as_ref(), &[0]).await;

            stalled.abort();
            drop(store);
            wait_until(|| LIVE_ACTORS.load(Ordering::SeqCst) == 0).await;
            assert_eq!(vec![0], object_seqs(io.as_ref()).await);
            assert_indexed(io.as_ref(), &[0]).await;
            assert_eq!(
                Counters {
                    stalled_waits: 1,
                    ..stalled_once()
                },
                Counters::since(before)
            );
        }

        /// Every fetch of recovery and of a read parks until the case releases
        /// it, which is how the recovery and read timers are read for the
        /// durations they recorded. Each is observed around two holds, the
        /// second longer than anything the first observations took.
        async fn object_store_answers_slowly() {
            const SHORT: Duration = Duration::from_millis(20);
            let object_store = memory_store();
            put_entry(&object_store, 0, "a0").await;
            let (io, mut parked) = ParkedIo::over(object_store);
            let before = Counters::sample();
            let region_id = region(1);
            let mut recoveries = Vec::new();
            let mut reads = Vec::new();
            let mut hold = SHORT;
            for objects in 1..=2 {
                let timer = timer_before(&METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS);
                let started_at = Instant::now();
                let opening = {
                    let io = io.clone();
                    tokio::spawn(async move {
                        ObjectStoreLogStore::open(io, &eager(), PREFIX.to_string()).await
                    })
                };
                // Every object is short enough to be read whole, and every
                // such read parks.
                let releases = parked_operations(&mut parked, objects).await;
                let parked_at = Instant::now();
                tokio::time::sleep(hold).await;
                let held = parked_at.elapsed();
                for release in releases.into_values() {
                    release.send(true).unwrap();
                }
                let store = timeout(WAIT, opening).await.unwrap().unwrap().unwrap();
                recoveries.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_RECOVERY_SECONDS,
                    timer,
                    held,
                    started_at.elapsed(),
                ));

                io.range_reads_parked.store(true, Ordering::SeqCst);
                let timer = timer_before(&METRIC_OBJECT_STORE_WAL_READ_SECONDS);
                let started_at = Instant::now();
                let reading = {
                    let store = store.clone();
                    tokio::spawn(async move { read_entries(&store, region_id, 1).await })
                };
                let (object_seq, release) = next_parked_operation(&mut parked).await;
                assert_eq!(0, object_seq);
                let parked_at = Instant::now();
                tokio::time::sleep(hold).await;
                let held = parked_at.elapsed();
                release.send(true).unwrap();
                let entries = timeout(WAIT, reading).await.unwrap().unwrap();
                reads.push(Timed::since(
                    &METRIC_OBJECT_STORE_WAL_READ_SECONDS,
                    timer,
                    held,
                    started_at.elapsed(),
                ));
                io.range_reads_parked.store(false, Ordering::SeqCst);
                assert_eq!(expected_entries(region_id, &[(id(0, 1), "a0")]), entries);
                store.stop().await.unwrap();
                hold = recoveries[0].elapsed.max(reads[0].elapsed) + SHORT;
            }
            Timed::assert_pair("recovery", recoveries[0], recoveries[1]);
            Timed::assert_pair("read", reads[0], reads[1]);
            // Each open starts above the sequences the writer of the newest
            // listed object can have handed out.
            assert_indexed(
                io.as_ref(),
                &[0, 1 + UNLISTED_SEQUENCES, 2 * (1 + UNLISTED_SEQUENCES)],
            )
            .await;
            assert_eq!(
                Counters {
                    created_objects: 2,
                    recovered_objects: 3,
                    recoveries: 2,
                    reads: 2,
                    ..Counters::default()
                },
                Counters::since(before)
            );
        }
    }
}

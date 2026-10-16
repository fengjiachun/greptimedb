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

use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use api::v1::{BulkWalEntry, Mutation, OpType, Rows, WalEntry, WriteHint};
use futures::stream::{FuturesUnordered, StreamExt};
use snafu::{IntoError, ResultExt};
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{
    AbandonedWalWriteSnafu, Error, Result, UnconfirmedWalWriteSnafu, WriteGroupSnafu,
};
use crate::memtable::KeyValues;
use crate::memtable::bulk::part::BulkPart;
use crate::metrics;
use crate::region::version::{VersionControlData, VersionControlRef, VersionRef};
use crate::request::OptionOutputTx;
use crate::wal::{DurabilityBarrier, EntryId, WalWriter};

/// Notifier to notify write result on drop.
struct WriteNotify {
    /// Error to send to the waiter.
    err: Option<Arc<Error>>,
    /// Whether the write goes into the WAL entry of its context.
    in_wal: bool,
    /// Whether `err` is the error the writer is answered with unless its WAL
    /// entry is confirmed durable, see [`RegionWriteCtx::defer_acks`].
    awaiting_durable: bool,
    /// Sender to send write result to the waiter for this mutation.
    sender: OptionOutputTx,
    /// Number of rows to be written.
    num_rows: usize,
}

impl WriteNotify {
    /// Creates a new notify from the `sender`.
    fn new(sender: OptionOutputTx, num_rows: usize, in_wal: bool) -> WriteNotify {
        WriteNotify {
            err: None,
            in_wal,
            awaiting_durable: false,
            sender,
            num_rows,
        }
    }

    /// Answers the writer with `err` whatever its WAL entry becomes.
    fn fail(&mut self, err: Arc<Error>) {
        self.err = Some(err);
        self.awaiting_durable = false;
    }

    /// Makes the writer wait for its WAL entry to be confirmed durable:
    /// until then it is answered with `unconfirmed`.
    fn await_durable(&mut self, unconfirmed: Arc<Error>) {
        if self.err.is_none() {
            self.err = Some(unconfirmed);
            self.awaiting_durable = true;
        }
    }

    /// Settles the wait for the WAL entry: a writer still waiting is answered
    /// with success once the entry is durable, and with `failure` otherwise.
    fn settle_durable(&mut self, failure: Option<&Arc<Error>>) {
        if self.awaiting_durable {
            self.err = failure.cloned();
            self.awaiting_durable = false;
        }
    }

    /// Send result to the waiter.
    fn notify_result(&mut self) {
        if let Some(err) = &self.err {
            // Try to send the error to waiters.
            self.sender
                .send_mut(Err(err.clone()).context(WriteGroupSnafu));
        } else {
            // Send success result.
            self.sender.send_mut(Ok(self.num_rows));
        }
    }
}

impl Drop for WriteNotify {
    fn drop(&mut self) {
        self.notify_result();
    }
}

/// Source of writes installed into memtables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteSource {
    Request,
    WalReplay,
}

/// Context to keep region metadata and buffer write requests.
pub(crate) struct RegionWriteCtx {
    /// Source of the buffered writes.
    source: WriteSource,
    /// Whether accepted non-empty writes bypass WAL. WAL replay never sets this flag.
    has_skip_wal_writes: bool,
    /// Id of region to write.
    region_id: RegionId,
    /// Version of the region while creating the context.
    version: VersionRef,
    /// VersionControl of the region.
    version_control: VersionControlRef,
    /// Next sequence number to write.
    ///
    /// The context assigns a unique sequence number for each row.
    next_sequence: SequenceNumber,
    /// Next entry id of WAL to write.
    next_entry_id: EntryId,
    /// Valid WAL entry to write.
    ///
    /// We keep [WalEntry] instead of mutations to avoid taking mutations
    /// out of the context to construct the wal entry when we write to the wal.
    wal_entry: WalEntry,
    /// Mutations that skip WAL, paired with their write notifiers.
    memtable_mutations: Vec<(Mutation, WriteNotify)>,
    /// The barrier and the last entry id of the region that the writers of
    /// the WAL entry wait for, set once the entry is appended, see
    /// [`defer_acks`](Self::defer_acks).
    deferred_ack: Option<(DurabilityBarrier, EntryId)>,
    /// Wal options of the region being written to.
    provider: Provider,
    /// Notifiers to send write results to waiters.
    ///
    /// The i-th notify is for the i-th mutation in `wal_entry`.
    wal_notifiers: Vec<WriteNotify>,
    /// Notifiers for bulk requests.
    bulk_notifiers: Vec<WriteNotify>,
    /// Pending bulk write requests
    pub(crate) bulk_parts: Vec<BulkPart>,
    /// The write operation is failed and we should not write to the mutable memtable.
    failed: bool,

    // Metrics:
    /// Rows to put.
    pub(crate) put_num: usize,
    /// Rows to delete.
    pub(crate) delete_num: usize,
    /// The total bytes written to the region.
    pub(crate) written_bytes: Option<Arc<AtomicU64>>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    pub(crate) fn new(
        region_id: RegionId,
        version_control: &VersionControlRef,
        provider: Provider,
        written_bytes: Option<Arc<AtomicU64>>,
        source: WriteSource,
    ) -> RegionWriteCtx {
        let VersionControlData {
            version,
            committed_sequence,
            last_entry_id,
            ..
        } = version_control.current();

        RegionWriteCtx {
            source,
            has_skip_wal_writes: false,
            region_id,
            version,
            version_control: version_control.clone(),
            next_sequence: committed_sequence + 1,
            next_entry_id: last_entry_id + 1,
            wal_entry: WalEntry::default(),
            memtable_mutations: Vec::new(),
            deferred_ack: None,
            provider,
            wal_notifiers: Vec::new(),
            bulk_notifiers: vec![],
            failed: false,
            put_num: 0,
            delete_num: 0,
            bulk_parts: vec![],
            written_bytes,
        }
    }

    /// Push mutation to the context.
    /// This method adopts the sequence number in parameters if present.
    pub(crate) fn push_mutation(
        &mut self,
        op_type: i32,
        rows: Option<Rows>,
        write_hint: Option<WriteHint>,
        tx: OptionOutputTx,
        sequence: Option<SequenceNumber>,
        skip_wal: bool,
    ) {
        if let Some(sequence) = sequence {
            self.next_sequence = sequence;
        }
        let num_rows = rows.as_ref().map(|rows| rows.rows.len()).unwrap_or(0);
        self.has_skip_wal_writes |= num_rows > 0 && self.is_skip_wal_write(skip_wal);
        let mutation = Mutation {
            op_type,
            sequence: self.next_sequence,
            rows,
            write_hint,
        };

        // Assign sequences before routing so concurrent memtable writes retain
        // their logical order regardless of the WAL policy.
        let notify = WriteNotify::new(tx, num_rows, !skip_wal);
        if skip_wal {
            self.memtable_mutations.push((mutation, notify));
        } else {
            self.wal_entry.mutations.push(mutation);
            self.wal_notifiers.push(notify);
        }

        // Increase sequence number.
        self.next_sequence += num_rows as u64;

        // Update metrics.
        match OpType::try_from(op_type) {
            Ok(OpType::Delete) => self.delete_num += num_rows,
            Ok(OpType::Put) => self.put_num += num_rows,
            Err(_) => (),
        }
    }

    /// Encode and add WAL entry to the writer.
    pub(crate) fn add_wal_entry<S: LogStore>(
        &mut self,
        wal_writer: &mut WalWriter<S>,
    ) -> Result<()> {
        wal_writer.add_entry(
            self.region_id,
            self.next_entry_id,
            &self.wal_entry,
            &self.provider,
        )?;
        self.next_entry_id += 1;
        Ok(())
    }

    pub(crate) fn version(&self) -> &VersionRef {
        &self.version
    }

    pub(crate) fn provider(&self) -> &Provider {
        &self.provider
    }

    #[cfg(test)]
    pub(crate) fn version_control(&self) -> &VersionControlRef {
        &self.version_control
    }

    /// Returns whether writes in this context should skip WAL.
    pub(crate) fn skip_wal(&self) -> bool {
        self.provider == Provider::Noop
            || self.version.options.skip_wal
            || (self.wal_entry.mutations.is_empty() && self.wal_entry.bulk_entries.is_empty())
    }

    /// Sets error and marks all write operations are failed.
    pub(crate) fn set_error(&mut self, err: Arc<Error>) {
        // Set error for all notifiers.
        for notify in self
            .wal_notifiers
            .iter_mut()
            .chain(self.memtable_mutations.iter_mut().map(|(_, notify)| notify))
        {
            notify.fail(err.clone());
        }
        for notify in &mut self.bulk_notifiers {
            notify.fail(err.clone());
        }

        // Fail the whole write operation.
        self.failed = true;
    }

    /// Returns whether the write operation is already marked as failed.
    pub(crate) fn is_failed(&self) -> bool {
        self.failed
    }

    /// Updates next entry id.
    pub(crate) fn set_next_entry_id(&mut self, next_entry_id: EntryId) {
        self.next_entry_id = next_entry_id
    }

    /// Makes every writer whose write went into the WAL entry, which the log
    /// store admitted through `entry_id`, wait until `barrier` confirms the
    /// entry durable before it is answered. Until then such a writer is
    /// answered with an error, so a write whose wait is dropped, cancelled or
    /// panics is never acknowledged. Call it once the entry is appended.
    pub(crate) fn defer_acks(&mut self, barrier: DurabilityBarrier, entry_id: EntryId) {
        let unconfirmed = Arc::new(
            AbandonedWalWriteSnafu {
                region_id: self.region_id,
                entry_id,
            }
            .build(),
        );
        for notify in self.wal_notifiers.iter_mut().chain(
            self.bulk_notifiers
                .iter_mut()
                .filter(|notify| notify.in_wal),
        ) {
            notify.await_durable(unconfirmed.clone());
        }
        self.deferred_ack = Some((barrier, entry_id));
    }

    /// Takes the writers whose answers wait for the WAL entry, see
    /// [`defer_acks`](Self::defer_acks), once the memtables are written and
    /// the entry is published. The other writers stay and are answered when
    /// the context is dropped.
    pub(crate) fn take_deferred_acks(&mut self) -> Option<DeferredAcks> {
        let (barrier, entry_id) = self.deferred_ack.take()?;
        let notifiers = self
            .wal_notifiers
            .drain(..)
            .chain(self.bulk_notifiers.extract_if(.., |notify| notify.in_wal))
            .collect();
        Some(DeferredAcks {
            region_id: self.region_id,
            entry_id,
            barrier,
            notifiers,
        })
    }

    /// Returns the next entry id to write.
    #[cfg(test)]
    pub(crate) fn next_entry_id(&self) -> EntryId {
        self.next_entry_id
    }

    /// Installs ordinary and bulk writes, then publishes their sequence and entry ID.
    pub(crate) async fn write_memtables(&mut self) {
        if self.failed {
            return;
        }
        // A failed installation may leave some rows in memory.
        if self.has_skip_wal_writes {
            self.version.memtables.mutable.mark_skip_wal_writes();
        }
        self.write_mutations().await;
        self.write_bulk().await;
        self.publish_sequence_and_entry_id();
    }

    fn is_skip_wal_write(&self, request_skip_wal: bool) -> bool {
        self.source == WriteSource::Request
            && (request_skip_wal
                || self.provider == Provider::Noop
                || self.version.options.skip_wal)
    }

    /// Consumes mutations and writes them into mutable memtables.
    async fn write_mutations(&mut self) {
        debug_assert_eq!(self.wal_notifiers.len(), self.wal_entry.mutations.len());

        if self.failed {
            return;
        }

        let mutable_memtable = self.version.memtables.mutable.clone();
        let prev_memory_usage = if self.written_bytes.is_some() {
            Some(mutable_memtable.memory_usage())
        } else {
            None
        };

        let mut mutations = mem::take(&mut self.wal_entry.mutations)
            .into_iter()
            .zip(&mut self.wal_notifiers)
            .chain(
                self.memtable_mutations
                    .iter_mut()
                    // Keep notifiers in the context until all writes complete.
                    .map(|(mutation, notify)| (mem::take(mutation), notify)),
            )
            .filter_map(|(mutation, notify)| {
                let kvs = KeyValues::new(&self.version.metadata, mutation)?;
                Some((notify, kvs))
            })
            .collect::<Vec<_>>();

        if mutations.len() == 1 {
            if let Err(err) = mutable_memtable.write(&mutations[0].1) {
                mutations[0].0.fail(Arc::new(err));
            }
        } else {
            let mut tasks = FuturesUnordered::new();
            for (notify, kvs) in mutations {
                let mutable = mutable_memtable.clone();
                // use tokio runtime to schedule tasks.
                let task = common_runtime::spawn_blocking_global(move || mutable.write(&kvs));
                tasks.push(async move { (notify, task.await) });
            }

            while let Some((notify, result)) = tasks.next().await {
                // First unwrap the result from `spawn` above.
                if let Err(err) = result.unwrap() {
                    notify.fail(Arc::new(err));
                }
            }
        }

        if let Some(written_bytes) = &self.written_bytes {
            let new_memory_usage = mutable_memtable.memory_usage();
            let bytes = new_memory_usage.saturating_sub(prev_memory_usage.unwrap_or_default());
            written_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn push_bulk(
        &mut self,
        sender: OptionOutputTx,
        mut bulk: BulkPart,
        sequence: Option<SequenceNumber>,
        skip_wal: bool,
    ) -> bool {
        if let Some(sequence) = sequence {
            self.next_sequence = sequence;
        }
        bulk.sequence = self.next_sequence;
        bulk.min_sequence = self.next_sequence;
        if !skip_wal {
            let entry = match BulkWalEntry::try_from(&bulk) {
                Ok(entry) => entry,
                Err(e) => {
                    sender.send(Err(e));
                    return false;
                }
            };
            self.wal_entry.bulk_entries.push(entry);
        }

        self.has_skip_wal_writes |= bulk.num_rows() > 0 && self.is_skip_wal_write(skip_wal);
        self.bulk_notifiers
            .push(WriteNotify::new(sender, bulk.num_rows(), !skip_wal));

        self.next_sequence += bulk.num_rows() as u64;
        self.bulk_parts.push(bulk);
        true
    }

    async fn write_bulk(&mut self) {
        if self.failed || self.bulk_parts.is_empty() {
            return;
        }
        #[cfg(test)]
        test_hooks::pause_before_bulk_install(self.region_id, &self.version_control).await;
        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["write_bulk"])
            .start_timer();

        let mutable_memtable = &self.version.memtables.mutable;
        let prev_memory_usage = if self.written_bytes.is_some() {
            Some(mutable_memtable.memory_usage())
        } else {
            None
        };

        if self.bulk_parts.len() == 1 {
            let part = self.bulk_parts.swap_remove(0);
            let num_rows = part.num_rows();
            if let Err(e) = self.version.memtables.mutable.write_bulk(part) {
                self.bulk_notifiers[0].fail(Arc::new(e));
            } else {
                self.put_num += num_rows;
            }
            return;
        }

        let mut tasks = FuturesUnordered::new();
        for (i, part) in self.bulk_parts.drain(..).enumerate() {
            let mutable = mutable_memtable.clone();
            tasks.push(common_runtime::spawn_blocking_global(move || {
                let num_rows = part.num_rows();
                (i, mutable.write_bulk(part), num_rows)
            }));
        }
        while let Some(result) = tasks.next().await {
            // first unwrap the result from `spawn` above
            let (i, result, num_rows) = result.unwrap();
            if let Err(err) = result {
                self.bulk_notifiers[i].fail(Arc::new(err));
            } else {
                self.put_num += num_rows;
            }
        }

        if let Some(written_bytes) = &self.written_bytes {
            let new_memory_usage = mutable_memtable.memory_usage();
            let bytes = new_memory_usage.saturating_sub(prev_memory_usage.unwrap_or_default());
            written_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Publishes the assigned sequences and entry id to the region's committed
    /// watermark. Only call after both [`write_mutations`](Self::write_mutations)
    /// and [`write_bulk`](Self::write_bulk) have completed; a failed context
    /// must not publish.
    fn publish_sequence_and_entry_id(&self) {
        if self.failed {
            return;
        }
        self.version_control
            .set_sequence_and_entry_id(self.next_sequence - 1, self.next_entry_id - 1);
    }
}

/// The writers of a region whose answers wait until their WAL entry is
/// confirmed durable. It holds only the writers, the barrier and the entry id,
/// never the write context, the version or the memtables.
pub(crate) struct DeferredAcks {
    region_id: RegionId,
    entry_id: EntryId,
    barrier: DurabilityBarrier,
    notifiers: Vec<WriteNotify>,
}

impl DeferredAcks {
    /// Waits until the WAL entry is durable, then answers the writers: with
    /// success, or with the error of the wait. The writers are answered when
    /// this drops, so a task that is aborted or panics answers them with the
    /// error [`RegionWriteCtx::defer_acks`] set.
    pub(crate) async fn answer_when_durable(mut self) {
        let _timer = metrics::WRITE_STAGE_ELAPSED
            .with_label_values(&["wait_durable"])
            .start_timer();
        let failure = self.barrier.wait(self.entry_id).await.err().map(|err| {
            Arc::new(
                UnconfirmedWalWriteSnafu {
                    region_id: self.region_id,
                    entry_id: self.entry_id,
                }
                .into_error(Arc::new(err)),
            )
        });
        for notify in &mut self.notifiers {
            notify.settle_durable(failure.as_ref());
        }
    }
}

/// Test-only hooks to make write ordering races deterministic.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use store_api::storage::RegionId;
    use tokio::sync::watch;

    use crate::region::version::VersionControlRef;

    /// Channels of an armed bulk-install barrier; dropping the senders (by
    /// disarming) unblocks writes paused on it.
    struct ActiveBarrier {
        id: u64,
        /// Only bulk writes for this region on this version control (Arc
        /// identity) pause at the barrier.
        target_region_id: RegionId,
        target_version_control: VersionControlRef,
        reached: watch::Sender<bool>,
        release: watch::Sender<bool>,
    }

    static ACTIVE_BARRIER: Mutex<Option<ActiveBarrier>> = Mutex::new(None);
    static NEXT_BARRIER_ID: AtomicU64 = AtomicU64::new(1);

    fn lock_active_barrier() -> std::sync::MutexGuard<'static, Option<ActiveBarrier>> {
        // Never let a poisoned mutex (e.g. a panic in another test while
        // holding the lock) hang or break unrelated tests.
        ACTIVE_BARRIER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// RAII guard: releasing (or dropping) it unblocks a paused write and
    /// disarms the barrier.
    pub(crate) struct BulkInstallBarrier {
        id: u64,
        reached_rx: watch::Receiver<bool>,
        release_tx: watch::Sender<bool>,
        released: bool,
    }

    impl BulkInstallBarrier {
        pub(crate) async fn wait_until_reached(&mut self) {
            if !*self.reached_rx.borrow() {
                let _ = self.reached_rx.wait_for(|reached| *reached).await;
            }
        }

        pub(crate) fn release(&mut self) {
            if self.released {
                return;
            }
            self.released = true;
            let _ = self.release_tx.send(true);
            disarm_barrier(self.id);
        }
    }

    impl Drop for BulkInstallBarrier {
        fn drop(&mut self) {
            self.release();
        }
    }

    /// Arms the bulk-install barrier and returns the owning guard; any
    /// previously armed barrier is replaced.
    pub(crate) fn arm_bulk_install_barrier(
        target_region_id: RegionId,
        target_version_control: VersionControlRef,
    ) -> BulkInstallBarrier {
        let (reached_tx, reached_rx) = watch::channel(false);
        let (release_tx, _release_rx) = watch::channel(false);
        let id = NEXT_BARRIER_ID.fetch_add(1, Ordering::Relaxed);
        let mut active = lock_active_barrier();
        *active = Some(ActiveBarrier {
            id,
            target_region_id,
            target_version_control,
            reached: reached_tx,
            release: release_tx.clone(),
        });
        BulkInstallBarrier {
            id,
            reached_rx,
            release_tx,
            released: false,
        }
    }

    fn disarm_barrier(id: u64) {
        let mut active = lock_active_barrier();
        if active.as_ref().is_some_and(|barrier| barrier.id == id) {
            *active = None;
        }
    }

    /// Pauses a bulk write before installing its parts until the barrier is
    /// released or disarmed.
    pub(crate) async fn pause_before_bulk_install(
        region_id: RegionId,
        version_control: &VersionControlRef,
    ) {
        let (reached_tx, release_rx) = {
            let active = lock_active_barrier();
            match active.as_ref() {
                Some(barrier)
                    if barrier.target_region_id == region_id
                        && std::sync::Arc::ptr_eq(
                            &barrier.target_version_control,
                            version_control,
                        ) =>
                {
                    (barrier.reached.clone(), barrier.release.subscribe())
                }
                _ => return,
            }
        };
        let _ = reached_tx.send(true);
        let mut release_rx = release_rx;
        if !*release_rx.borrow() {
            // The sender is dropped when the barrier is disarmed, which makes
            // `wait_for` return an error instead of hanging forever.
            let _ = release_rx.wait_for(|released| *released).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_recordbatch::DfRecordBatch;
    use datatypes::arrow::array::{ArrayRef, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use prost::Message;
    use store_api::logstore::provider::Provider;
    use tokio::sync::{oneshot, watch};

    use super::*;
    use crate::error::UnexpectedSnafu;
    use crate::memtable::bulk::part::BulkPart;
    use crate::test_util::version_util::VersionControlBuilder;

    #[test]
    fn test_request_skip_wal_preserves_sequences_and_other_writes() {
        // Ablate only the request flag: the workload and sequence allocation stay identical.
        check_request_skip_wal_preserves_sequences_and_other_writes(false, false);
        check_request_skip_wal_preserves_sequences_and_other_writes(false, true);
        check_request_skip_wal_preserves_sequences_and_other_writes(true, false);
        check_request_skip_wal_preserves_sequences_and_other_writes(true, true);
    }

    fn check_request_skip_wal_preserves_sequences_and_other_writes(
        skip_wal: bool,
        bulk_skip_wal: bool,
    ) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
            WriteSource::Request,
        );
        for (op_type, skip, num_rows) in [
            (OpType::Put, skip_wal, 2),
            (OpType::Put, false, 3),
            (OpType::Delete, false, 1),
        ] {
            ctx.push_mutation(
                op_type as i32,
                Some(Rows {
                    schema: vec![],
                    rows: vec![api::v1::Row::default(); num_rows],
                }),
                None,
                OptionOutputTx::none(),
                None,
                skip,
            );
        }
        // Unequal row counts make a notifier/mutation pairing mismatch visible.
        for (mutation, notify) in ctx
            .wal_entry
            .mutations
            .iter()
            .zip(&ctx.wal_notifiers)
            .chain(
                ctx.memtable_mutations
                    .iter()
                    .map(|(mutation, notify)| (mutation, notify)),
            )
        {
            assert_eq!(mutation.rows.as_ref().unwrap().rows.len(), notify.num_rows);
        }
        assert!(ctx.push_bulk(OptionOutputTx::none(), new_bulk_part(), None, bulk_skip_wal));
        assert!(!ctx.skip_wal());
        assert_eq!(ctx.next_sequence, 9);
        assert_eq!(
            ctx.wal_entry.bulk_entries.len(),
            usize::from(!bulk_skip_wal)
        );
        assert_eq!(ctx.bulk_parts[0].sequence, 7);
        assert_eq!(ctx.bulk_parts[0].min_sequence, 7);
        let sequences: Vec<_> = ctx.wal_entry.mutations.iter().map(|m| m.sequence).collect();
        assert_eq!(sequences, if skip_wal { vec![3, 6] } else { vec![1, 3, 6] });
        // Check the actual WAL bytes after routing the mutations.
        let encoded = crate::wal::encoder::WalEntryEncoder::new().encode_to_vec(&ctx.wal_entry);
        let decoded = WalEntry::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            decoded
                .mutations
                .iter()
                .map(|m| m.sequence)
                .collect::<Vec<_>>(),
            sequences
        );
        assert_eq!(decoded.bulk_entries, ctx.wal_entry.bulk_entries);
        assert_eq!(ctx.wal_entry.mutations.len(), if skip_wal { 2 } else { 3 });
        assert_eq!(ctx.memtable_mutations.len(), usize::from(skip_wal));
        if skip_wal {
            assert_eq!(ctx.memtable_mutations[0].0.sequence, 1);
        }
        assert_eq!(
            ctx.wal_entry.mutations.last().unwrap().op_type,
            OpType::Delete as i32
        );
    }

    #[test]
    fn test_internal_delete_respects_skip_wal_flag() {
        check_internal_delete_respects_skip_wal_flag(false);
        check_internal_delete_respects_skip_wal_flag(true);
    }

    fn check_internal_delete_respects_skip_wal_flag(skip_wal: bool) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
            WriteSource::Request,
        );
        ctx.push_mutation(
            OpType::Delete as i32,
            Some(Rows {
                schema: vec![],
                rows: vec![api::v1::Row::default(); 2],
            }),
            None,
            OptionOutputTx::none(),
            None,
            skip_wal,
        );
        assert_eq!(ctx.skip_wal(), skip_wal);
        assert_eq!(ctx.next_sequence, 3);
        assert_eq!(ctx.delete_num, 2);
        assert_eq!(ctx.wal_entry.mutations.len(), usize::from(!skip_wal));
        assert_eq!(ctx.memtable_mutations.len(), usize::from(skip_wal));
        let encoded = crate::wal::encoder::WalEntryEncoder::new().encode_to_vec(&ctx.wal_entry);
        let decoded = WalEntry::decode(encoded.as_slice()).unwrap();
        if skip_wal {
            assert!(decoded.mutations.is_empty());
        } else {
            assert_eq!(decoded, ctx.wal_entry);
        }
    }

    #[test]
    fn test_all_request_skip_wal_keeps_entry_id_and_propagates_errors() {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
            WriteSource::Request,
        );
        let (tx, rx) = oneshot::channel();
        ctx.push_mutation(
            OpType::Put as i32,
            Some(Rows {
                schema: vec![],
                rows: vec![api::v1::Row::default(); 2],
            }),
            None,
            OptionOutputTx::from(tx),
            None,
            true,
        );
        assert!(ctx.skip_wal());
        assert!(ctx.wal_entry.mutations.is_empty());
        assert_eq!(ctx.memtable_mutations.len(), 1);
        assert_eq!(ctx.next_entry_id(), 1);
        assert_eq!(ctx.next_sequence, 3);
        ctx.set_error(Arc::new(
            UnexpectedSnafu {
                reason: "wal failed".to_string(),
            }
            .build(),
        ));
        drop(ctx);
        assert!(rx.blocking_recv().unwrap().is_err());
    }

    #[test]
    fn test_bulk_skip_wal_preserves_sequences() {
        check_bulk_skip_wal_preserves_sequences(false);
        check_bulk_skip_wal_preserves_sequences(true);
    }

    fn check_bulk_skip_wal_preserves_sequences(skip_wal: bool) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
            WriteSource::Request,
        );
        // Alternate policies in one context, retaining all parts for the memtable.
        for skip in [skip_wal, false, skip_wal] {
            assert!(ctx.push_bulk(OptionOutputTx::none(), new_bulk_part(), None, skip));
        }
        assert_eq!(ctx.next_sequence, 7);
        assert_eq!(ctx.bulk_notifiers.len(), 3);
        assert_eq!(
            ctx.bulk_parts
                .iter()
                .map(|p| p.sequence)
                .collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        let encoded = crate::wal::encoder::WalEntryEncoder::new().encode_to_vec(&ctx.wal_entry);
        let decoded = WalEntry::decode(encoded.as_slice()).unwrap();
        assert_eq!(
            decoded
                .bulk_entries
                .iter()
                .map(|e| e.sequence)
                .collect::<Vec<_>>(),
            if skip_wal { vec![3] } else { vec![1, 3, 5] }
        );
    }

    #[test]
    fn test_set_error_marks_bulk_notifiers_failed() {
        check_set_error_marks_bulk_notifiers_failed(false);
        check_set_error_marks_bulk_notifiers_failed(true);
    }

    fn check_set_error_marks_bulk_notifiers_failed(skip_wal: bool) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::noop_provider(),
            None,
            WriteSource::Request,
        );
        let (tx, rx) = oneshot::channel();

        assert!(ctx.push_bulk(OptionOutputTx::from(tx), new_bulk_part(), None, skip_wal));
        assert_eq!(ctx.wal_entry.bulk_entries.len(), usize::from(!skip_wal));
        assert_eq!(ctx.bulk_parts.len(), 1);
        ctx.set_error(Arc::new(
            UnexpectedSnafu {
                reason: "wal failed".to_string(),
            }
            .build(),
        ));
        drop(ctx);

        let result = rx.blocking_recv().unwrap();
        assert!(result.is_err(), "bulk notifier should report WAL error");
    }

    #[tokio::test]
    async fn test_skip_wal_writes_source_and_policy() {
        use api::v1::helper::{tag_column_schema, time_index_column_schema};
        use api::v1::value::ValueData;
        use api::v1::{ColumnDataType, Row, Value};

        for source in [WriteSource::Request, WriteSource::WalReplay] {
            for skip_wal in [false, true] {
                for bulk in [false, true] {
                    for fail_before_install in [false, true] {
                        let builder = VersionControlBuilder::new();
                        let region_id = builder.region_id();
                        let version_control = Arc::new(builder.build());
                        let mut ctx = RegionWriteCtx::new(
                            region_id,
                            &version_control,
                            Provider::raft_engine_provider(region_id.as_u64()),
                            None,
                            source,
                        );
                        let (tx, rx) = oneshot::channel();
                        if bulk {
                            let mut part = new_bulk_part();
                            part.fill_missing_columns(&ctx.version.metadata).unwrap();
                            assert!(ctx.push_bulk(OptionOutputTx::from(tx), part, None, skip_wal));
                        } else {
                            ctx.push_mutation(
                                OpType::Put as i32,
                                Some(Rows {
                                    schema: vec![
                                        time_index_column_schema(
                                            "ts",
                                            ColumnDataType::TimestampMillisecond,
                                        ),
                                        tag_column_schema("tag_0", ColumnDataType::String),
                                    ],
                                    rows: vec![Row {
                                        values: vec![
                                            crate::test_util::ts_ms_value(0),
                                            Value {
                                                value_data: Some(ValueData::StringValue(
                                                    "a".to_string(),
                                                )),
                                            },
                                        ],
                                    }],
                                }),
                                None,
                                OptionOutputTx::from(tx),
                                None,
                                skip_wal,
                            );
                        }
                        if fail_before_install {
                            ctx.set_error(Arc::new(
                                UnexpectedSnafu {
                                    reason: "WAL append failed".to_string(),
                                }
                                .build(),
                            ));
                        }
                        ctx.write_memtables().await;
                        assert_eq!(
                            version_control
                                .current()
                                .version
                                .memtables
                                .has_skip_wal_writes(),
                            source == WriteSource::Request && skip_wal && !fail_before_install,
                            "source={source:?}, skip_wal={skip_wal}, bulk={bulk}, failed={fail_before_install}",
                        );
                        assert_eq!(
                            version_control.committed_sequence(),
                            if fail_before_install {
                                0
                            } else if bulk {
                                2
                            } else {
                                1
                            }
                        );
                        drop(ctx);
                        assert_eq!(rx.await.unwrap().is_err(), fail_before_install);
                    }
                }
            }
        }
    }

    #[test]
    fn test_skip_wal_writes_region_policy_and_empty_requests() {
        for source in [WriteSource::Request, WriteSource::WalReplay] {
            for noop in [false, true] {
                for region_skip_wal in [false, true] {
                    let builder = VersionControlBuilder::new();
                    let region_id = builder.region_id();
                    let mut version = builder.build_version();
                    version.options.skip_wal = region_skip_wal;
                    let version_control =
                        Arc::new(crate::region::version::VersionControl::new(version));
                    let provider = if noop {
                        Provider::Noop
                    } else {
                        Provider::raft_engine_provider(region_id.as_u64())
                    };
                    let mut ctx =
                        RegionWriteCtx::new(region_id, &version_control, provider, None, source);
                    ctx.push_mutation(
                        OpType::Put as i32,
                        None,
                        None,
                        OptionOutputTx::none(),
                        None,
                        true,
                    );
                    assert!(!ctx.has_skip_wal_writes);
                    ctx.push_mutation(
                        OpType::Put as i32,
                        Some(Rows {
                            schema: vec![],
                            rows: vec![api::v1::Row::default()],
                        }),
                        None,
                        OptionOutputTx::none(),
                        None,
                        false,
                    );
                    assert_eq!(
                        ctx.has_skip_wal_writes,
                        source == WriteSource::Request && (noop || region_skip_wal)
                    );
                }
            }
        }
    }

    /// A context with a WAL row write, a row write that skips the WAL, a WAL
    /// bulk part and a bulk part that skips the WAL, whose entry the log store
    /// admitted through entry 7, and the receivers of the four writers in that
    /// order.
    fn mixed_ctx_awaiting(
        barrier: DurabilityBarrier,
    ) -> (
        RegionWriteCtx,
        Vec<oneshot::Receiver<Result<store_api::region_request::AffectedRows>>>,
    ) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut ctx = RegionWriteCtx::new(
            region_id,
            &version_control,
            Provider::raft_engine_provider(region_id.as_u64()),
            None,
            WriteSource::Request,
        );
        let mut receivers = Vec::new();
        for skip_wal in [false, true] {
            let (tx, rx) = oneshot::channel();
            ctx.push_mutation(
                OpType::Put as i32,
                Some(Rows {
                    schema: vec![],
                    rows: vec![api::v1::Row::default(); 2],
                }),
                None,
                OptionOutputTx::from(tx),
                None,
                skip_wal,
            );
            receivers.push(rx);
        }
        for skip_wal in [false, true] {
            let (tx, rx) = oneshot::channel();
            assert!(ctx.push_bulk(OptionOutputTx::from(tx), new_bulk_part(), None, skip_wal));
            receivers.push(rx);
        }
        ctx.defer_acks(barrier, 7);
        (ctx, receivers)
    }

    /// A barrier that waits until `outcome` holds a value and answers with it.
    fn barrier_answering(outcome: watch::Receiver<Option<bool>>) -> DurabilityBarrier {
        DurabilityBarrier::from_fn(move |_| {
            let mut outcome = outcome.clone();
            Box::pin(async move {
                let durable = *outcome
                    .wait_for(Option::is_some)
                    .await
                    .unwrap()
                    .as_ref()
                    .unwrap();
                if durable {
                    Ok(())
                } else {
                    UnexpectedSnafu {
                        reason: "the WAL failed".to_string(),
                    }
                    .fail()
                }
            })
        })
    }

    #[tokio::test]
    async fn test_deferred_acks_wait_only_for_writes_in_the_wal() {
        for durable in [true, false] {
            let (outcome_tx, outcome_rx) = watch::channel(None);
            let (mut ctx, mut receivers) = mixed_ctx_awaiting(barrier_answering(outcome_rx));
            let acks = ctx.take_deferred_acks().unwrap();
            // The writes that skipped the WAL are answered with the context.
            drop(ctx);
            assert_eq!(2, receivers[1].try_recv().unwrap().unwrap());
            assert_eq!(2, receivers[3].try_recv().unwrap().unwrap());
            let answered = tokio::spawn(acks.answer_when_durable());
            tokio::task::yield_now().await;
            assert!(receivers[0].try_recv().is_err());
            assert!(receivers[2].try_recv().is_err());

            outcome_tx.send_replace(Some(durable));
            answered.await.unwrap();
            for index in [0, 2] {
                match receivers[index].try_recv().unwrap() {
                    Ok(rows) => {
                        assert!(durable);
                        assert_eq!(2, rows);
                    }
                    Err(error) => {
                        assert!(!durable);
                        assert_eq!(StatusCode::DeadlineExceeded, error.status_code());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_deferred_acks_never_acknowledge_an_abandoned_wait() {
        // The wait is aborted, or it panics.
        for panics in [false, true] {
            let barrier = DurabilityBarrier::from_fn(move |_| {
                Box::pin(async move {
                    if panics {
                        panic!("the wait panics");
                    }
                    std::future::pending().await
                })
            });
            let (mut ctx, mut receivers) = mixed_ctx_awaiting(barrier);
            let acks = ctx.take_deferred_acks().unwrap();
            drop(ctx);
            let answered = tokio::spawn(acks.answer_when_durable());
            if !panics {
                tokio::task::yield_now().await;
                answered.abort();
            }
            assert!(answered.await.is_err());
            for index in [0, 2] {
                let error = receivers[index].try_recv().unwrap().unwrap_err();
                assert_eq!(StatusCode::DeadlineExceeded, error.status_code());
            }
        }
    }

    #[tokio::test]
    async fn test_deferred_acks_keep_a_failed_memtable_write() {
        let (outcome_tx, outcome_rx) = watch::channel(Some(true));
        let (mut ctx, mut receivers) = mixed_ctx_awaiting(barrier_answering(outcome_rx));
        ctx.wal_notifiers[0].fail(Arc::new(
            UnexpectedSnafu {
                reason: "memtable write failed".to_string(),
            }
            .build(),
        ));
        let acks = ctx.take_deferred_acks().unwrap();
        drop(ctx);
        acks.answer_when_durable().await;
        drop(outcome_tx);
        assert!(receivers[0].try_recv().unwrap().is_err());
        assert_eq!(2, receivers[2].try_recv().unwrap().unwrap());
    }

    fn new_bulk_part() -> BulkPart {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        )]));
        let arrays = vec![Arc::new(TimestampMillisecondArray::from(vec![1, 2])) as ArrayRef];
        let batch = DfRecordBatch::try_new(schema, arrays).unwrap();

        BulkPart {
            batch,
            max_timestamp: 2,
            min_timestamp: 1,
            sequence: 0,
            min_sequence: 0,
            timestamp_index: 0,
            raw_data: None,
        }
    }
}

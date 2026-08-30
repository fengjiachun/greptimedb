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

//! PoC of the memory ledger: accounts with runtime-adjustable limits.
//!
//! This module validates the resize contract of the ARM memory ledger design:
//!
//! - An [Account] is a bounded memory budget backed by a semaphore, shared by
//!   any number of handles (the limit lives in shared atomics, so every handle
//!   observes the same `target`/`effective` values).
//! - `set_limit_bytes` takes effect instantly for grow; shrink never revokes
//!   granted memory: idle capacity is harvested immediately, the remaining
//!   deficit is collected by [Account::collect_shrink] as guards release.
//! - The collector is driven by the caller (a controller tick in production,
//!   the test in tests) instead of a spawned task, so this crate stays free of
//!   runtime dependencies and the "single writer of primitive parameters"
//!   principle holds.
//! - [AccountGuard] exposes both faces the ledger needs: async acquisition
//!   with wait/fail policies (the scan-tracker face) and synchronous
//!   `try_grow`/`shrink` (the DataFusion memory-pool face). Both draw from the
//!   same semaphore, so their sum can never exceed the account limit.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use snafu::ensure;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::error::{
    MemoryAcquireTimeoutSnafu, MemoryLimitExceededSnafu, MemorySemaphoreClosedSnafu, Result,
};
use crate::granularity::PermitGranularity;
use crate::policy::OnExhaustedPolicy;

/// Workload category of an account, matching the ledger tree's top level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Ingest,
    Query,
    Background,
    Cache,
}

/// Max permits the shrink collector acquires per round.
///
/// Bounds head-of-line blocking in the semaphore's FIFO queue to one chunk.
const SHRINK_CHUNK_MAX_PERMITS: u32 = 64;

struct AccountInner {
    name: String,
    category: Category,
    granularity: PermitGranularity,
    semaphore: Arc<Semaphore>,
    /// Desired capacity in permits. Updated instantly by `set_limit_bytes`.
    target_permits: AtomicU32,
    /// Capacity the semaphore currently embodies (available + outstanding).
    /// Converges towards `target_permits` while shrinking.
    effective_permits: AtomicU32,
    /// Serializes limit changes and collector bookkeeping (control plane only,
    /// never taken on the acquisition hot path).
    control: Mutex<()>,
    /// Single-flight flag for the shrink collector.
    collecting: AtomicBool,
}

impl AccountInner {
    fn bytes_to_permits(&self, bytes: u64) -> u32 {
        self.granularity.bytes_to_permits(bytes)
    }

    fn permits_to_bytes(&self, permits: u32) -> u64 {
        self.granularity.permits_to_bytes(permits)
    }
}

/// RAII backstop that clears the shrink collector's single-flight flag.
///
/// [Account::collect_shrink] arms this right after winning the flag, so every
/// exit path releases it — including the future being dropped at an await
/// point (e.g. under `tokio::time::timeout`). The normal convergence path
/// calls [Self::clear] inside the `control` critical section instead, which
/// keeps "converged when the flag is released" atomic; the drop backstop only
/// fires on cancellation or on a closed semaphore.
struct CollectingFlagGuard<'a> {
    collecting: &'a AtomicBool,
    armed: bool,
}

impl CollectingFlagGuard<'_> {
    /// Clears the flag immediately and disarms the drop backstop.
    ///
    /// Call while holding the `control` lock so the clear is atomic with the
    /// convergence check.
    fn clear(&mut self) {
        self.collecting.store(false, Ordering::Release);
        self.armed = false;
    }
}

impl Drop for CollectingFlagGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.collecting.store(false, Ordering::Release);
        }
    }
}

/// A bounded memory account with a runtime-adjustable limit.
///
/// Cloning shares the same underlying budget.
#[derive(Clone)]
pub struct Account {
    inner: Arc<AccountInner>,
}

impl Account {
    /// Creates a bounded account. Accounts are always bounded: "unlimited" is
    /// expressed by passing the parent budget as the limit.
    ///
    /// The limit saturates at `granularity.permits_to_bytes(u32::MAX)` (4 TiB
    /// at 1 KB granularity, further capped by `Semaphore::MAX_PERMITS` where
    /// smaller): larger values are stored as that maximum. Admission checks
    /// compare request bytes against the saturated target, so oversized
    /// requests fail instead of being silently clamped.
    pub fn new(
        name: impl Into<String>,
        category: Category,
        limit_bytes: u64,
        granularity: PermitGranularity,
    ) -> Self {
        // Saturates: the conversion clamps to the max permit count.
        let limit_permits = granularity.bytes_to_permits(limit_bytes);
        Self {
            inner: Arc::new(AccountInner {
                name: name.into(),
                category,
                granularity,
                semaphore: Arc::new(Semaphore::new(limit_permits as usize)),
                target_permits: AtomicU32::new(limit_permits),
                effective_permits: AtomicU32::new(limit_permits),
                control: Mutex::new(()),
                collecting: AtomicBool::new(false),
            }),
        }
    }

    /// Account name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Workload category.
    pub fn category(&self) -> Category {
        self.inner.category
    }

    /// Permit granularity of this account, for adapter layers that need to
    /// reconcile byte-level remainders against whole-permit accounting.
    pub fn granularity(&self) -> PermitGranularity {
        self.inner.granularity
    }

    /// Desired limit in bytes (set instantly by `set_limit_bytes`).
    pub fn target_limit_bytes(&self) -> u64 {
        self.inner
            .permits_to_bytes(self.inner.target_permits.load(Ordering::Acquire))
    }

    /// Capacity the account currently embodies. Equals the target except while
    /// a shrink is converging.
    pub fn effective_limit_bytes(&self) -> u64 {
        self.inner
            .permits_to_bytes(self.inner.effective_permits.load(Ordering::Acquire))
    }

    /// Bytes currently granted to guards.
    ///
    /// Derived as `effective - available`; while the shrink collector holds a
    /// chunk the chunk transiently counts as used. The two reads are not
    /// atomic with respect to limit changes: a concurrent `set_limit_bytes`
    /// or collector step may transiently over-report usage, bounded by the
    /// in-flight delta. Mutation orderings are chosen so the transient error
    /// is towards over-reporting — the safe direction for admission checks.
    pub fn used_bytes(&self) -> u64 {
        let effective = self.inner.effective_permits.load(Ordering::Acquire);
        let available = self
            .inner
            .semaphore
            .available_permits()
            .min(effective as usize) as u32;
        self.inner.permits_to_bytes(effective - available)
    }

    /// Adjusts the limit. Returns the remaining shrink deficit in bytes.
    ///
    /// Grow takes effect instantly (waiters wake). Shrink harvests idle
    /// capacity instantly and leaves the remainder to [Self::collect_shrink];
    /// granted memory is never revoked.
    ///
    /// Like [Self::new], the limit saturates at
    /// `granularity.permits_to_bytes(u32::MAX)`: larger values are stored as
    /// that maximum.
    pub fn set_limit_bytes(&self, bytes: u64) -> u64 {
        // Saturates: the conversion clamps to the max permit count.
        let new_target = self.inner.bytes_to_permits(bytes);
        let _guard = self.inner.control.lock().unwrap();
        let effective = self.inner.effective_permits.load(Ordering::Acquire);
        self.inner
            .target_permits
            .store(new_target, Ordering::Release);

        if new_target >= effective {
            let delta = new_target - effective;
            if delta > 0 {
                // Publish the higher effective limit before crediting the
                // semaphore: a concurrent `used_bytes` then transiently
                // over-reports (bounded by `delta`) instead of
                // under-reporting granted memory.
                self.inner
                    .effective_permits
                    .store(new_target, Ordering::Release);
                self.inner.semaphore.add_permits(delta as usize);
            }
            0
        } else {
            let deficit = effective - new_target;
            let forgotten = self.inner.semaphore.forget_permits(deficit as usize) as u32;
            let now_effective = effective - forgotten;
            self.inner
                .effective_permits
                .store(now_effective, Ordering::Release);
            self.inner.permits_to_bytes(now_effective - new_target)
        }
    }

    /// Drives an in-progress shrink until the account has converged
    /// (`effective <= target`). Never revokes granted memory: capacity is
    /// acquired in chunks through the semaphore's FIFO queue, competing
    /// fairly with normal waiters.
    ///
    /// Concurrency contract:
    /// - Idempotent: calling on a converged account is a cheap no-op.
    /// - Single-flight: while one call collects, concurrent calls return
    ///   immediately without waiting for convergence.
    /// - No stranded deficit: the single-flight flag is cleared in the same
    ///   `control` critical section that confirms convergence, and
    ///   `set_limit_bytes` mutates the target only under that lock — so a
    ///   new deficit is always seen either by the still-running collector or
    ///   by the next call, which then wins the flag.
    /// - Cancellation-safe: if the future is dropped at an await point (e.g.
    ///   under `tokio::time::timeout`), [CollectingFlagGuard] releases the
    ///   flag and the next call resumes collection.
    pub async fn collect_shrink(&self) {
        if self.inner.collecting.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut flag_guard = CollectingFlagGuard {
            collecting: &self.inner.collecting,
            armed: true,
        };

        loop {
            // Harvest idle capacity and size the next chunk. Convergence and
            // the flag clear happen in one critical section: a concurrent
            // shrink of the target can never slip between them and strand
            // its deficit behind a still-set flag.
            let chunk = {
                let _guard = self.inner.control.lock().unwrap();
                let target = self.inner.target_permits.load(Ordering::Acquire);
                let effective = self.inner.effective_permits.load(Ordering::Acquire);
                if effective <= target {
                    flag_guard.clear();
                    return;
                }
                let deficit = effective - target;
                let forgotten = self.inner.semaphore.forget_permits(deficit as usize) as u32;
                let now_effective = effective - forgotten;
                self.inner
                    .effective_permits
                    .store(now_effective, Ordering::Release);
                if now_effective <= target {
                    flag_guard.clear();
                    return;
                }
                (now_effective - target).min(SHRINK_CHUNK_MAX_PERMITS)
            };

            // Queue for the chunk like any other waiter (FIFO). If the
            // future is dropped while waiting here, the drop backstop
            // releases the single-flight flag.
            let Ok(mut permit) = self.inner.semaphore.clone().acquire_many_owned(chunk).await
            else {
                // Semaphore closed: no progress is possible; the drop
                // backstop releases the flag.
                return;
            };

            let _guard = self.inner.control.lock().unwrap();
            let target = self.inner.target_permits.load(Ordering::Acquire);
            let effective = self.inner.effective_permits.load(Ordering::Acquire);
            let needed = effective.saturating_sub(target);
            if needed == 0 {
                // Target was raised meanwhile; return the capacity.
                drop(permit);
                continue;
            }
            let forget_n = chunk.min(needed);
            if forget_n < chunk {
                // Return the excess before forgetting the rest.
                let excess = permit.split((chunk - forget_n) as usize);
                drop(excess);
            }
            permit.forget();
            self.inner
                .effective_permits
                .store(effective - forget_n, Ordering::Release);
        }
    }

    /// Checks a request against the target limit, comparing in bytes before
    /// any permit-conversion clamping: a request larger than the (possibly
    /// saturated) target must fail loudly instead of being silently clamped
    /// to the maximum permit count.
    fn ensure_within_target(&self, bytes: u64) -> Result<()> {
        let target_bytes = self.target_limit_bytes();
        ensure!(
            bytes <= target_bytes,
            MemoryLimitExceededSnafu {
                requested_bytes: bytes,
                limit_bytes: target_bytes,
            }
        );
        Ok(())
    }

    /// Acquires memory, waiting until enough capacity is available.
    pub async fn acquire(&self, bytes: u64) -> Result<AccountGuard> {
        self.ensure_within_target(bytes)?;
        let permits = self.inner.bytes_to_permits(bytes);
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| MemorySemaphoreClosedSnafu.build())?;
        Ok(AccountGuard {
            inner: self.inner.clone(),
            permit,
        })
    }

    /// Tries to acquire memory without waiting.
    pub fn try_acquire(&self, bytes: u64) -> Option<AccountGuard> {
        // Compare in bytes, like `ensure_within_target`.
        if bytes > self.target_limit_bytes() {
            return None;
        }
        let permits = self.inner.bytes_to_permits(bytes);
        match self.inner.semaphore.clone().try_acquire_many_owned(permits) {
            Ok(permit) => Some(AccountGuard {
                inner: self.inner.clone(),
                permit,
            }),
            Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => None,
        }
    }

    /// Acquires memory according to the given policy.
    pub async fn acquire_with_policy(
        &self,
        bytes: u64,
        policy: OnExhaustedPolicy,
    ) -> Result<AccountGuard> {
        match policy {
            OnExhaustedPolicy::Wait { timeout } => {
                match tokio::time::timeout(timeout, self.acquire(bytes)).await {
                    Ok(result) => result,
                    Err(_elapsed) => MemoryAcquireTimeoutSnafu {
                        requested_bytes: bytes,
                        waited: timeout,
                    }
                    .fail(),
                }
            }
            OnExhaustedPolicy::Fail => self.try_acquire(bytes).ok_or_else(|| {
                MemoryLimitExceededSnafu {
                    requested_bytes: bytes,
                    limit_bytes: self.target_limit_bytes(),
                }
                .build()
            }),
        }
    }
}

/// Guard over granted capacity, usable from both the async (wait/fail policy)
/// and the synchronous (DataFusion pool) face.
pub struct AccountGuard {
    inner: Arc<AccountInner>,
    permit: OwnedSemaphorePermit,
}

impl AccountGuard {
    /// Bytes granted to this guard.
    pub fn granted_bytes(&self) -> u64 {
        self.inner
            .permits_to_bytes(self.permit.num_permits() as u32)
    }

    /// Synchronously grows this guard. Returns false if capacity or the
    /// target limit does not allow it. The limit check compares bytes, so a
    /// request beyond the (possibly saturated) target fails instead of being
    /// clamped.
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        let target_bytes = self
            .inner
            .permits_to_bytes(self.inner.target_permits.load(Ordering::Acquire));
        if self.granted_bytes().saturating_add(bytes) > target_bytes {
            return false;
        }
        let permits = self.inner.bytes_to_permits(bytes);
        match self.inner.semaphore.clone().try_acquire_many_owned(permits) {
            Ok(extra) => {
                self.permit.merge(extra);
                true
            }
            Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => false,
        }
    }

    /// Grows this guard, waiting until capacity is available. The limit
    /// check compares bytes, like [Self::try_grow].
    pub async fn grow(&mut self, bytes: u64) -> Result<()> {
        let target_bytes = self
            .inner
            .permits_to_bytes(self.inner.target_permits.load(Ordering::Acquire));
        ensure!(
            self.granted_bytes().saturating_add(bytes) <= target_bytes,
            MemoryLimitExceededSnafu {
                requested_bytes: bytes,
                limit_bytes: target_bytes,
            }
        );
        let permits = self.inner.bytes_to_permits(bytes);
        let extra = self
            .inner
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| MemorySemaphoreClosedSnafu.build())?;
        self.permit.merge(extra);
        Ok(())
    }

    /// Returns part of the granted capacity, releasing whole permits only:
    /// `bytes` is rounded DOWN to the permit granularity, so a request below
    /// one permit releases nothing and returns 0. Returns the bytes actually
    /// released (also clamped to the granted amount); any sub-permit
    /// remainder stays granted and is the caller's (e.g. a pool adapter's)
    /// responsibility to track.
    pub fn shrink(&mut self, bytes: u64) -> u64 {
        let whole_permits = bytes / self.inner.granularity.bytes();
        let permits = whole_permits.min(self.permit.num_permits() as u64) as u32;
        if permits == 0 {
            return 0;
        }
        match self.permit.split(permits as usize) {
            Some(returned) => {
                drop(returned);
                self.inner.permits_to_bytes(permits)
            }
            None => {
                // Unreachable: `permits` is clamped to `num_permits` above,
                // and `split` only refuses requests beyond the held amount.
                debug_assert!(false, "split refused a request clamped to num_permits");
                0
            }
        }
    }
}

/// Point-in-time view of an account, for the future system table.
///
/// Fields are sampled independently, without a common lock: a snapshot taken
/// while a limit change or the shrink collector is in flight may be
/// internally inconsistent (e.g. `used_bytes` derived from a newer effective
/// limit than the one captured in `effective_limit_bytes`).
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub name: String,
    pub category: Category,
    pub target_limit_bytes: u64,
    pub effective_limit_bytes: u64,
    pub used_bytes: u64,
}

/// Registry of accounts. PoC scope: flat registry plus on-demand aggregation;
/// the category tree interior is observation-only by design.
pub struct MemoryLedger {
    root_budget_bytes: u64,
    accounts: RwLock<Vec<Account>>,
}

impl MemoryLedger {
    pub fn new(root_budget_bytes: u64) -> Self {
        Self {
            root_budget_bytes,
            accounts: RwLock::new(Vec::new()),
        }
    }

    pub fn root_budget_bytes(&self) -> u64 {
        self.root_budget_bytes
    }

    /// Registers a bounded account.
    pub fn register(
        &self,
        name: impl Into<String>,
        category: Category,
        limit_bytes: u64,
        granularity: PermitGranularity,
    ) -> Account {
        let account = Account::new(name, category, limit_bytes, granularity);
        self.accounts.write().unwrap().push(account.clone());
        account
    }

    /// Sum of bytes granted across all accounts.
    pub fn total_used_bytes(&self) -> u64 {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .map(|a| a.used_bytes())
            .sum()
    }

    /// The unaccounted gap between an externally observed RSS and the ledger.
    pub fn unaccounted_bytes(&self, rss_bytes: u64) -> u64 {
        rss_bytes.saturating_sub(self.total_used_bytes())
    }

    /// Snapshot of all accounts.
    pub fn snapshot(&self) -> Vec<AccountSnapshot> {
        self.accounts
            .read()
            .unwrap()
            .iter()
            .map(|a| AccountSnapshot {
                name: a.name().to_string(),
                category: a.category(),
                target_limit_bytes: a.target_limit_bytes(),
                effective_limit_bytes: a.effective_limit_bytes(),
                used_bytes: a.used_bytes(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const KB: u64 = 1024;

    fn account(limit_kb: u64) -> Account {
        Account::new(
            "test",
            Category::Query,
            limit_kb * KB,
            PermitGranularity::Kilobyte,
        )
    }

    #[tokio::test]
    async fn grow_wakes_waiter_instantly() {
        let acc = account(4);
        let held = acc.acquire(4 * KB).await.unwrap();

        let acc2 = acc.clone();
        let waiter = tokio::spawn(async move { acc2.acquire(2 * KB).await.unwrap() });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        assert_eq!(acc.set_limit_bytes(8 * KB), 0);
        let got = waiter.await.unwrap();
        assert_eq!(got.granted_bytes(), 2 * KB);
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);
        assert_eq!(acc.used_bytes(), 6 * KB);
        drop(held);
    }

    #[tokio::test]
    async fn shrink_idle_capacity_is_instant() {
        let acc = account(8);
        let deficit = acc.set_limit_bytes(4 * KB);
        assert_eq!(deficit, 0);
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        // Oversized requests fail fast against the new target.
        assert!(acc.acquire(5 * KB).await.is_err());
        assert!(acc.try_acquire(5 * KB).is_none());
        assert!(acc.try_acquire(4 * KB).is_some());
    }

    #[tokio::test]
    async fn shrink_is_non_preemptive_and_converges() {
        let acc = account(10);
        let mut held = acc.acquire(8 * KB).await.unwrap();

        // Shrink to 4: 2 idle permits harvested instantly, 4 in deficit.
        let deficit = acc.set_limit_bytes(4 * KB);
        assert_eq!(deficit, 4 * KB);
        // Granted memory is untouched.
        assert_eq!(held.granted_bytes(), 8 * KB);
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        let collector = {
            let acc = acc.clone();
            tokio::spawn(async move { acc.collect_shrink().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Still not converged: nothing released yet.
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // Release 4 KB; the collector should absorb it.
        assert_eq!(held.shrink(4 * KB), 4 * KB);
        collector.await.unwrap();
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        assert_eq!(acc.target_limit_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 4 * KB);
        drop(held);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn shrink_collector_does_not_jump_the_queue() {
        let acc = account(4);
        let held = acc.acquire(4 * KB).await.unwrap();

        // A waiter queues before the shrink starts.
        let acc2 = acc.clone();
        let waiter = tokio::spawn(async move { acc2.acquire(2 * KB).await.unwrap() });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Shrink to 2 while 4 are held; collector queues after the waiter.
        assert_eq!(acc.set_limit_bytes(2 * KB), 2 * KB);
        let collector = {
            let acc = acc.clone();
            tokio::spawn(async move { acc.collect_shrink().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Release everything: FIFO serves the earlier waiter first, then the
        // collector converges on what remains.
        drop(held);
        let got = waiter.await.unwrap();
        collector.await.unwrap();
        assert_eq!(got.granted_bytes(), 2 * KB);
        assert_eq!(acc.effective_limit_bytes(), 2 * KB);
        // The account is exactly full: the waiter holds the entire capacity.
        assert_eq!(acc.used_bytes(), 2 * KB);
        assert!(acc.try_acquire(KB).is_none());
        drop(got);
    }

    #[tokio::test]
    async fn two_faces_share_one_budget() {
        let acc = account(10);

        // Async face holds 6.
        let async_guard = acc
            .acquire_with_policy(6 * KB, OnExhaustedPolicy::Fail)
            .await
            .unwrap();

        // Sync face grows to exactly the remainder.
        let mut sync_guard = acc.try_acquire(0).unwrap();
        assert!(sync_guard.try_grow(4 * KB));
        assert_eq!(acc.used_bytes(), 10 * KB);
        // One budget: nothing left for either face.
        assert!(!sync_guard.try_grow(KB));
        assert!(acc.try_acquire(KB).is_none());

        // Async face releases; the sync face can claim it.
        drop(async_guard);
        assert!(sync_guard.try_grow(6 * KB));
        assert_eq!(acc.used_bytes(), 10 * KB);

        // Partial shrink returns capacity to the shared budget. Sub-permit
        // amounts round down: the remainder stays granted.
        assert_eq!(sync_guard.shrink(KB - 1), 0);
        assert_eq!(sync_guard.shrink(3 * KB + 512), 3 * KB);
        assert_eq!(acc.used_bytes(), 7 * KB);
        assert!(acc.try_acquire(3 * KB).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_storm_keeps_invariants() {
        let acc = account(12);
        let mut tasks = Vec::new();
        for i in 0..8u64 {
            let acc = acc.clone();
            tasks.push(tokio::spawn(async move {
                for j in 0..200u64 {
                    let bytes = ((i + j) % 3 + 1) * KB;
                    let policy = OnExhaustedPolicy::Wait {
                        timeout: Duration::from_millis(500),
                    };
                    if let Ok(guard) = acc.acquire_with_policy(bytes, policy).await {
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                }
            }));
        }

        // Storm of limit changes while the workload runs.
        for round in 0..40u64 {
            let target = if round % 2 == 0 { 6 } else { 12 };
            acc.set_limit_bytes(target * KB);
            acc.collect_shrink().await;
            // This task is the only `collect_shrink` caller, so the call
            // above won the single-flight flag and must have converged.
            assert_eq!(acc.effective_limit_bytes(), target * KB);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for task in tasks {
            task.await.unwrap();
        }
        acc.set_limit_bytes(6 * KB);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 6 * KB);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn concurrent_collect_shrink_single_flight_converges() {
        let acc = account(8);
        let mut held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        // The collector blocks on its first chunk: all capacity is granted.
        let collector = {
            let acc = acc.clone();
            tokio::spawn(async move { acc.collect_shrink().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!collector.is_finished());

        // A concurrent call early-returns (single-flight) without waiting
        // for convergence.
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // Retarget below the in-flight target: the running collector must
        // observe the new target and converge to it.
        assert_eq!(acc.set_limit_bytes(2 * KB), 6 * KB);
        assert_eq!(held.shrink(6 * KB), 6 * KB);
        collector.await.unwrap();
        assert_eq!(acc.effective_limit_bytes(), 2 * KB);
        assert_eq!(acc.used_bytes(), 2 * KB);

        // The flag was released on convergence: a fresh call wins it and
        // collects a new deficit instead of early-returning.
        assert_eq!(acc.set_limit_bytes(KB), KB);
        assert_eq!(held.shrink(KB), KB);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), KB);
        assert_eq!(acc.used_bytes(), KB);
        drop(held);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn cancelled_collect_shrink_releases_single_flight_flag() {
        let acc = account(8);
        let held = acc.acquire(8 * KB).await.unwrap();
        assert_eq!(acc.set_limit_bytes(4 * KB), 4 * KB);

        // Cancel the collector while it waits for its chunk: the future is
        // dropped at the acquire await point.
        let cancelled = tokio::time::timeout(Duration::from_millis(20), acc.collect_shrink()).await;
        assert!(cancelled.is_err());
        assert_eq!(acc.effective_limit_bytes(), 8 * KB);

        // The drop backstop released the flag: a later call must win it and
        // converge once capacity frees up.
        drop(held);
        acc.collect_shrink().await;
        assert_eq!(acc.effective_limit_bytes(), 4 * KB);
        assert_eq!(acc.used_bytes(), 0);
    }

    #[tokio::test]
    async fn oversized_request_fails_instead_of_clamping() {
        // The limit saturates at u32::MAX permits (4 TiB at KB granularity).
        let acc = Account::new(
            "sat",
            Category::Query,
            u64::MAX,
            PermitGranularity::Kilobyte,
        );
        assert_eq!(acc.granularity(), PermitGranularity::Kilobyte);
        let max_bytes = PermitGranularity::Kilobyte.permits_to_bytes(u32::MAX);
        assert_eq!(acc.target_limit_bytes(), max_bytes);

        // Requests beyond the saturated target fail loudly instead of being
        // clamped to the maximum permit count by the byte conversion.
        assert!(acc.acquire(max_bytes + KB).await.is_err());
        assert!(acc.try_acquire(max_bytes + KB).is_none());

        let mut guard = acc.acquire(KB).await.unwrap();
        assert!(!guard.try_grow(max_bytes));
        assert!(guard.grow(max_bytes).await.is_err());
    }

    #[tokio::test]
    async fn ledger_snapshot_aggregates() {
        let ledger = MemoryLedger::new(100 * KB);
        let a = ledger.register(
            "query/engine",
            Category::Query,
            50 * KB,
            PermitGranularity::Kilobyte,
        );
        let b = ledger.register(
            "ingest/request_bytes",
            Category::Ingest,
            20 * KB,
            PermitGranularity::Kilobyte,
        );
        let _g1 = a.acquire(10 * KB).await.unwrap();
        let _g2 = b.acquire(5 * KB).await.unwrap();

        assert_eq!(ledger.total_used_bytes(), 15 * KB);
        assert_eq!(ledger.unaccounted_bytes(40 * KB), 25 * KB);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].used_bytes, 10 * KB);
        assert_eq!(snapshot[1].used_bytes, 5 * KB);
    }

    /// Micro-benchmark of the sync face vs a raw atomic counter (stand-in for
    /// `GreedyMemoryPool`'s accounting). Run with:
    /// `cargo nextest run -p common-memory-manager bench_sync_face --run-ignored all`
    #[test]
    #[ignore]
    #[allow(clippy::print_stdout)]
    fn bench_sync_face_throughput() {
        use std::sync::atomic::AtomicU64;
        use std::time::Instant;

        const THREADS: usize = 4;
        const ITERS: u64 = 200_000;

        let acc = Account::new(
            "bench",
            Category::Query,
            1 << 30,
            PermitGranularity::Kilobyte,
        );
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let acc = acc.clone();
                s.spawn(move || {
                    let mut guard = acc.try_acquire(0).unwrap();
                    for _ in 0..ITERS {
                        assert!(guard.try_grow(KB));
                        guard.shrink(KB);
                    }
                });
            }
        });
        let ledger_ops = (THREADS as u64 * ITERS * 2) as f64 / start.elapsed().as_secs_f64();

        let counter = AtomicU64::new(0);
        let limit = 1u64 << 30;
        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let counter = &counter;
                s.spawn(move || {
                    for _ in 0..ITERS {
                        let mut ok = false;
                        while !ok {
                            let cur = counter.load(Ordering::Relaxed);
                            if cur + KB > limit {
                                break;
                            }
                            ok = counter
                                .compare_exchange(
                                    cur,
                                    cur + KB,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok();
                        }
                        counter.fetch_sub(KB, Ordering::Relaxed);
                    }
                });
            }
        });
        let atomic_ops = (THREADS as u64 * ITERS * 2) as f64 / start.elapsed().as_secs_f64();

        println!(
            "sync face: {:.2}M ops/s, raw atomic: {:.2}M ops/s, ratio: {:.2}x",
            ledger_ops / 1e6,
            atomic_ops / 1e6,
            atomic_ops / ledger_ops
        );
    }
}

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

//! [`LedgerMemoryPool`]: adapts a memory-ledger [`Account`] to DataFusion's
//! [`MemoryPool`] trait, so DataFusion operator memory draws from the same
//! budget as every other handle on the account (e.g. the scan tracker).
//!
//! # Mapping
//!
//! The pool holds one zero-sized [`AccountGuard`] acquired at construction and
//! grows/shrinks it as reservations change:
//!
//! - [`MemoryPool::try_grow`] maps to [`AccountGuard::try_grow`]; on failure it
//!   returns a DataFusion resources-exhausted error naming the consumer and the
//!   account's current usage and limit.
//! - [`MemoryPool::shrink`] maps to [`AccountGuard::shrink`], called only with
//!   whole-permit amounts once that many bytes have actually been freed
//!   ([`AccountGuard::shrink`] rounds up, so passing raw byte counts through
//!   would over-release).
//! - [`MemoryPool::reserved`] reports the bytes charged to the account plus any
//!   overdraft (see below).
//!
//! Account permits are coarse-grained (KB/MB), so the adapter tracks the exact
//! reserved bytes itself and keeps the guard at exactly
//! `ceil(reserved / granularity)` permits: sub-permit requests aggregate into
//! shared permits instead of charging one permit per call, and an aggregated
//! shrink releases exactly the permits the preceding grows acquired.
//!
//! # PoC decision: infallible `grow` overdrafts
//!
//! [`MemoryPool::grow`] is contractually infallible, but the backing account
//! can be exhausted. This adapter neither panics nor drops the accounting: the
//! portion the account cannot back is recorded in an internal overdraft
//! counter, [`MemoryPool::reserved`] includes it, and [`MemoryPool::shrink`]
//! writes it off before returning real permits to the account. The account
//! itself never over-grants, so an overdraft is visible as
//! `pool.reserved() > account.used_bytes()`. This is a PoC decision pending
//! architecture review.

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use common_memory_manager::PermitGranularity;
use common_memory_manager::ledger::{Account, AccountGuard};
use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use datafusion_common::{DataFusionError, resources_datafusion_err};

/// A [`MemoryPool`] backed by a memory-ledger [`Account`].
pub struct LedgerMemoryPool {
    account: Account,
    /// Permit granularity of `account`. Must match the granularity the account
    /// was created with; [`Account`] does not expose it.
    granularity: PermitGranularity,
    state: Mutex<PoolState>,
    /// Bytes `grow` charged but the account could not back (see module docs).
    overdraft: AtomicU64,
}

struct PoolState {
    /// The pool's grant on the account, always holding exactly
    /// `ceil(reserved_bytes / granularity)` permits minus the overdraft.
    guard: AccountGuard,
    /// Exact bytes reserved by DataFusion reservations, before permit rounding.
    reserved_bytes: u64,
}

impl LedgerMemoryPool {
    /// Creates a pool drawing from `account`. `granularity` must be the permit
    /// granularity the account was created with.
    pub fn new(account: Account, granularity: PermitGranularity) -> Self {
        let guard = account
            .try_acquire(0)
            .expect("zero-byte acquisition cannot fail");
        Self {
            account,
            granularity,
            state: Mutex::new(PoolState {
                guard,
                reserved_bytes: 0,
            }),
            overdraft: AtomicU64::new(0),
        }
    }

    /// Rounds `bytes` up to whole permits of the account's granularity.
    fn round_up_to_permits(&self, bytes: u64) -> u64 {
        self.granularity
            .permits_to_bytes(self.granularity.bytes_to_permits(bytes))
    }

    /// Grows the guard so that guard plus overdraft cover `new_reserved` bytes
    /// rounded up to whole permits. On failure returns the missing bytes and
    /// leaves the state untouched.
    fn try_cover(&self, state: &mut PoolState, new_reserved: u64) -> Result<(), u64> {
        let needed = self.round_up_to_permits(new_reserved);
        let covered = state.guard.granted_bytes() + self.overdraft.load(Ordering::Relaxed);
        if needed <= covered || state.guard.try_grow(needed - covered) {
            Ok(())
        } else {
            Err(needed - covered)
        }
    }

    fn insufficient_capacity_err(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> DataFusionError {
        resources_datafusion_err!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - memory account {} has {} used of {} limit",
            human_readable_size(additional),
            reservation.consumer().name(),
            human_readable_size(reservation.size()),
            self.account.name(),
            human_readable_size(self.account.used_bytes() as usize),
            human_readable_size(self.account.target_limit_bytes() as usize)
        )
    }

    #[cfg(test)]
    fn overdraft_bytes(&self) -> u64 {
        self.overdraft.load(Ordering::Relaxed)
    }
}

impl MemoryPool for LedgerMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock().unwrap();
        let new_reserved = state.reserved_bytes + additional as u64;
        if let Err(missing) = self.try_cover(&mut state, new_reserved) {
            self.overdraft.fetch_add(missing, Ordering::Relaxed);
        }
        state.reserved_bytes = new_reserved;
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock().unwrap();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(shrink as u64);
        let needed = self.round_up_to_permits(state.reserved_bytes);
        let overdraft = self.overdraft.load(Ordering::Relaxed);
        let release = (state.guard.granted_bytes() + overdraft).saturating_sub(needed);

        // Write off overdraft first; only the remainder returns real permits.
        let written_off = release.min(overdraft);
        if written_off > 0 {
            self.overdraft.fetch_sub(written_off, Ordering::Relaxed);
        }
        let returned = release - written_off;
        if returned > 0 {
            let released = state.guard.shrink(returned);
            // `returned` is permit-aligned and within the grant, so the
            // round-up inside `AccountGuard::shrink` releases exactly it.
            // Every operation rereads `granted_bytes` as ground truth, so a
            // deviation would be corrected on the next shrink anyway.
            debug_assert_eq!(released, returned);
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        let mut state = self.state.lock().unwrap();
        let new_reserved = state.reserved_bytes + additional as u64;
        if self.try_cover(&mut state, new_reserved).is_err() {
            return Err(self.insufficient_capacity_err(reservation, additional));
        }
        state.reserved_bytes = new_reserved;
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock().unwrap();
        (state.guard.granted_bytes() + self.overdraft.load(Ordering::Relaxed)) as usize
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.account.target_limit_bytes() as usize)
    }
}

impl fmt::Debug for LedgerMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LedgerMemoryPool")
            .field("account", &self.account.name())
            .field("reserved", &self.reserved())
            .field("overdraft", &self.overdraft.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_memory_manager::ledger::Category;
    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;

    const KB: u64 = 1024;

    fn pool_with_limit(limit_kb: u64) -> (Account, Arc<dyn MemoryPool>) {
        let account = Account::new(
            "query/engine",
            Category::Query,
            limit_kb * KB,
            PermitGranularity::Kilobyte,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(LedgerMemoryPool::new(
            account.clone(),
            PermitGranularity::Kilobyte,
        ));
        (account, pool)
    }

    #[test]
    fn grow_shrink_round_trip_returns_reserved_to_zero() {
        let (account, pool) = pool_with_limit(16);
        let r1 = MemoryConsumer::new("op-a").register(&pool);
        let r2 = MemoryConsumer::new("op-b").register(&pool);

        // Sub-permit requests aggregate instead of charging one permit each.
        r1.try_grow(512).unwrap();
        r1.try_grow(512).unwrap();
        assert_eq!(pool.reserved() as u64, KB);

        // 1024 + 1500 bytes reserved round up to three 1 KB permits.
        r2.try_grow(1500).unwrap();
        assert_eq!(pool.reserved() as u64, 3 * KB);
        assert_eq!(account.used_bytes(), 3 * KB);

        // Aggregated frees release exactly what the grows acquired.
        r1.free();
        assert_eq!(pool.reserved() as u64, 2 * KB);
        r2.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn repeated_sub_permit_shrinks_keep_books_consistent() {
        let (account, pool) = pool_with_limit(8);
        let r = MemoryConsumer::new("dribble").register(&pool);
        r.try_grow(2 * KB as usize).unwrap();

        // 20 x 100 B shrinks at KB granularity: the guard releases a permit
        // only once a whole permit's worth of bytes has actually been freed,
        // and pool/account books stay in lockstep the whole way.
        for _ in 0..20 {
            r.shrink(100);
            let expected = r.size().div_ceil(KB as usize) * KB as usize;
            assert_eq!(pool.reserved(), expected);
            assert_eq!(account.used_bytes() as usize, expected);
        }
        // 2048 - 2000 = 48 B still reserved: the guard is not emptied while
        // the reservation holds a balance.
        assert_eq!(r.size(), 48);
        assert_eq!(pool.reserved() as u64, KB);
        r.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn try_grow_beyond_limit_reports_resources_exhausted() {
        let (_account, pool) = pool_with_limit(4);
        let r = MemoryConsumer::new("hungry-operator").register(&pool);

        let err = r.try_grow(8 * KB as usize).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "unexpected error: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("hungry-operator"), "missing consumer: {msg}");
        assert!(msg.contains("query/engine"), "missing account: {msg}");

        // A failed grow leaves no charge behind; the limit itself still fits.
        assert_eq!(pool.reserved(), 0);
        r.try_grow(4 * KB as usize).unwrap();
    }

    #[test]
    fn concurrent_reservations_never_exceed_account_limit() {
        let (account, pool) = pool_with_limit(8);

        std::thread::scope(|s| {
            for t in 0..4u64 {
                let pool = pool.clone();
                let account = account.clone();
                s.spawn(move || {
                    let r = MemoryConsumer::new(format!("op-{t}")).register(&pool);
                    for i in 0..200u64 {
                        let bytes = ((t + i) % 3 + 1) * KB;
                        if r.try_grow(bytes as usize).is_ok() {
                            assert!(account.used_bytes() <= account.effective_limit_bytes());
                            assert!(pool.reserved() as u64 <= 8 * KB);
                            r.shrink(bytes as usize);
                        }
                    }
                });
            }
        });

        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn pool_shares_budget_with_external_account_handle() {
        let (account, pool) = pool_with_limit(10);
        let r = MemoryConsumer::new("pool-face").register(&pool);

        // An external handle on the same account takes most of the budget.
        let external = account.try_acquire(8 * KB).unwrap();
        // 2 KB left: a 4 KB pool-side grow must fail...
        let err = r.try_grow(4 * KB as usize).unwrap_err();
        assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));
        // ...while the exact remainder still fits.
        r.try_grow(2 * KB as usize).unwrap();
        assert_eq!(account.used_bytes(), 10 * KB);

        // Capacity released by the external face becomes visible to the pool.
        drop(external);
        r.try_grow(8 * KB as usize).unwrap();
        assert_eq!(account.used_bytes(), 10 * KB);
        r.free();
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn grow_overdrafts_instead_of_panicking_when_exhausted() {
        let account = Account::new(
            "query/engine",
            Category::Query,
            4 * KB,
            PermitGranularity::Kilobyte,
        );
        let pool = Arc::new(LedgerMemoryPool::new(
            account.clone(),
            PermitGranularity::Kilobyte,
        ));
        let dyn_pool: Arc<dyn MemoryPool> = pool.clone();
        let r = MemoryConsumer::new("infallible-grow").register(&dyn_pool);

        r.try_grow(4 * KB as usize).unwrap(); // account full
        r.grow(2 * KB as usize); // must not panic
        // The un-backed portion is carried as overdraft and reported honestly.
        assert_eq!(pool.overdraft_bytes(), 2 * KB);
        assert_eq!(dyn_pool.reserved() as u64, 6 * KB);
        // The account itself never over-grants.
        assert_eq!(account.used_bytes(), 4 * KB);

        // Shrink writes off the overdraft before returning real permits.
        r.shrink(2 * KB as usize);
        assert_eq!(pool.overdraft_bytes(), 0);
        assert_eq!(dyn_pool.reserved() as u64, 4 * KB);
        r.free();
        assert_eq!(dyn_pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);

        // With capacity available again, grow takes real permits.
        r.grow(KB as usize);
        assert_eq!(pool.overdraft_bytes(), 0);
        assert_eq!(account.used_bytes(), KB);
        r.free();
        assert_eq!(dyn_pool.reserved(), 0);
    }
}

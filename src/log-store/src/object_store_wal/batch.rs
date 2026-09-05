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

//! Accumulation of admitted entries into the batch that becomes the next object.

use std::collections::HashMap;

use store_api::logstore::EntryId;
use store_api::logstore::entry::Entry;
use store_api::storage::RegionId;

/// Entries admitted since the last seal, together with the largest entry id
/// handed out per region.
///
/// Entry ids are assigned in admission order, so the ids of a region increase
/// across batches. The accepted ids include entries that are not durable yet;
/// a failed flush rolls them back to the durable watermarks with [`reset`].
///
/// [`reset`]: OpenBatch::reset
#[derive(Debug)]
pub(super) struct OpenBatch {
    max_bytes: usize,
    entries: Vec<Entry>,
    estimated_bytes: usize,
    accepted_entry_ids: HashMap<RegionId, EntryId>,
}

impl OpenBatch {
    /// Creates an empty batch that continues the ids in `accepted_entry_ids`.
    pub(super) fn new(max_bytes: usize, accepted_entry_ids: HashMap<RegionId, EntryId>) -> Self {
        Self {
            max_bytes,
            entries: Vec::new(),
            estimated_bytes: 0,
            accepted_entry_ids,
        }
    }

    /// Admits `entries`, assigning each the next entry id of its region, and
    /// returns the last id assigned to every region in `entries`.
    pub(super) fn admit(&mut self, mut entries: Vec<Entry>) -> HashMap<RegionId, EntryId> {
        let mut last_entry_ids = HashMap::new();
        for entry in &mut entries {
            let region_id = entry.region_id();
            let entry_id = self
                .accepted_entry_ids
                .get(&region_id)
                .copied()
                .unwrap_or(0)
                + 1;
            entry.set_entry_id(entry_id);
            self.accepted_entry_ids.insert(region_id, entry_id);
            last_entry_ids.insert(region_id, entry_id);
            self.estimated_bytes += entry.estimated_size();
        }
        self.entries.extend(entries);
        last_entry_ids
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns true once the admitted entries reach the size limit.
    pub(super) fn should_seal(&self) -> bool {
        !self.is_empty() && self.estimated_bytes >= self.max_bytes
    }

    /// Takes the admitted entries out of the batch.
    pub(super) fn seal(&mut self) -> Vec<Entry> {
        self.estimated_bytes = 0;
        std::mem::take(&mut self.entries)
    }

    /// Drops the admitted entries and rolls the accepted ids back to
    /// `durable_entry_ids`, so the next admission hands out the same ids again.
    pub(super) fn reset(&mut self, durable_entry_ids: HashMap<RegionId, EntryId>) {
        self.entries.clear();
        self.estimated_bytes = 0;
        self.accepted_entry_ids = durable_entry_ids;
    }
}

#[cfg(test)]
mod tests {
    use store_api::logstore::entry::NaiveEntry;
    use store_api::logstore::provider::Provider;

    use super::*;

    fn entry(region_id: RegionId, payload_len: usize) -> Entry {
        Entry::Naive(NaiveEntry {
            provider: Provider::object_store_provider(region_id, "wal".to_string()),
            region_id,
            entry_id: 0,
            data: vec![0; payload_len],
        })
    }

    fn entry_ids(entries: &[Entry]) -> Vec<(RegionId, EntryId)> {
        entries
            .iter()
            .map(|entry| (entry.region_id(), entry.entry_id()))
            .collect()
    }

    #[test]
    fn test_batch_assigns_entry_ids_per_region() {
        let region_a = RegionId::new(1, 1);
        let region_b = RegionId::new(1, 2);
        let mut batch = OpenBatch::new(usize::MAX, HashMap::from([(region_a, 5)]));

        let first = batch.admit(vec![entry(region_a, 1), entry(region_b, 1)]);
        assert_eq!(HashMap::from([(region_a, 6), (region_b, 1)]), first);
        let second = batch.admit(vec![
            entry(region_b, 1),
            entry(region_a, 1),
            entry(region_b, 1),
        ]);
        assert_eq!(HashMap::from([(region_a, 7), (region_b, 3)]), second);

        assert_eq!(
            vec![
                (region_a, 6),
                (region_b, 1),
                (region_b, 2),
                (region_a, 7),
                (region_b, 3),
            ],
            entry_ids(&batch.seal())
        );
        assert!(batch.is_empty());
        assert_eq!(
            HashMap::from([(region_a, 8)]),
            batch.admit(vec![entry(region_a, 1)])
        );
    }

    #[test]
    fn test_batch_seals_at_size_limit() {
        let region_id = RegionId::new(1, 1);
        let first = entry(region_id, 8);
        let second = entry(region_id, 8);
        let max_bytes = first.estimated_size() + second.estimated_size();
        let mut batch = OpenBatch::new(max_bytes, HashMap::new());

        assert!(!batch.should_seal());
        batch.admit(vec![first]);
        assert!(!batch.should_seal());
        batch.admit(vec![second]);
        assert!(batch.should_seal());

        assert_eq!(2, batch.seal().len());
        assert!(!batch.should_seal());
    }

    #[test]
    fn test_batch_reset_hands_out_the_same_ids_again() {
        let region_id = RegionId::new(1, 1);
        let mut batch = OpenBatch::new(usize::MAX, HashMap::from([(region_id, 3)]));

        assert_eq!(
            HashMap::from([(region_id, 4)]),
            batch.admit(vec![entry(region_id, 1)])
        );
        batch.reset(HashMap::from([(region_id, 3)]));
        assert!(batch.is_empty());
        assert_eq!(
            HashMap::from([(region_id, 4)]),
            batch.admit(vec![entry(region_id, 1)])
        );
    }
}

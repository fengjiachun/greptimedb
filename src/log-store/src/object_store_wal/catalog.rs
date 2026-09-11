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

//! In-memory index over the footers of the objects of one WAL prefix.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};

use snafu::{OptionExt, ensure};
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{
    CorruptedWalObjectSnafu, InvalidWalEntryRangeSnafu, Result, WalObjectSequenceExhaustedSnafu,
};
use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, sequence_floor};
use crate::object_store_wal::format::FooterEntry;

/// Indexes objects by sequence and, per region, the objects that hold entries
/// of that region.
#[derive(Debug, Default)]
pub(super) struct ObjectCatalog {
    objects: BTreeMap<u64, Vec<FooterEntry>>,
    regions: BTreeMap<RegionId, RegionObjects>,
}

/// The objects that hold entries of one region, by sequence, and the largest
/// entry id the region ever had indexed. The id outlives the removal of the
/// object that held it, so that within one run of the store a durability wait
/// or an obsolete watermark keeps its reference point after the object was
/// collected; a store opened later learns only what is listed.
#[derive(Debug, Default)]
struct RegionObjects {
    objects: BTreeMap<u64, FooterEntry>,
    max_entry_id: EntryId,
}

/// One pass of [`ObjectCatalog::deletable_objects`].
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct DeletableObjects {
    /// The objects the pass accepted, in sequence order.
    pub(super) objects: Vec<u64>,
    /// The sequence the next pass starts at, zero once a pass reached the end
    /// of the catalog, so that a sweep wraps and objects a pass left behind
    /// are seen again.
    pub(super) resume_from: u64,
}

impl ObjectCatalog {
    /// Indexes the footer of the object `object_seq`. Objects may be inserted
    /// in any order, which lets recovery index them as it discovers them.
    /// Inserting a sequence that is already indexed is rejected, whether or not
    /// the footer matches the indexed one.
    pub(super) fn insert_object(
        &mut self,
        object_seq: u64,
        mut footer: Vec<FooterEntry>,
    ) -> Result<()> {
        ensure!(
            !footer.is_empty(),
            CorruptedWalObjectSnafu {
                reason: format!("object {object_seq} has an empty footer"),
            }
        );

        footer.sort_unstable_by_key(|entry| entry.region_id);
        for entries in footer.windows(2) {
            ensure!(
                entries[0].region_id != entries[1].region_id,
                CorruptedWalObjectSnafu {
                    reason: format!(
                        "object {object_seq} has duplicate footer entries for region {}",
                        entries[0].region_id
                    ),
                }
            );
        }
        for entry in &footer {
            ensure!(
                entry.entry_count > 0 && entry.min_entry_id <= entry.max_entry_id,
                CorruptedWalObjectSnafu {
                    reason: format!(
                        "object {object_seq} has invalid entry range {}..={} with {} entries for region {}",
                        entry.min_entry_id, entry.max_entry_id, entry.entry_count, entry.region_id
                    ),
                }
            );
        }

        // Object sequences are unique: recovery indexes every listed key once, and
        // accepting a repeated insertion would hide a caller that lost track of it.
        // Retrying an identical write stays an object store concern.
        ensure!(
            !self.objects.contains_key(&object_seq),
            CorruptedWalObjectSnafu {
                reason: format!("object {object_seq} is already indexed"),
            }
        );

        // Validate every region before mutating either index so insertion is atomic.
        for entry in &footer {
            let Some(region_objects) = self.regions.get(&entry.region_id) else {
                continue;
            };
            let region_objects = &region_objects.objects;
            if let Some((&previous_seq, previous)) = region_objects.range(..object_seq).next_back()
            {
                ensure!(
                    previous.max_entry_id < entry.min_entry_id,
                    CorruptedWalObjectSnafu {
                        reason: out_of_order(
                            entry.region_id,
                            previous_seq,
                            previous.max_entry_id,
                            object_seq,
                            entry.min_entry_id
                        ),
                    }
                );
            }
            if let Some((&next_seq, next)) = region_objects
                .range((Excluded(object_seq), Unbounded))
                .next()
            {
                ensure!(
                    entry.max_entry_id < next.min_entry_id,
                    CorruptedWalObjectSnafu {
                        reason: out_of_order(
                            entry.region_id,
                            object_seq,
                            entry.max_entry_id,
                            next_seq,
                            next.min_entry_id
                        ),
                    }
                );
            }
        }

        for entry in &footer {
            let region = self.regions.entry(entry.region_id).or_default();
            region.max_entry_id = region.max_entry_id.max(entry.max_entry_id);
            region.objects.insert(object_seq, entry.clone());
        }
        self.objects.insert(object_seq, footer);
        Ok(())
    }

    /// Removes the object `object_seq` from the index once it was deleted.
    /// The largest entry id of every region it held is kept.
    pub(super) fn remove_object(&mut self, object_seq: u64) {
        let Some(footer) = self.objects.remove(&object_seq) else {
            return;
        };
        for entry in footer {
            if let Some(region) = self.regions.get_mut(&entry.region_id) {
                region.objects.remove(&object_seq);
            }
        }
    }

    /// Returns true while the object `object_seq` is indexed.
    pub(super) fn contains_object(&self, object_seq: u64) -> bool {
        self.objects.contains_key(&object_seq)
    }

    /// Scans the catalog for objects garbage collection may delete, starting
    /// at `resume_from` and inspecting at most `scan_limit` objects, and
    /// returns at most `limit` of them together with the sequence the next
    /// pass starts at, see [`DeletableObjects`].
    ///
    /// Both bounds are what keeps a collection off the critical path of the
    /// actor: the scan bound caps the work of one pass however many objects
    /// the prefix holds, and the candidate bound caps the requests it starts.
    /// Because a pass resumes where the last one stopped and wraps at the
    /// end, a prefix is swept in passes that cost a bounded amount each, and
    /// objects whose delete failed are retried on a later sweep rather than
    /// holding up the objects behind them.
    ///
    /// An object is deletable when every segment it holds has its maximum
    /// entry id at or below the watermark of its region; a region without a
    /// watermark keeps its objects, and the comparison is on the full id, so a
    /// watermark that stops inside an object keeps the object. The object with
    /// the highest sequence is never deletable, so that the sequence resumes
    /// above everything ever written. Nothing is deletable while an entry id
    /// assigned under the earlier contiguous scheme names an object beyond
    /// the highest indexed one: the highest object alone must resume the
    /// sequence, which it does only once an object is durable at the raised
    /// sequence.
    pub(super) fn deletable_objects(
        &self,
        obsolete_entry_ids: &HashMap<RegionId, EntryId>,
        resume_from: u64,
        limit: usize,
        scan_limit: usize,
    ) -> DeletableObjects {
        let mut scan = DeletableObjects::default();
        let Some((&last_object_seq, _)) = self.objects.last_key_value() else {
            return scan;
        };
        if self.entry_id_floor() > last_object_seq.saturating_add(1) {
            return scan;
        }
        // A cursor at or past the object that is always kept has nothing left
        // to scan, so the sweep wraps.
        if resume_from >= last_object_seq {
            return scan;
        }
        for (inspected, (&object_seq, footer)) in
            self.objects.range(resume_from..last_object_seq).enumerate()
        {
            // Stopping before this object rather than after the last one
            // inspected is what makes the pass resumable: the next one starts
            // here, and nothing between the two passes is skipped.
            if inspected == scan_limit || scan.objects.len() == limit {
                scan.resume_from = object_seq;
                return scan;
            }
            let deletable = footer.iter().all(|entry| {
                obsolete_entry_ids
                    .get(&entry.region_id)
                    .is_some_and(|obsolete| entry.max_entry_id <= *obsolete)
            });
            if deletable {
                scan.objects.push(object_seq);
            }
        }
        // The pass reached the object that is always kept, so the next one
        // starts at the beginning and sees the objects this one left behind.
        scan
    }

    /// Returns the objects that hold entries of `region_id` overlapping
    /// `start_entry_id..=end_entry_id`, ordered by object sequence.
    pub(super) fn objects_for_entry_range(
        &self,
        region_id: RegionId,
        start_entry_id: u64,
        end_entry_id: u64,
    ) -> Result<Vec<(u64, &FooterEntry)>> {
        ensure!(
            start_entry_id <= end_entry_id,
            InvalidWalEntryRangeSnafu {
                region_id,
                start_entry_id,
                end_entry_id,
            }
        );

        let Some(region) = self.regions.get(&region_id) else {
            return Ok(Vec::new());
        };
        Ok(region
            .objects
            .iter()
            .filter(|(_, entry)| {
                entry.max_entry_id >= start_entry_id && entry.min_entry_id <= end_entry_id
            })
            .map(|(&object_seq, entry)| (object_seq, entry))
            .collect())
    }

    /// Returns the largest entry id ever indexed for `region_id`, which is
    /// kept after the object holding it was removed.
    pub(super) fn region_max_entry_id(&self, region_id: RegionId) -> Option<u64> {
        self.regions
            .get(&region_id)
            .map(|region| region.max_entry_id)
    }

    /// Returns the smallest sequence whose ids are greater than the largest
    /// entry id of every region, see [`sequence_floor`].
    fn entry_id_floor(&self) -> u64 {
        self.regions
            .values()
            .map(|region| sequence_floor(region.max_entry_id))
            .max()
            .unwrap_or(0)
    }

    /// Returns the sequence to assign to the next object written after recovery.
    ///
    /// An empty catalog starts at zero, so the first object of a prefix always
    /// takes sequence zero. Otherwise the sequence continues after the largest
    /// indexed one, which recovery discovers regardless of insertion order, and
    /// is raised further when the largest entry id of a region lies at or above
    /// the ids that sequence would assign: ids assigned under the earlier
    /// contiguous scheme carry no object information, and every new id of a
    /// region must be greater than every id it already has. A sequence at or
    /// above [`OBJECT_SEQ_LIMIT`] does not fit an entry id and is rejected.
    pub(super) fn next_object_seq(&self) -> Result<u64> {
        let after_last = match self.objects.last_key_value() {
            None => 0,
            Some((&last_object_seq, _)) => last_object_seq
                .checked_add(1)
                .context(WalObjectSequenceExhaustedSnafu { last_object_seq })?,
        };
        let next_object_seq = after_last.max(self.entry_id_floor());
        ensure!(
            next_object_seq < OBJECT_SEQ_LIMIT,
            WalObjectSequenceExhaustedSnafu {
                last_object_seq: next_object_seq - 1,
            }
        );
        Ok(next_object_seq)
    }

    /// Iterates over the indexed objects ordered by object sequence.
    pub(super) fn objects_in_order(&self) -> impl Iterator<Item = (u64, &[FooterEntry])> + '_ {
        self.objects
            .iter()
            .map(|(&object_seq, footer)| (object_seq, footer.as_slice()))
    }
}

fn out_of_order(
    region_id: RegionId,
    lower_object_seq: u64,
    lower_max_entry_id: u64,
    upper_object_seq: u64,
    upper_min_entry_id: u64,
) -> String {
    format!(
        "entry ranges of region {region_id} are not strictly increasing, object {lower_object_seq} ends at {lower_max_entry_id}, object {upper_object_seq} starts at {upper_min_entry_id}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::object_store_wal::batch::entry_id;

    #[test]
    fn test_catalog_indexes_objects_and_queries_ranges() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(2, 1);
        let mut catalog = ObjectCatalog::default();

        // Recovery may discover objects out of order.
        catalog
            .insert_object(
                2,
                vec![
                    footer_entry(region_one, 4, 6),
                    footer_entry(region_two, 8, 9),
                ],
            )
            .unwrap();
        catalog
            .insert_object(1, vec![footer_entry(region_one, 1, 3)])
            .unwrap();
        catalog
            .insert_object(4, vec![footer_entry(region_one, 10, 12)])
            .unwrap();

        assert_eq!(Some(12), catalog.region_max_entry_id(region_one));
        assert_eq!(Some(9), catalog.region_max_entry_id(region_two));
        assert_eq!(None, catalog.region_max_entry_id(RegionId::new(3, 1)));

        let objects = catalog.objects_for_entry_range(region_one, 3, 10).unwrap();
        assert_eq!(vec![1, 2, 4], object_seqs(&objects));
        assert_eq!(
            vec![1, 2, 4],
            catalog
                .objects_in_order()
                .map(|(object_seq, _)| object_seq)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_catalog_rejects_duplicate_object_sequences() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        let first = footer_entry(region_id, 1, 2);
        let second = footer_entry(RegionId::new(2, 1), 4, 5);

        catalog
            .insert_object(1, vec![first.clone(), second.clone()])
            .unwrap();

        // An identical footer is rejected just like a conflicting one.
        assert_corrupted(
            catalog.insert_object(1, vec![second, first.clone()]),
            "object 1 is already indexed",
        );

        let mut conflicting = first;
        conflicting.segment_offset += 1;
        assert_corrupted(
            catalog.insert_object(1, vec![conflicting]),
            "object 1 is already indexed",
        );
        assert_eq!(1, catalog.objects_in_order().count());
    }

    #[test]
    fn test_catalog_resumes_object_sequence_after_recovery() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        assert_eq!(0, catalog.next_object_seq().unwrap());

        // Recovery may discover objects out of order.
        catalog
            .insert_object(4, vec![footer_entry(region_id, 10, 12)])
            .unwrap();
        catalog
            .insert_object(1, vec![footer_entry(region_id, 1, 3)])
            .unwrap();

        assert_eq!(5, catalog.next_object_seq().unwrap());
    }

    #[test]
    fn test_catalog_raises_object_sequence_above_existing_entry_ids() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(1, 2);
        let mut catalog = ObjectCatalog::default();

        // Ids that fit below the ids of the next sequence leave it alone.
        catalog
            .insert_object(0, vec![footer_entry(region_one, 1, 3)])
            .unwrap();
        catalog
            .insert_object(
                1,
                vec![footer_entry(region_two, entry_id(1, 1), entry_id(1, 2))],
            )
            .unwrap();
        assert_eq!(2, catalog.next_object_seq().unwrap());

        // A contiguous id past them names a later object: the sequence
        // resumes above it, whichever region holds it.
        catalog
            .insert_object(2, vec![footer_entry(region_one, 5_000_000, 5_000_000)])
            .unwrap();
        assert_eq!(5, catalog.next_object_seq().unwrap());
        catalog
            .insert_object(
                3,
                vec![footer_entry(region_two, entry_id(7, 4), entry_id(7, 4))],
            )
            .unwrap();
        assert_eq!(8, catalog.next_object_seq().unwrap());
    }

    #[test]
    fn test_catalog_deletable_objects_follow_the_watermarks() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(2, 1);
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(
                0,
                vec![
                    footer_entry(region_one, entry_id(0, 1), entry_id(0, 1)),
                    footer_entry(region_two, entry_id(0, 1), entry_id(0, 1)),
                ],
            )
            .unwrap();
        catalog
            .insert_object(
                1,
                vec![footer_entry(region_one, entry_id(1, 1), entry_id(1, 2))],
            )
            .unwrap();
        catalog
            .insert_object(
                2,
                vec![footer_entry(region_two, entry_id(2, 1), entry_id(2, 1))],
            )
            .unwrap();
        catalog
            .insert_object(
                3,
                vec![footer_entry(region_one, entry_id(3, 1), entry_id(3, 1))],
            )
            .unwrap();

        // Without watermarks nothing is deletable.
        assert!(deletable(&catalog, &HashMap::new()).is_empty());
        // A watermark inside object 1 keeps it; object 0 holds a segment of
        // a region without a watermark.
        let mut obsolete = HashMap::from([(region_one, entry_id(1, 1))]);
        assert!(deletable(&catalog, &obsolete).is_empty());
        // A watermark at the last id of the segment releases object 1.
        obsolete.insert(region_one, entry_id(1, 2));
        assert_eq!(vec![1], deletable(&catalog, &obsolete));
        // Object 0 needs both regions at or above its segments; object 2 is
        // above the watermark of region two.
        obsolete.insert(region_two, entry_id(0, 1));
        assert_eq!(vec![0, 1], deletable(&catalog, &obsolete));
        // The highest-sequence object is kept whatever the watermarks.
        obsolete.insert(region_one, EntryId::MAX);
        obsolete.insert(region_two, EntryId::MAX);
        assert_eq!(vec![0, 1, 2], deletable(&catalog, &obsolete));

        // Removing objects keeps the largest entry id of every region, so a
        // region whose last object is gone still reports it.
        for object_seq in [0, 1, 2] {
            catalog.remove_object(object_seq);
        }
        catalog.remove_object(7);
        assert_eq!(vec![3], object_seqs_of(&catalog));
        assert!(!catalog.contains_object(2));
        assert!(catalog.contains_object(3));
        assert_eq!(
            Some(entry_id(3, 1)),
            catalog.region_max_entry_id(region_one)
        );
        assert_eq!(
            Some(entry_id(2, 1)),
            catalog.region_max_entry_id(region_two)
        );
        assert!(
            catalog
                .objects_for_entry_range(region_two, 0, EntryId::MAX)
                .unwrap()
                .is_empty()
        );
        assert!(deletable(&catalog, &obsolete).is_empty());
        assert_eq!(4, catalog.next_object_seq().unwrap());
    }

    #[test]
    fn test_catalog_scan_is_bounded_and_resumes_where_it_stopped() {
        let retained = RegionId::new(1, 1);
        let collectible = RegionId::new(2, 1);
        let mut catalog = ObjectCatalog::default();
        let insert = |catalog: &mut ObjectCatalog, object_seq, region_id| {
            catalog
                .insert_object(
                    object_seq,
                    vec![footer_entry(
                        region_id,
                        entry_id(object_seq, 1),
                        entry_id(object_seq, 1),
                    )],
                )
                .unwrap();
        };
        // Twenty objects of a region that never gets a watermark, then nine
        // of a region that does, then the object that is always kept.
        for object_seq in 0..20 {
            insert(&mut catalog, object_seq, retained);
        }
        for object_seq in 20..30 {
            insert(&mut catalog, object_seq, collectible);
        }
        let obsolete = HashMap::from([(collectible, entry_id(29, 1))]);

        // A pass inspects at most the scan bound and says where to resume,
        // whether or not it accepted anything, so the objects of the region
        // without a watermark are walked once per sweep, not once per pass.
        let mut pass = catalog.deletable_objects(&obsolete, 0, 4, 8);
        assert_eq!(
            DeletableObjects {
                objects: Vec::new(),
                resume_from: 8,
            },
            pass
        );
        pass = catalog.deletable_objects(&obsolete, pass.resume_from, 4, 8);
        assert_eq!(
            DeletableObjects {
                objects: Vec::new(),
                resume_from: 16,
            },
            pass
        );

        // The pass that reaches the collectible objects stops at the scan
        // bound with what it accepted so far.
        pass = catalog.deletable_objects(&obsolete, pass.resume_from, 4, 8);
        assert_eq!(
            DeletableObjects {
                objects: vec![20, 21, 22, 23],
                resume_from: 24,
            },
            pass
        );

        // The next one continues after them rather than at the beginning,
        // and stops at the candidate bound this time.
        pass = catalog.deletable_objects(&obsolete, pass.resume_from, 4, 8);
        assert_eq!(
            DeletableObjects {
                objects: vec![24, 25, 26, 27],
                resume_from: 28,
            },
            pass
        );

        // Object 29 is the one that is always kept, so the pass that reaches
        // it ends the sweep and wraps.
        pass = catalog.deletable_objects(&obsolete, pass.resume_from, 4, 8);
        assert_eq!(
            DeletableObjects {
                objects: vec![28],
                resume_from: 0,
            },
            pass
        );
        // A cursor past everything wraps as well.
        assert_eq!(
            DeletableObjects::default(),
            catalog.deletable_objects(&obsolete, 1_000, 4, 8)
        );
        // The bounds do not change which objects are deletable.
        assert_eq!(
            (20..29).collect::<Vec<u64>>(),
            deletable(&catalog, &obsolete)
        );
    }

    #[test]
    fn test_catalog_holds_deletion_while_contiguous_ids_name_a_later_object() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(2, 1);
        let mut catalog = ObjectCatalog::default();
        // Objects of the contiguous scheme: id 5_000_000 names object 4, so
        // the sequence resumes at 5 although object 2 is the highest.
        catalog
            .insert_object(0, vec![footer_entry(region_one, 1, 2)])
            .unwrap();
        catalog
            .insert_object(1, vec![footer_entry(region_one, 4_999_999, 5_000_000)])
            .unwrap();
        catalog
            .insert_object(2, vec![footer_entry(region_two, 1, 1)])
            .unwrap();
        assert_eq!(5, catalog.next_object_seq().unwrap());

        // Every object is below its watermark, but object 2 alone could not
        // resume the sequence above 5_000_000: nothing is deletable.
        let obsolete = HashMap::from([(region_one, 5_000_000), (region_two, 1)]);
        assert!(deletable(&catalog, &obsolete).is_empty());

        // An object durable at the raised sequence resumes it on its own, so
        // the old objects go and it is kept, even under a watermark at its
        // own entry.
        catalog
            .insert_object(
                5,
                vec![footer_entry(region_two, entry_id(5, 1), entry_id(5, 1))],
            )
            .unwrap();
        assert_eq!(vec![0, 1, 2], deletable(&catalog, &obsolete));
        let obsolete = HashMap::from([(region_one, 5_000_000), (region_two, entry_id(5, 1))]);
        assert_eq!(vec![0, 1, 2], deletable(&catalog, &obsolete));
        for object_seq in [0, 1, 2] {
            catalog.remove_object(object_seq);
        }
        assert_eq!(vec![5], object_seqs_of(&catalog));
        assert_eq!(6, catalog.next_object_seq().unwrap());
    }

    #[test]
    fn test_catalog_rejects_exhausted_object_sequence() {
        let region_id = RegionId::new(1, 1);
        let assert_exhausted = |catalog: &ObjectCatalog| {
            let error = catalog.next_object_seq().unwrap_err();
            assert!(
                error.to_string().contains("object sequence is exhausted"),
                "unexpected error: {error}"
            );
        };

        // The last sequence that fits an entry id is indexed.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(OBJECT_SEQ_LIMIT - 2, vec![footer_entry(region_id, 1, 2)])
            .unwrap();
        assert_eq!(OBJECT_SEQ_LIMIT - 1, catalog.next_object_seq().unwrap());
        catalog
            .insert_object(OBJECT_SEQ_LIMIT - 1, vec![footer_entry(region_id, 3, 4)])
            .unwrap();
        assert_exhausted(&catalog);

        // A sequence that does not fit was written by an earlier scheme.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(u64::MAX, vec![footer_entry(region_id, 1, 2)])
            .unwrap();
        assert_exhausted(&catalog);

        // An entry id that leaves no sequence above it.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(0, vec![footer_entry(region_id, u64::MAX, u64::MAX)])
            .unwrap();
        assert_exhausted(&catalog);
    }

    #[test]
    fn test_catalog_rejects_overlapping_or_reversed_region_ranges() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(2, vec![footer_entry(region_id, 10, 20)])
            .unwrap();

        assert_corrupted(
            catalog.insert_object(3, vec![footer_entry(region_id, 20, 30)]),
            "are not strictly increasing",
        );
        assert_corrupted(
            catalog.insert_object(3, vec![footer_entry(region_id, 5, 9)]),
            "are not strictly increasing",
        );
        assert_corrupted(
            catalog.insert_object(1, vec![footer_entry(region_id, 15, 19)]),
            "are not strictly increasing",
        );
        assert_eq!(1, catalog.objects_in_order().count());
    }

    #[test]
    fn test_catalog_rejects_duplicate_region_and_invalid_ranges() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        assert_corrupted(
            catalog.insert_object(
                1,
                vec![footer_entry(region_id, 1, 1), footer_entry(region_id, 2, 2)],
            ),
            "duplicate footer entries",
        );
        assert_corrupted(
            catalog.insert_object(1, vec![]),
            "object 1 has an empty footer",
        );

        let mut invalid = footer_entry(region_id, 2, 1);
        invalid.entry_count = 0;
        assert_corrupted(
            catalog.insert_object(1, vec![invalid]),
            "has invalid entry range 2..=1",
        );

        let error = catalog
            .objects_for_entry_range(region_id, 2, 1)
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntryRange { start_entry_id, end_entry_id, .. } if start_entry_id == 2 && end_entry_id == 1),
            "unexpected error: {error:?}"
        );
    }

    fn footer_entry(region_id: RegionId, min_entry_id: u64, max_entry_id: u64) -> FooterEntry {
        FooterEntry {
            region_id,
            min_entry_id,
            max_entry_id,
            entry_count: max_entry_id
                .checked_sub(min_entry_id)
                .and_then(|count| count.checked_add(1))
                .unwrap_or(0) as u32,
            segment_offset: min_entry_id.wrapping_mul(100),
            segment_len: 100,
            segment_crc32: min_entry_id as u32,
        }
    }

    fn object_seqs(objects: &[(u64, &FooterEntry)]) -> Vec<u64> {
        objects.iter().map(|(object_seq, _)| *object_seq).collect()
    }

    /// The whole deletable set in one unbounded pass, for the tests that do
    /// not exercise the bounds.
    fn deletable(catalog: &ObjectCatalog, obsolete: &HashMap<RegionId, EntryId>) -> Vec<u64> {
        catalog
            .deletable_objects(obsolete, 0, usize::MAX, usize::MAX)
            .objects
    }

    fn object_seqs_of(catalog: &ObjectCatalog) -> Vec<u64> {
        catalog
            .objects_in_order()
            .map(|(object_seq, _)| object_seq)
            .collect()
    }

    fn assert_corrupted(result: Result<()>, expected_reason: &str) {
        match result {
            Err(Error::CorruptedWalObject { reason, .. }) => assert!(
                reason.contains(expected_reason),
                "expected reason to contain {expected_reason:?}, actual {reason:?}"
            ),
            other => panic!("expected a corrupted object error, actual {other:?}"),
        }
    }
}

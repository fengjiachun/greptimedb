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

//! Recovery tests for regions on a real [`ObjectStoreLogStore`] over an
//! in-memory object store. The store seals objects only through its testing
//! hooks, so every test controls which entries are durable.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use api::v1::Rows;
use common_base::readable_size::ReadableSize;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::config::object_store::{AckMode, CorruptedSegmentAction, ObjectStoreWalConfig};
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use log_store::object_store_wal::{ObjectStoreLogStore, WalHole, entry_id};
use object_store::ObjectStore;
use object_store::services::Memory;
use rstest::rstest;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::mito_engine_options::SKIP_WAL_KEY;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    PathType, RegionDropRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest,
    RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::region::MitoRegionRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

/// The node prefix a standalone datanode derives from the root `cluster-a/wal`.
const PREFIX: &str = "cluster-a/wal/datanodes/0/epochs/0";
/// The engine runs two workers and these regions map to different ones, so
/// their writes can be admitted into the same open batch.
const REGION_A: RegionId = RegionId::new(1, 1);
const REGION_B: RegionId = RegionId::new(1, 2);
const WAIT: Duration = Duration::from_secs(30);

fn memory_store() -> ObjectStore {
    ObjectStore::new(Memory::default()).unwrap().finish()
}

/// Opens a store under `prefix` that never seals a batch on its own.
async fn open_store(object_store: &ObjectStore, prefix: &str) -> Arc<ObjectStoreLogStore> {
    open_store_with(object_store, prefix, AckMode::Durable).await
}

/// The configuration of a store under `prefix` that never seals a batch on
/// its own.
fn store_config(prefix: &str) -> ObjectStoreWalConfig {
    ObjectStoreWalConfig {
        storage_provider: String::new(),
        prefix: prefix.to_string(),
        flush_interval: Duration::from_secs(3600),
        max_batch_bytes: ReadableSize(u64::MAX),
        ..Default::default()
    }
}

/// Opens a store under `prefix` with `ack_mode` that never seals a batch on
/// its own.
async fn open_store_with(
    object_store: &ObjectStore,
    prefix: &str,
    ack_mode: AckMode,
) -> Arc<ObjectStoreLogStore> {
    let config = ObjectStoreWalConfig {
        ack_mode,
        ..store_config(prefix)
    };
    ObjectStoreLogStore::try_new(object_store.clone(), &config)
        .await
        .unwrap()
}

async fn new_engine(env: &mut TestEnv, store: Arc<ObjectStoreLogStore>) -> MitoEngine {
    let config = MitoConfig {
        num_workers: 2,
        ..Default::default()
    };
    env.create_engine_with_log_store(config, store).await
}

fn wal_options(prefix: &str) -> HashMap<String, String> {
    let options = WalOptions::ObjectStore(ObjectStoreWalOptions::new(prefix.to_string()));
    HashMap::from([(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&options).unwrap(),
    )])
}

fn provider(region_id: RegionId) -> Provider {
    Provider::object_store_provider(region_id, PREFIX.to_string())
}

/// Creates a region on the object store WAL with `extra_options` and returns
/// its table dir and row schema.
async fn create_region(
    engine: &MitoEngine,
    region_id: RegionId,
    extra_options: &[(&str, &str)],
) -> (String, Vec<api::v1::ColumnSchema>) {
    let mut builder = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options(PREFIX)[WAL_OPTIONS_KEY]);
    for (key, value) in extra_options {
        builder = builder.insert_option(key, value);
    }
    let request = builder.build();
    let table_dir = request.table_dir.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (table_dir, schema)
}

/// Opens a region whose options select the object store WAL under `prefix`
/// plus `extra_options`, and makes it writable.
async fn open_region(
    engine: &MitoEngine,
    region_id: RegionId,
    table_dir: &str,
    prefix: &str,
    extra_options: &[(&str, &str)],
) -> std::result::Result<(), BoxedError> {
    let mut options = wal_options(prefix);
    options.extend(
        extra_options
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.to_string(),
                options,
                skip_wal_replay: false,
                path_type: PathType::Bare,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await?;
    engine.set_region_role(region_id, RegionRole::Leader)
}

fn rows(schema: &[api::v1::ColumnSchema], start: usize, end: usize) -> Rows {
    Rows {
        schema: schema.to_vec(),
        rows: build_rows(start, end),
    }
}

/// Returns every row of the region as a table, so two scans can be compared
/// row by row.
async fn scan_rows(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .pretty_print()
        .unwrap()
}

fn region(engine: &MitoEngine, region_id: RegionId) -> MitoRegionRef {
    engine.get_region(region_id).unwrap()
}

/// The entry ids a region tracks in memory and in its manifest.
#[derive(Debug, PartialEq, Eq)]
struct EntryIds {
    flushed_entry_id: u64,
    last_entry_id: u64,
    topic_latest_entry_id: u64,
    manifest_flushed_entry_id: u64,
    memtable_rows: u64,
}

async fn entry_ids(engine: &MitoEngine, region_id: RegionId) -> EntryIds {
    let region = region(engine, region_id);
    let current = region.version_control.current();
    let manifest = region.manifest_ctx.manifest().await;
    EntryIds {
        flushed_entry_id: current.version.flushed_entry_id,
        last_entry_id: current.last_entry_id,
        topic_latest_entry_id: region.topic_latest_entry_id.load(Ordering::Relaxed),
        manifest_flushed_entry_id: manifest.flushed_entry_id,
        memtable_rows: current.version.memtables.num_rows(),
    }
}

/// Writes rows through the engine and seals them into one WAL object.
///
/// A put blocks until its entries are durable, so the writes run in the
/// background until the store has admitted all of them and the batch is
/// sealed by hand.
struct SealedWriter {
    store: Arc<ObjectStoreLogStore>,
    admitted: usize,
}

impl SealedWriter {
    fn new(store: &Arc<ObjectStoreLogStore>) -> Self {
        Self {
            store: store.clone(),
            admitted: 0,
        }
    }

    async fn put_and_seal(&mut self, engine: &MitoEngine, writes: Vec<(RegionId, Rows)>) {
        let handles = writes
            .into_iter()
            .map(|(region_id, rows)| {
                let engine = engine.clone();
                tokio::spawn(async move { put_rows(&engine, region_id, rows).await })
            })
            .collect::<Vec<_>>();
        self.admitted += handles.len();
        tokio::time::timeout(WAIT, self.store.wait_for_admitted_appends(self.admitted))
            .await
            .expect("writes must reach the store")
            .unwrap();
        self.store.seal_open_batch().await.unwrap();
        for handle in handles {
            handle.await.unwrap();
        }
    }
}

fn latest(store: &ObjectStoreLogStore, region_id: RegionId) -> u64 {
    store.latest_entry_id(&provider(region_id)).unwrap()
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_reopen_after_partial_flush_replays_only_unflushed_regions(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-partial-flush").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Two objects, each holding one entry of both regions: the ids of a
    // region are its position under the sequence of its object.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_B, rows(&schema_b, 3, 5)),
                (REGION_A, rows(&schema_a, 2, 4)),
            ],
        )
        .await;
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(entry_id(1, 1), latest(&store, REGION_B));

    // Flushing empties the memtables, so the topic latest entry id follows
    // the store.
    flush_region(&engine, REGION_A, None).await;
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    let rows_a = scan_rows(&engine, REGION_A).await;
    let rows_b = scan_rows(&engine, REGION_B).await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, PREFIX, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, PREFIX, &[])
        .await
        .unwrap();

    // Region A replays nothing: it flushed both entries, so the topic latest
    // entry id comes from the store. Region B replays both of its entries.
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 5,
        },
        entry_ids(&engine, REGION_B).await
    );
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(entry_id(1, 1), latest(&store, REGION_B));
    assert_eq!(rows_a, scan_rows(&engine, REGION_A).await);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_reopen_after_abrupt_drop_replays_durable_entries_once(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-abrupt-drop").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry is durable as an object but the manifest never learns of it.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 3))])
        .await;
    assert_eq!(1, latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;

    // Drops the engine without stopping it, like a crashed process.
    drop(engine);
    drop(writer);
    drop(store);

    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();

    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 1,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 3,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(1, latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(3, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[rstest]
#[case(CorruptedSegmentAction::Skip)]
#[case(CorruptedSegmentAction::Fail)]
#[tokio::test]
async fn test_reopen_with_a_corrupted_segment(
    #[case] on_corrupted_segment: CorruptedSegmentAction,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-corrupted-segment").await;
    let object_store = memory_store();
    let store = open_store(&object_store, PREFIX).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Two objects, each holding one entry of both regions; nothing is flushed.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 2, 4)),
                (REGION_B, rows(&schema_b, 3, 5)),
            ],
        )
        .await;
    let rows_b = scan_rows(&engine, REGION_B).await;
    engine.stop().await.unwrap();

    // The segment of region A in object 0 is damaged.
    let (path, segment) = store
        .segment_location(&provider(REGION_A), 0)
        .unwrap()
        .unwrap();
    let mut bytes = object_store.read(&path).await.unwrap().to_vec();
    bytes[segment.end as usize - 1] ^= 1;
    object_store.write(&path, bytes).await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    let config = ObjectStoreWalConfig {
        on_corrupted_segment,
        ..store_config(PREFIX)
    };
    let store = ObjectStoreLogStore::try_new(object_store.clone(), &config)
        .await
        .unwrap();
    let engine = new_engine(&mut env, store.clone()).await;

    // Region B replays both of its entries whatever the damage does to A.
    open_region(&engine, REGION_B, &table_dir_b, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);
    assert!(store.wal_holes(&provider(REGION_B)).unwrap().is_empty());

    let opened = open_region(&engine, REGION_A, &table_dir_a, PREFIX, &[]).await;
    match on_corrupted_segment {
        // Region A replays the entry of object 1 alone and records the
        // segment of object 0 as a hole.
        CorruptedSegmentAction::Skip => {
            opened.unwrap();
            assert_eq!(
                EntryIds {
                    flushed_entry_id: 0,
                    last_entry_id: entry_id(1, 1),
                    topic_latest_entry_id: 0,
                    manifest_flushed_entry_id: 0,
                    memtable_rows: 2,
                },
                entry_ids(&engine, REGION_A).await
            );
            assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
            assert_eq!(
                vec![WalHole {
                    path,
                    object_seq: 0,
                    min_entry_id: 1,
                    max_entry_id: 1,
                }],
                store.wal_holes(&provider(REGION_A)).unwrap()
            );
        }
        // The read fails, so the region does not open.
        CorruptedSegmentAction::Fail => {
            let err = opened.unwrap_err();
            assert_eq!(StatusCode::Unexpected, err.status_code());
            assert!(!engine.is_region_exists(REGION_A));
            assert!(store.wal_holes(&provider(REGION_A)).unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_open_region_rejects_mismatched_wal_prefix() {
    let mut env = TestEnv::with_prefix("object-store-wal-prefix-mismatch").await;
    let object_store = memory_store();
    let store = open_store(&object_store, PREFIX).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 3))])
        .await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The process now runs its store under the next generation while the
    // region still persists the prefix it was created with.
    let other_prefix = "cluster-a/wal/datanodes/0/epochs/1";
    let store = open_store(&object_store, other_prefix).await;
    let engine = new_engine(&mut env, store).await;
    let err = open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
    let message = err.output_msg();
    assert!(
        message.contains(PREFIX) && message.contains(other_prefix),
        "unexpected error: {message}"
    );
    assert!(!engine.is_region_exists(REGION_A));
}

#[tokio::test]
async fn test_reopen_on_empty_prefix_without_durable_entries() {
    let mut env = TestEnv::with_prefix("object-store-wal-empty-prefix").await;
    let store = open_store(&memory_store(), PREFIX).await;
    let engine = new_engine(&mut env, store.clone()).await;
    // Region A never writes; region B skips the WAL and flushes its rows.
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[(SKIP_WAL_KEY, "true")]).await;
    put_rows(&engine, REGION_B, rows(&schema_b, 0, 3)).await;
    flush_region(&engine, REGION_B, None).await;
    let rows_b = scan_rows(&engine, REGION_B).await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // A fresh object store holds no object under the prefix.
    let object_store = memory_store();
    let store = open_store(&object_store, PREFIX).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, PREFIX, &[])
        .await
        .unwrap();
    open_region(
        &engine,
        REGION_B,
        &table_dir_b,
        PREFIX,
        &[(SKIP_WAL_KEY, "true")],
    )
    .await
    .unwrap();

    assert_eq!(0, latest(&store, REGION_A));
    assert_eq!(0, latest(&store, REGION_B));
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 0,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    // Region B keeps the object store provider but wrote nothing to it; its
    // rows come back from the SST alone.
    assert_eq!(provider(REGION_B), region(&engine, REGION_B).provider);
    assert_eq!(0, entry_ids(&engine, REGION_B).await.memtable_rows);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);

    // The region is usable and its entry ids start from one.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 0, 2))])
        .await;
    assert_eq!(1, latest(&store, REGION_A));
    assert_eq!(1, entry_ids(&engine, REGION_A).await.last_entry_id);
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_entry_ids_continue_across_two_restarts(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-two-restarts").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry of object 0 is flushed, the entry of object 1 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 2))])
        .await;
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 2, 4))])
        .await;
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // First restart replays the entry of object 1 only.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 1,
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: 1,
            manifest_flushed_entry_id: 1,
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);

    // The sequence continues at object 2, whose entry is flushed; the entry
    // of object 3 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 4, 6))])
        .await;
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    assert_eq!(
        entry_id(2, 1),
        entry_ids(&engine, REGION_A).await.last_entry_id
    );
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 6, 8))])
        .await;
    assert_eq!(entry_id(3, 1), latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Second restart replays the entry of object 3 only.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(2, 1),
            last_entry_id: entry_id(3, 1),
            topic_latest_entry_id: entry_id(2, 1),
            manifest_flushed_entry_id: entry_id(2, 1),
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(entry_id(3, 1), latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(8, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(
        8,
        region(&engine, REGION_A)
            .version_control
            .committed_sequence()
    );
}

/// Lists the WAL objects under the prefix.
async fn wal_objects(object_store: &ObjectStore) -> Vec<String> {
    object_store
        .list(&format!("{PREFIX}/objects/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_string())
        .collect()
}

/// Returns the sequences of the WAL objects under the prefix, in order.
async fn wal_object_seqs(object_store: &ObjectStore) -> Vec<u64> {
    let mut seqs = wal_objects(object_store)
        .await
        .into_iter()
        .map(|path| {
            path.rsplit('/')
                .next()
                .and_then(|name| name.strip_suffix(".wal"))
                .and_then(|seq| seq.parse().ok())
                .unwrap_or_else(|| panic!("unexpected WAL object key {path}"))
        })
        .collect::<Vec<u64>>();
    seqs.sort_unstable();
    seqs
}

/// Waits for the collection the last `obsolete` started.
async fn wait_for_collection(store: &ObjectStoreLogStore) {
    tokio::time::timeout(WAIT, store.wait_for_garbage_collection())
        .await
        .expect("collection must complete");
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_flush_collects_objects_below_the_watermark(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-collection").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Object 0 holds an entry of both regions, object 1 one of region A and
    // object 2 one of region B.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 2, 4))])
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 3, 5))])
        .await;
    assert_eq!(vec![0, 1, 2], wal_object_seqs(&object_store).await);
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(entry_id(2, 1), latest(&store, REGION_B));

    // Flushing region A moves its watermark to the entry of object 1. That
    // object held nothing else and goes; object 0 holds an entry of region
    // B, which has no watermark, and object 2 is the highest: both stay.
    flush_region(&engine, REGION_A, None).await;
    wait_for_collection(&store).await;
    assert_eq!(vec![0, 2], wal_object_seqs(&object_store).await);
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    let rows_a = scan_rows(&engine, REGION_A).await;
    let rows_b = scan_rows(&engine, REGION_B).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Region A replays nothing above its watermark; region B replays its
    // entries of objects 0 and 2. Opening re-establishes both watermarks
    // and collects nothing: object 0 is still needed by region B and object
    // 2 is the highest. The largest id the store lists for region A is now
    // the entry of object 0, which only the pruning hint consumes.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, PREFIX, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, PREFIX, &[])
        .await
        .unwrap();
    wait_for_collection(&store).await;
    assert_eq!(vec![0, 2], wal_object_seqs(&object_store).await);
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(0, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: entry_id(2, 1),
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 5,
        },
        entry_ids(&engine, REGION_B).await
    );
    assert_eq!(rows_a, scan_rows(&engine, REGION_A).await);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);

    // The sequence resumes after the retained object, so every new id is
    // above every id either region had, including those of object 1.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 4, 6)),
                (REGION_B, rows(&schema_b, 5, 7)),
            ],
        )
        .await;
    assert_eq!(vec![0, 2, 3], wal_object_seqs(&object_store).await);
    assert_eq!(
        entry_id(3, 1),
        entry_ids(&engine, REGION_A).await.last_entry_id
    );
    assert_eq!(
        entry_id(3, 1),
        entry_ids(&engine, REGION_B).await.last_entry_id
    );

    // Flushing region B puts both regions above the segments of objects 0
    // and 2; object 3 is the highest and holds the unflushed entry of A.
    flush_region(&engine, REGION_B, None).await;
    wait_for_collection(&store).await;
    assert_eq!(vec![3], wal_object_seqs(&object_store).await);
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(3, 1),
            last_entry_id: entry_id(3, 1),
            topic_latest_entry_id: entry_id(3, 1),
            manifest_flushed_entry_id: entry_id(3, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_B).await
    );
    let rows_a = scan_rows(&engine, REGION_A).await;
    let rows_b = scan_rows(&engine, REGION_B).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Region A replays its entry of object 3, region B nothing.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, PREFIX, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, PREFIX, &[])
        .await
        .unwrap();
    wait_for_collection(&store).await;
    assert_eq!(vec![3], wal_object_seqs(&object_store).await);
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(3, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(
        EntryIds {
            flushed_entry_id: entry_id(3, 1),
            last_entry_id: entry_id(3, 1),
            topic_latest_entry_id: entry_id(3, 1),
            manifest_flushed_entry_id: entry_id(3, 1),
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_B).await
    );
    assert_eq!(rows_a, scan_rows(&engine, REGION_A).await);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(6, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(7, engine.get_region_statistic(REGION_B).unwrap().num_rows);
}

/// Requests a flush of the region in the background and returns its handle;
/// the result is up to the caller.
fn spawn_flush(
    engine: &MitoEngine,
    region_id: RegionId,
) -> tokio::task::JoinHandle<std::result::Result<(), BoxedError>> {
    let engine = engine.clone();
    tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                    reason: None,
                }),
            )
            .await
            .map(|_| ())
    })
}

#[tokio::test]
async fn test_flush_waits_until_the_wal_is_durable() {
    let mut env = TestEnv::with_prefix("object-store-wal-flush-barrier").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The write is acknowledged on admission; its object is not created
    // while creates are held.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 1,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 3,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(0, latest(&store, REGION_A));

    // The flush writes its SST, then waits for entry 1 to be durable before
    // it records the entry as flushed in the manifest.
    let flush = spawn_flush(&engine, REGION_A);
    let seal = {
        let store = store.clone();
        tokio::spawn(async move { store.seal_open_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!flush.is_finished());
    assert!(!seal.is_finished());
    assert_eq!(
        0,
        entry_ids(&engine, REGION_A).await.manifest_flushed_entry_id
    );
    assert!(wal_objects(&object_store).await.is_empty());

    store.release_creates();
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, flush)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(1, latest(&store, REGION_A));
    assert_eq!(
        EntryIds {
            flushed_entry_id: 1,
            last_entry_id: 1,
            topic_latest_entry_id: 1,
            manifest_flushed_entry_id: 1,
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(3, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_enqueued_crash_before_the_object_exists_replays_durable_entries() {
    let mut env = TestEnv::with_prefix("object-store-wal-enqueued-crash").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // Entry 1 is acknowledged and a flush is attempted, but the object is
    // never created: the flush waits and the manifest keeps watermark 0.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(1, entry_ids(&engine, REGION_A).await.last_entry_id);
    let flush = spawn_flush(&engine, REGION_A);
    let seal = {
        let store = store.clone();
        tokio::spawn(async move { store.seal_open_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!flush.is_finished());
    assert_eq!(
        0,
        entry_ids(&engine, REGION_A).await.manifest_flushed_entry_id
    );
    assert!(wal_objects(&object_store).await.is_empty());

    // The process dies with the create still held.
    drop(engine);
    drop(store);
    flush.abort();
    seal.abort();
    assert!(wal_objects(&object_store).await.is_empty());

    // Nothing is durable, so nothing replays and the region starts over
    // from entry 1; the lost entry was inside the unpersisted backlog.
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 0,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(0, latest(&store, REGION_A));
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 3, 5))])
        .await;
    assert_eq!(1, latest(&store, REGION_A));
    assert_eq!(1, entry_ids(&engine, REGION_A).await.last_entry_id);
    assert_eq!(1, wal_objects(&object_store).await.len());
    let rows_before = scan_rows(&engine, REGION_A).await;
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    drop(engine);
    drop(writer);
    drop(store);

    // The second restart replays the entry that became durable and skips
    // nothing: the manifest never named an entry that was not durable.
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 1,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(1, latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_drop_cancels_a_flush_waiting_for_wal_durability() {
    let mut env = TestEnv::with_prefix("object-store-wal-flush-barrier-drop").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The flush reaches the durability barrier and waits for a create that
    // is held.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let flush = spawn_flush(&engine, REGION_A);
    let seal = {
        let store = store.clone();
        tokio::spawn(async move { store.seal_open_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!flush.is_finished());

    // The drop cancels the flush instead of waiting behind the upload.
    tokio::time::timeout(
        WAIT,
        engine.handle_request(
            REGION_A,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!engine.is_region_exists(REGION_A));
    assert!(wal_objects(&object_store).await.is_empty());
    assert!(
        tokio::time::timeout(WAIT, flush)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );

    store.release_creates();
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[rstest]
#[case(RegionTruncateRequest::All)]
#[case(RegionTruncateRequest::Unflushed)]
#[tokio::test]
async fn test_enqueued_truncate_waits_until_the_wal_is_durable(
    #[case] request: RegionTruncateRequest,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-truncate-barrier").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // Entry 1 is acknowledged and a truncate is attempted, but the object is
    // never created: the truncate waits and the manifest keeps frontier 0.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(1, entry_ids(&engine, REGION_A).await.last_entry_id);
    let truncate = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .handle_request(REGION_A, RegionRequest::Truncate(request))
                .await
                .map(|_| ())
        })
    };
    let seal = {
        let store = store.clone();
        tokio::spawn(async move { store.seal_open_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!truncate.is_finished());
    let manifest = region(&engine, REGION_A).manifest_ctx.manifest().await;
    assert_eq!(0, manifest.flushed_entry_id);
    assert_eq!(None, manifest.truncated_entry_id);
    assert!(wal_objects(&object_store).await.is_empty());

    // The process dies with the create still held.
    drop(engine);
    drop(store);
    truncate.abort();
    seal.abort();

    // Nothing is durable and the manifest names no entry, so the region
    // starts over from entry 1, which becomes durable this time.
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 0,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 3, 5))])
        .await;
    assert_eq!(1, latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;
    drop(engine);
    drop(writer);
    drop(store);

    // The second restart replays the durable entry instead of skipping it.
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 1,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_flush_does_not_publish_a_frontier_for_a_lost_enqueued_backlog() {
    let mut env = TestEnv::with_prefix("object-store-wal-lost-backlog").await;
    let object_store = memory_store();
    let store = open_store_with(&object_store, PREFIX, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // Entry 1 is acknowledged, then its create fails while stop has begun
    // but the stop command is not handled yet: the backlog is lost.
    store.hold_creates();
    store.fail_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let seal = {
        let store = store.clone();
        tokio::spawn(async move { store.seal_open_batch().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    store.begin_stop();
    store.release_creates();
    assert!(
        tokio::time::timeout(WAIT, seal)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(wal_objects(&object_store).await.is_empty());

    // A flush in that window fails at the barrier instead of recording the
    // lost entry as flushed.
    let flush = spawn_flush(&engine, REGION_A);
    assert!(
        tokio::time::timeout(WAIT, flush)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(
        0,
        entry_ids(&engine, REGION_A).await.manifest_flushed_entry_id
    );
    assert!(store.stop().await.is_err());
}

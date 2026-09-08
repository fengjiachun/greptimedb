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
use common_wal::config::object_store::{AckMode, ObjectStoreWalConfig};
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use log_store::object_store_wal::ObjectStoreLogStore;
use object_store::ObjectStore;
use object_store::services::Memory;
use rstest::rstest;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::mito_engine_options::SKIP_WAL_KEY;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{PathType, RegionFlushRequest, RegionOpenRequest, RegionRequest};
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

/// Opens a store under `prefix` with `ack_mode` that never seals a batch on
/// its own.
async fn open_store_with(
    object_store: &ObjectStore,
    prefix: &str,
    ack_mode: AckMode,
) -> Arc<ObjectStoreLogStore> {
    let config = ObjectStoreWalConfig {
        storage_provider: String::new(),
        prefix: prefix.to_string(),
        flush_interval: Duration::from_secs(3600),
        max_batch_bytes: ReadableSize(u64::MAX),
        ack_mode,
        ..Default::default()
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

    // Two objects, each holding one entry of both regions.
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
    assert_eq!(2, latest(&store, REGION_A));
    assert_eq!(2, latest(&store, REGION_B));

    // Flushing empties the memtables, so the topic latest entry id follows
    // the store.
    flush_region(&engine, REGION_A, None).await;
    assert_eq!(
        EntryIds {
            flushed_entry_id: 2,
            last_entry_id: 2,
            topic_latest_entry_id: 2,
            manifest_flushed_entry_id: 2,
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
            flushed_entry_id: 2,
            last_entry_id: 2,
            topic_latest_entry_id: 2,
            manifest_flushed_entry_id: 2,
            memtable_rows: 0,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(
        EntryIds {
            flushed_entry_id: 0,
            last_entry_id: 2,
            topic_latest_entry_id: 0,
            manifest_flushed_entry_id: 0,
            memtable_rows: 5,
        },
        entry_ids(&engine, REGION_B).await
    );
    assert_eq!(2, latest(&store, REGION_A));
    assert_eq!(2, latest(&store, REGION_B));
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

    // Entry 1 is flushed, entry 2 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 2))])
        .await;
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 2, 4))])
        .await;
    assert_eq!(2, latest(&store, REGION_A));
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // First restart replays entry 2 only.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 1,
            last_entry_id: 2,
            topic_latest_entry_id: 1,
            manifest_flushed_entry_id: 1,
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);

    // Entry 3 continues the sequence and is flushed, entry 4 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 4, 6))])
        .await;
    assert_eq!(3, latest(&store, REGION_A));
    assert_eq!(3, entry_ids(&engine, REGION_A).await.last_entry_id);
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 6, 8))])
        .await;
    assert_eq!(4, latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Second restart replays entry 4 only.
    let store = open_store_with(&object_store, PREFIX, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, PREFIX, &[])
        .await
        .unwrap();
    assert_eq!(
        EntryIds {
            flushed_entry_id: 3,
            last_entry_id: 4,
            topic_latest_entry_id: 3,
            manifest_flushed_entry_id: 3,
            memtable_rows: 2,
        },
        entry_ids(&engine, REGION_A).await
    );
    assert_eq!(4, latest(&store, REGION_A));
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

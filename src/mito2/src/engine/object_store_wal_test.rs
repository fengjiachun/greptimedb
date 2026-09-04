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

//! Tests for regions whose WAL options select the object store WAL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    PathType, RegionCleanUpRequest, RegionCloseRequest, RegionDropRequest, RegionOpenRequest,
    RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    CreateRequestBuilder, SharedNamespaceLogStore, TestEnv, build_rows, flush_region, put_rows,
    reopen_region, rows_schema,
};

const PREFIX: &str = "cluster-a/wal";

fn wal_options() -> HashMap<String, String> {
    let options = WalOptions::ObjectStore(ObjectStoreWalOptions::new(PREFIX.to_string()));
    HashMap::from([(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&options).unwrap(),
    )])
}

fn object_store_provider(region_id: RegionId) -> Provider {
    Provider::object_store_provider(region_id, PREFIX.to_string())
}

fn open_request(table_dir: String) -> RegionOpenRequest {
    RegionOpenRequest {
        engine: String::new(),
        table_dir,
        options: wal_options(),
        skip_wal_replay: false,
        path_type: PathType::Bare,
        checkpoint: None,
        requirements: Default::default(),
    }
}

async fn new_engine(env: &mut TestEnv) -> (MitoEngine, Arc<SharedNamespaceLogStore>) {
    let log_store = Arc::new(SharedNamespaceLogStore::default());
    let engine = env
        .create_engine_with_log_store(MitoConfig::default(), log_store.clone())
        .await;
    (engine, log_store)
}

/// Creates a region with object store WAL options and returns its table dir and row schema.
async fn create_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> (String, Vec<api::v1::ColumnSchema>) {
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let table_dir = request.table_dir.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (table_dir, schema)
}

/// Creates a region and writes rows `[start, end)` to it in one WAL entry.
async fn create_region_with_rows(
    engine: &MitoEngine,
    region_id: RegionId,
    start: usize,
    end: usize,
) -> String {
    let (table_dir, schema) = create_region(engine, region_id).await;
    put_rows(
        engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(start, end),
        },
    )
    .await;
    table_dir
}

async fn close_region(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
}

async fn count_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

#[tokio::test]
async fn test_create_and_reopen_region_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-reopen").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_id = RegionId::new(1, 1);
    let provider = object_store_provider(region_id);

    let table_dir = create_region_with_rows(&engine, region_id, 0, 3).await;
    assert_eq!(provider, engine.get_region(region_id).unwrap().provider);
    assert_eq!(1, log_store.latest_entry_id(&provider).unwrap());

    reopen_region(&engine, region_id, table_dir, true, wal_options()).await;
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(provider, region.provider);
    assert_eq!(1, region.version_control.current().last_entry_id);
    assert_eq!(3, count_rows(&engine, region_id).await);
    assert_eq!(vec![provider], log_store.read_providers());
}

#[tokio::test]
async fn test_object_store_region_entry_ids_follow_shared_namespace() {
    let mut env = TestEnv::with_prefix("object-store-wal-entry-ids").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(1, 2);
    let provider_a = object_store_provider(region_a);

    // Region B advances the shared namespace before region A exists.
    let (_, schema_b) = create_region(&engine, region_b).await;
    let rows_b = Rows {
        schema: schema_b,
        rows: build_rows(0, 2),
    };
    put_rows(&engine, region_b, rows_b.clone()).await;
    assert_eq!(1, log_store.latest_entry_id(&provider_a).unwrap());

    // A fresh region starts from the latest entry id of the namespace.
    let (table_dir_a, schema_a) = create_region(&engine, region_a).await;
    let region = engine.get_region(region_a).unwrap();
    assert_eq!(1, region.version_control.current().version.flushed_entry_id);
    assert_eq!(1, region.topic_latest_entry_id.load(Ordering::Relaxed));

    put_rows(
        &engine,
        region_a,
        Rows {
            schema: schema_a,
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_a, None).await;
    assert_eq!(2, region.version_control.current().version.flushed_entry_id);
    // Region B advances the namespace again after region A flushed.
    put_rows(&engine, region_b, rows_b).await;
    assert_eq!(3, log_store.latest_entry_id(&provider_a).unwrap());

    // Nothing of region A is replayed, so the topic latest entry id comes from the store.
    reopen_region(&engine, region_a, table_dir_a, true, wal_options()).await;
    let region = engine.get_region(region_a).unwrap();
    assert_eq!(2, region.version_control.current().version.flushed_entry_id);
    assert_eq!(3, region.topic_latest_entry_id.load(Ordering::Relaxed));
    assert_eq!(3, count_rows(&engine, region_a).await);
}

#[tokio::test]
async fn test_open_region_replays_only_its_own_entries() {
    let mut env = TestEnv::with_prefix("object-store-wal-isolation").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(1, 2);

    let table_dir_a = create_region_with_rows(&engine, region_a, 0, 3).await;
    let _ = create_region_with_rows(&engine, region_b, 0, 5).await;
    close_region(&engine, region_a).await;
    close_region(&engine, region_b).await;
    // The store hands out the entries of both regions under the same prefix.
    assert_eq!(vec![region_a, region_b], log_store.region_ids());

    engine
        .handle_request(region_a, RegionRequest::Open(open_request(table_dir_a)))
        .await
        .unwrap();
    assert_eq!(3, count_rows(&engine, region_a).await);
    assert!(!engine.is_region_exists(region_b));
    assert_eq!(
        vec![object_store_provider(region_a)],
        log_store.read_providers()
    );
    // Replay obsoletes region A's entries up to its flushed entry id, which is still 0.
    assert_eq!(
        vec![(object_store_provider(region_a), region_a, 0)],
        log_store.obsoleted()
    );
    assert_eq!(vec![region_a, region_b], log_store.region_ids());
}

#[tokio::test]
async fn test_batch_open_regions_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-batch-open").await;
    let (engine, _) = new_engine(&mut env).await;

    let mut regions = Vec::new();
    for i in 1..=3 {
        let region_id = RegionId::new(1, i);
        let num_rows = i as usize + 1;
        let table_dir = create_region_with_rows(&engine, region_id, 0, num_rows).await;
        close_region(&engine, region_id).await;
        regions.push((region_id, table_dir, num_rows));
    }

    let requests = regions
        .iter()
        .map(|(region_id, table_dir, _)| (*region_id, open_request(table_dir.clone())))
        .collect();
    let responses = engine
        .handle_batch_open_requests(4, requests)
        .await
        .unwrap();
    assert_eq!(regions.len(), responses.len());
    for (_, response) in responses {
        response.unwrap();
    }

    for (region_id, _, num_rows) in regions {
        assert_eq!(
            object_store_provider(region_id),
            engine.get_region(region_id).unwrap().provider
        );
        assert_eq!(num_rows, count_rows(&engine, region_id).await);
    }
}

#[tokio::test]
async fn test_drop_and_offline_cleanup_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-cleanup").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(1, 2);

    let _ = create_region_with_rows(&engine, region_a, 0, 3).await;
    let table_dir_b = create_region_with_rows(&engine, region_b, 0, 3).await;

    engine
        .handle_request(
            region_a,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        vec![(object_store_provider(region_a), region_a, 1)],
        log_store.obsoleted()
    );
    assert_eq!(vec![region_b], log_store.region_ids());

    close_region(&engine, region_b).await;
    engine
        .handle_request(
            region_b,
            RegionRequest::CleanUp(RegionCleanUpRequest {
                engine: String::new(),
                table_dir: table_dir_b,
                path_type: PathType::Bare,
                options: wal_options(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        vec![(object_store_provider(region_b), region_b)],
        log_store.obsoleted_all()
    );
    assert!(log_store.region_ids().is_empty());
}

#[tokio::test]
async fn test_object_store_wal_options_reject_raft_engine_log_store() {
    let mut env = TestEnv::with_prefix("object-store-wal-raft-engine").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let err = engine
        .handle_request(RegionId::new(1, 1), RegionRequest::Create(request))
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
}

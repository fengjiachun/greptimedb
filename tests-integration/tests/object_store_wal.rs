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

use std::path::Path;
use std::time::{Duration, Instant};

use common_procedure::options::ProcedureConfig;
use common_query::Output;
use common_telemetry::info;
use common_wal::config::DatanodeWalConfig;
use common_wal::config::object_store::ObjectStoreWalConfig;
use frontend::instance::Instance;
use object_store::ObjectStore;
use object_store::config::ObjectStoreConfig;
use object_store::services::S3;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
use tests_integration::test_util::StorageType;

async fn execute_sql(instance: &Instance, sql: &str) -> Output {
    SqlQueryHandler::do_query(instance, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap()
}

/// Returns whether any file exists below `dir`, recursively.
fn has_files(dir: &Path) -> bool {
    std::fs::read_dir(dir).ok().is_some_and(|entries| {
        entries.flatten().any(|entry| {
            let path = entry.path();
            path.is_file() || has_files(&path)
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_object_store_wal_round_trip() {
    common_telemetry::init_default_ut_logging();

    let wal_config = ObjectStoreWalConfig {
        prefix: "cluster-a/wal".to_string(),
        ..Default::default()
    };
    let standalone = GreptimeDbStandaloneBuilder::new("object_store_wal")
        .with_datanode_wal_config(DatanodeWalConfig::ObjectStore(wal_config))
        .build()
        .await;
    let frontend = standalone.fe_instance();

    execute_sql(
        frontend,
        r#"
        CREATE TABLE cpu (
            hostname STRING PRIMARY KEY,
            usage_user DOUBLE,
            ts TIMESTAMP TIME INDEX
        )
        "#,
    )
    .await;
    execute_sql(
        frontend,
        r#"
        INSERT INTO cpu VALUES
            ('host_a', 10.0, '2023-06-12T11:00:00Z'),
            ('host_b', 20.0, '2023-06-12T11:00:01Z'),
            ('host_c', 30.0, '2023-06-12T11:00:02Z')
        "#,
    )
    .await;

    let expected = "\
+----------+------------+---------------------+
| hostname | usage_user | ts                  |
+----------+------------+---------------------+
| host_a   | 10.0       | 2023-06-12T11:00:00 |
| host_b   | 20.0       | 2023-06-12T11:00:01 |
| host_c   | 30.0       | 2023-06-12T11:00:02 |
+----------+------------+---------------------+";
    let query = "SELECT hostname, usage_user, ts FROM cpu ORDER BY ts";
    let rows = execute_sql(frontend, query).await.data.pretty_print().await;
    assert_eq!(expected, rows);

    // The acknowledged inserts are durable as WAL objects under the node prefix
    // derived from the configured root inside the default file store, which is
    // rooted at the data home.
    let data_home = Path::new(&standalone.opts.storage.data_home);
    assert!(has_files(
        &data_home.join("cluster-a/wal/datanodes/0/epochs/0/objects")
    ));
    // No Raft Engine log store is created as a fallback.
    assert!(!data_home.join("wal").exists());

    execute_sql(frontend, "ADMIN FLUSH_TABLE('cpu')").await;
    let rows = execute_sql(frontend, query).await.data.pretty_print().await;
    assert_eq!(expected, rows);
}

/// WAL objects under the configured root prefix of the default S3 store,
/// counted recursively so the count does not depend on the layout below it.
struct WalObjects {
    store: ObjectStore,
    path: String,
}

impl WalObjects {
    fn new(config: &ObjectStoreConfig, prefix: &str) -> Self {
        let ObjectStoreConfig::S3(s3) = config else {
            panic!("expected the S3 store, actual {config:?}");
        };
        let store = ObjectStore::new(S3::from(&s3.connection)).unwrap().finish();
        Self {
            store,
            path: format!("{prefix}/"),
        }
    }

    /// Returns the object count and their total size in bytes. Every key is
    /// logged when `log_keys` is set, so the driver script can show the
    /// layout the store derived below the root prefix.
    async fn count_and_bytes(&self, log_keys: bool) -> (usize, u64) {
        let entries = self
            .store
            .list_with(&self.path)
            .recursive(true)
            .await
            .unwrap();
        let mut count = 0;
        let mut bytes = 0;
        for entry in entries {
            if entry.metadata().is_dir() {
                continue;
            }
            let len = self
                .store
                .stat(entry.path())
                .await
                .unwrap()
                .content_length();
            count += 1;
            bytes += len;
            if log_keys {
                info!("object_store_wal object={} bytes={len}", entry.path());
            }
        }
        (count, bytes)
    }

    /// Logs the objects of a phase for the driver script to collect.
    async fn record(&self, phase: &str) -> (usize, u64) {
        let (count, bytes) = self.count_and_bytes(true).await;
        info!("object_store_wal phase={phase} objects={count} bytes={bytes}");
        (count, bytes)
    }

    /// Waits until at most `expected` objects remain: the store deletes the
    /// objects a flush releases in the background, after the flush returned.
    async fn wait_for_collection(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.count_and_bytes(false).await.0 > expected {
            assert!(
                Instant::now() < deadline,
                "the WAL objects were not collected down to {expected}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Builds the instance again on the metadata, data home and object store of
/// the dropped one, like a process restart, and returns how long it took.
async fn restart(
    builder: &GreptimeDbStandaloneBuilder,
    standalone: GreptimeDbStandalone,
) -> (GreptimeDbStandalone, Duration) {
    let GreptimeDbStandalone {
        frontend,
        opts,
        guard,
        kv_backend,
        procedure_manager,
        event_recorder_handle,
    } = standalone;
    drop(frontend);
    drop(procedure_manager);
    drop(event_recorder_handle);

    let (procedure_manager, event_recorder_handle) =
        standalone::build_procedure_manager(kv_backend.clone(), ProcedureConfig::default());
    let start = Instant::now();
    let standalone = builder
        .build_with(
            kv_backend,
            guard,
            opts,
            procedure_manager,
            event_recorder_handle,
            true,
        )
        .await;
    (standalone, start.elapsed())
}

/// Runs against the S3 bucket of the `GT_S3_*` environment variables, so it
/// is skipped unless they are set.
#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_object_store_wal_survives_restarts_on_s3() {
    if !StorageType::S3.test_on() {
        return;
    }
    common_telemetry::init_default_ut_logging();

    const PREFIX: &str = "cluster-a/wal";
    const BATCHES: usize = 5;
    const ROWS_PER_BATCH: usize = 4;

    let wal_config = ObjectStoreWalConfig {
        prefix: PREFIX.to_string(),
        ..Default::default()
    };
    let builder = GreptimeDbStandaloneBuilder::new("object_store_wal_s3")
        .with_default_store_type(StorageType::S3)
        .with_datanode_wal_config(DatanodeWalConfig::ObjectStore(wal_config));
    let standalone = builder.build().await;
    let wal_objects = WalObjects::new(&standalone.opts.storage.store, PREFIX);
    assert_eq!((0, 0), wal_objects.record("before-writes").await);

    execute_sql(
        standalone.fe_instance(),
        r#"
        CREATE TABLE cpu (
            hostname STRING PRIMARY KEY,
            usage_user DOUBLE,
            ts TIMESTAMP TIME INDEX
        )
        "#,
    )
    .await;
    // Every insert is acknowledged once its entries are durable, so the
    // batches land in separate WAL objects.
    for batch in 0..BATCHES {
        let values = (0..ROWS_PER_BATCH)
            .map(|row| {
                let i = batch * ROWS_PER_BATCH + row;
                format!(
                    "('host_{i}', {i}.0, {})",
                    1_686_567_600_000 + i as i64 * 1000
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        execute_sql(
            standalone.fe_instance(),
            &format!("INSERT INTO cpu VALUES {values}"),
        )
        .await;
    }
    let query = "SELECT hostname, usage_user, ts FROM cpu ORDER BY ts";
    let expected = execute_sql(standalone.fe_instance(), query)
        .await
        .data
        .pretty_print()
        .await;
    assert_eq!(BATCHES * ROWS_PER_BATCH + 4, expected.lines().count());
    let (objects, _) = wal_objects.record("after-writes").await;
    assert!(objects >= 2, "expected several WAL objects, got {objects}");

    // Nothing was flushed, so the rows come back from the WAL alone.
    let (standalone, elapsed) = restart(&builder, standalone).await;
    info!("object_store_wal restart=1 wall_ms={}", elapsed.as_millis());
    let rows = execute_sql(standalone.fe_instance(), query)
        .await
        .data
        .pretty_print()
        .await;
    assert_eq!(expected, rows);
    let (replayed_objects, _) = wal_objects.record("after-restart-1").await;
    assert_eq!(objects, replayed_objects);

    // The flush moves the region's watermark past every object: the store
    // collects all of them but the highest, which anchors the sequence.
    execute_sql(standalone.fe_instance(), "ADMIN FLUSH_TABLE('cpu')").await;
    wal_objects.wait_for_collection(1).await;
    let (remaining, _) = wal_objects.record("after-flush").await;
    assert_eq!(1, remaining);

    // The flushed rows come back from the SST and nothing is replayed twice;
    // the retained object is the highest, so opening the region keeps it.
    let (standalone, elapsed) = restart(&builder, standalone).await;
    info!("object_store_wal restart=2 wall_ms={}", elapsed.as_millis());
    let rows = execute_sql(standalone.fe_instance(), query)
        .await
        .data
        .pretty_print()
        .await;
    assert_eq!(expected, rows);
    let (remaining, _) = wal_objects.record("after-restart-2").await;
    assert_eq!(1, remaining);
}

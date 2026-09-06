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

use common_query::Output;
use common_wal::config::DatanodeWalConfig;
use common_wal::config::object_store::ObjectStoreWalConfig;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;

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

    // The acknowledged inserts are durable as WAL objects under the prefix of
    // the default file store, which is rooted at the data home.
    let data_home = Path::new(&standalone.opts.storage.data_home);
    assert!(has_files(&data_home.join("cluster-a").join("wal")));
    // No Raft Engine log store is created as a fallback.
    assert!(!data_home.join("wal").exists());

    execute_sql(frontend, "ADMIN FLUSH_TABLE('cpu')").await;
    let rows = execute_sql(frontend, query).await.data.pretty_print().await;
    assert_eq!(expected, rows);
}

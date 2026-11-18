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

//! Tests that reproduce behaviour around a failed or interrupted repartition
//! (GreptimeTeam/greptimedb#10066 and related issues).
//!
//! Each test asserts the behaviour it is named after. Lines starting with
//! `OBSERVED` print the scan results and errors the asserts look at, so a
//! reader can see them with `--no-capture`.

use std::collections::HashMap;
use std::time::Duration;

use api::v1::Rows;
use common_function::utils::partition_expr_version;
use common_recordbatch::RecordBatches;
use datatypes::value::Value;
use futures::TryStreamExt;
use partition::expr::col;
use store_api::region_engine::{RegionEngine, RemapManifestsRequest};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, RegionDropRequest, RegionFlushRequest,
    RegionPutRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::RegionId;

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, put_rows, reopen_region, rows_schema,
};

const SOURCE: RegionId = RegionId::new(1, 1);
const TARGET: RegionId = RegionId::new(1, 2);

/// The source holds every key below "z".
fn source_expr() -> String {
    col("tag_0").lt(Value::from("z")).as_json_str().unwrap()
}

/// After the split the source keeps ["m", "z").
fn kept_expr() -> String {
    col("tag_0")
        .gt_eq(Value::from("m"))
        .and(col("tag_0").lt(Value::from("z")))
        .as_json_str()
        .unwrap()
}

/// After the split the new region takes everything below "m".
fn moved_expr() -> String {
    col("tag_0").lt(Value::from("m")).as_json_str().unwrap()
}

fn obs(test: &str, what: &str, value: impl std::fmt::Display) {
    println!("OBSERVED {test} {what}:\n{value}");
}

async fn scan(engine: &MitoEngine, region_id: RegionId) -> String {
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

async fn create(engine: &MitoEngine, region_id: RegionId, expr: Option<String>) -> Rows {
    let request = CreateRequestBuilder::new()
        .partition_expr_json(expr)
        .build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    Rows {
        schema,
        rows: vec![],
    }
}

fn rows(template: &Rows, key: &str, value: usize) -> Rows {
    Rows {
        schema: template.schema.clone(),
        rows: build_rows_for_key(key, 0, 1, value),
    }
}

async fn flush(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
}

/// Puts rows with the given partition version, or with none.
async fn put_versioned(
    engine: &MitoEngine,
    region_id: RegionId,
    rows: Rows,
    version: Option<u64>,
) -> Result<(), String> {
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows,
                hint: None,
                partition_expr_version: version,
            }),
        )
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

fn version_of(expr: &str) -> Option<u64> {
    Some(partition_expr_version(Some(expr)))
}

async fn enter_staging(engine: &MitoEngine, region_id: RegionId, expr: &str) {
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    expr.to_string(),
                ),
            }),
        )
        .await
        .unwrap();
}

async fn remap_split(engine: &MitoEngine) -> HashMap<RegionId, String> {
    engine
        .remap_manifests(RemapManifestsRequest {
            region_id: SOURCE,
            input_regions: vec![SOURCE],
            region_mapping: [(SOURCE, vec![SOURCE, TARGET])].into_iter().collect(),
            new_partition_exprs: [(SOURCE, kept_expr()), (TARGET, moved_expr())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap()
        .manifest_paths
}

async fn apply(
    engine: &MitoEngine,
    region_id: RegionId,
    central: RegionId,
    expr: &str,
    paths: &HashMap<RegionId, String>,
) {
    engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: expr.to_string(),
                central_region_id: central,
                manifest_path: paths[&region_id].clone(),
            }),
        )
        .await
        .unwrap();
}

/// Source with a=1, b=2 (moved range) and x=3 (kept range) in one SST, and
/// an empty new region created the way AllocateRegion does today.
async fn split_fixture(env: &mut TestEnv, config: MitoConfig) -> (MitoEngine, Rows) {
    let engine = env.create_engine(config).await;
    let template = create(&engine, SOURCE, Some(source_expr())).await;
    for (key, value) in [("a", 1), ("b", 2), ("x", 3)] {
        put_rows(&engine, SOURCE, rows(&template, key, value)).await;
    }
    flush(&engine, SOURCE).await;
    create(&engine, TARGET, Some(moved_expr())).await;
    (engine, template)
}

/// The new region acknowledges rows before its import (as it does after the
/// route switch) and the import shadows them (#10065).
#[tokio::test]
async fn rpfault_d1_import_shadows_rows_acknowledged_before_it() {
    let test = "d1";
    let mut env = TestEnv::with_prefix("rpfault-d1").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;

    // After today's switch: the writable new region acknowledges a=99.
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "a", 99),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    // The staging new region acknowledges b=98.
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "b", 98),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();
    let before = scan(&engine, TARGET).await;
    obs(test, "target before import", &before);
    assert!(before.contains("| a     | 99.0") && before.contains("| b     | 98.0"));

    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    let after = scan(&engine, TARGET).await;
    obs(test, "target after import", &after);
    reopen_region(&engine, TARGET, "test".to_string(), true, HashMap::new()).await;
    let reopened = scan(&engine, TARGET).await;
    obs(test, "target after import and reopen", &reopened);
    let shadowed = !(after.contains("| a     | 99.0") && after.contains("| b     | 98.0"))
        || !(reopened.contains("| a     | 99.0") && reopened.contains("| b     | 98.0"));
    obs(test, "acknowledged rows shadowed", shadowed);
    assert!(shadowed);
}

/// When nothing is acknowledged between a region's flush and its import, the
/// rows written after the import are not shadowed, also after flush and reopen.
#[tokio::test]
async fn rpfault_d2_rows_written_after_the_import_are_not_shadowed() {
    let test = "d2";
    let mut env = TestEnv::with_prefix("rpfault-d2").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    // A row the source acknowledged before staging, still in its memtable.
    put_versioned(
        &engine,
        SOURCE,
        rows(&template, "y", 5),
        version_of(&source_expr()),
    )
    .await
    .unwrap();

    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;

    // Old-rule writes are rejected by the staging source.
    let old_rule = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "a", 50),
        version_of(&source_expr()),
    )
    .await;
    obs(
        test,
        "old-version write to staging source",
        format!("{old_rule:?}"),
    );
    assert!(old_rule.is_err());

    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    obs(test, "target after import", scan(&engine, TARGET).await);
    obs(test, "source after import", scan(&engine, SOURCE).await);

    // First writes under the new rule, after the switch.
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "a", 99),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();
    put_versioned(
        &engine,
        SOURCE,
        rows(&template, "x", 77),
        version_of(&kept_expr()),
    )
    .await
    .unwrap();
    let target = scan(&engine, TARGET).await;
    let source = scan(&engine, SOURCE).await;
    obs(test, "target after new-rule write", &target);
    obs(test, "source after new-rule write", &source);
    assert!(target.contains("| a     | 99.0") && target.contains("| b     | 2.0"));
    assert!(source.contains("| x     | 77.0") && source.contains("| y     | 5.0"));
    assert!(!source.contains("| a ") && !target.contains("| x "));

    flush(&engine, TARGET).await;
    flush(&engine, SOURCE).await;
    reopen_region(&engine, TARGET, "test".to_string(), true, HashMap::new()).await;
    reopen_region(&engine, SOURCE, "test".to_string(), true, HashMap::new()).await;
    let target = scan(&engine, TARGET).await;
    let source = scan(&engine, SOURCE).await;
    obs(test, "target after flush and reopen", &target);
    obs(test, "source after flush and reopen", &source);
    assert!(target.contains("| a     | 99.0") && target.contains("| b     | 2.0"));
    assert!(source.contains("| x     | 77.0") && source.contains("| y     | 5.0"));
}

/// A staging region accepts a write with no partition version. A row of the
/// moved range then stays in the source (visible after flush) and the new
/// region keeps the old value.
#[tokio::test]
async fn rpfault_d3_versionless_write_to_staging_region() {
    let test = "d3";
    let mut env = TestEnv::with_prefix("rpfault-d3").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;

    // A row of the moved range and a row of the kept range, both with no version.
    let moved = put_versioned(&engine, SOURCE, rows(&template, "a", 50), None).await;
    let kept = put_versioned(&engine, SOURCE, rows(&template, "x", 60), None).await;
    obs(
        test,
        "version-less write of a moved-range row",
        format!("{moved:?}"),
    );
    obs(
        test,
        "version-less write of a kept-range row",
        format!("{kept:?}"),
    );
    assert!(moved.is_ok() && kept.is_ok());

    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    obs(
        test,
        "source after both imports",
        scan(&engine, SOURCE).await,
    );
    obs(
        test,
        "target after both imports",
        scan(&engine, TARGET).await,
    );

    flush(&engine, SOURCE).await;
    reopen_region(&engine, SOURCE, "test".to_string(), true, HashMap::new()).await;
    reopen_region(&engine, TARGET, "test".to_string(), true, HashMap::new()).await;
    obs(
        test,
        "source after flush and reopen",
        scan(&engine, SOURCE).await,
    );
    obs(test, "target after reopen", scan(&engine, TARGET).await);
    assert!(scan(&engine, SOURCE).await.contains("| a     | 50.0"));
    assert!(scan(&engine, TARGET).await.contains("| a     | 1.0"));
}

/// Split: what each region returns before the imports, between them and after
/// them. Between the two imports both regions return the moved rows.
#[tokio::test]
async fn rpfault_c1_split_read_intervals() {
    let test = "c1";
    let mut env = TestEnv::with_prefix("rpfault-c1").await;
    let (engine, _) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let paths = remap_split(&engine).await;

    let source = scan(&engine, SOURCE).await;
    let target = scan(&engine, TARGET).await;
    obs(test, "before imports, source", &source);
    obs(test, "before imports, new region", &target);
    assert!(source.contains("| a ") && source.contains("| x ") && !target.contains("| a "));

    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    let source = scan(&engine, SOURCE).await;
    let target = scan(&engine, TARGET).await;
    obs(test, "new region imported, source", &source);
    obs(test, "new region imported, new region", &target);
    obs(
        test,
        "moved rows returned by both regions",
        source.contains("| a ") && target.contains("| a "),
    );
    assert!(source.contains("| a ") && target.contains("| a "));

    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    let source = scan(&engine, SOURCE).await;
    let target = scan(&engine, TARGET).await;
    obs(test, "both imported, source", &source);
    obs(test, "both imported, new region", &target);
    assert!(!source.contains("| a ") && source.contains("| x "));
    assert!(target.contains("| a ") && target.contains("| b ") && !target.contains("| x "));
}

/// Merge: after its import the reused region returns the dropped source's rows
/// while the dropped source still returns them.
#[tokio::test]
async fn rpfault_c2_merge_read_intervals() {
    let test = "c2";
    let mut env = TestEnv::with_prefix("rpfault-c2").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let reused = SOURCE;
    let dropped = TARGET;
    let template = create(&engine, reused, Some(moved_expr())).await;
    create(&engine, dropped, Some(kept_expr())).await;
    put_rows(&engine, reused, rows(&template, "a", 1)).await;
    put_rows(&engine, dropped, rows(&template, "x", 3)).await;
    flush(&engine, reused).await;
    flush(&engine, dropped).await;

    enter_staging(&engine, reused, &source_expr()).await;
    engine
        .handle_request(
            dropped,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    let rejected = put_versioned(
        &engine,
        dropped,
        rows(&template, "y", 9),
        version_of(&kept_expr()),
    )
    .await;
    obs(
        test,
        "write to the reject-all source",
        format!("{rejected:?}"),
    );
    assert!(rejected.is_err());

    let paths = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: reused,
            input_regions: vec![reused, dropped],
            region_mapping: [(reused, vec![reused]), (dropped, vec![reused])]
                .into_iter()
                .collect(),
            new_partition_exprs: [(reused, source_expr())].into_iter().collect(),
        })
        .await
        .unwrap()
        .manifest_paths;

    obs(test, "before import, reused", scan(&engine, reused).await);
    obs(
        test,
        "before import, dropped source",
        scan(&engine, dropped).await,
    );
    apply(&engine, reused, reused, &source_expr(), &paths).await;
    let reused_rows = scan(&engine, reused).await;
    let dropped_rows = scan(&engine, dropped).await;
    obs(test, "after import, reused", &reused_rows);
    obs(test, "after import, dropped source", &dropped_rows);
    assert!(reused_rows.contains("| a ") && reused_rows.contains("| x "));
    obs(
        test,
        "dropped source's rows returned by both regions",
        dropped_rows.contains("| x "),
    );
    assert!(dropped_rows.contains("| x "));
}

/// Dropping a new region that imported the source's SSTs. With `gc.enable` on
/// the source's SSTs stay and the source reads as before; with it off (local
/// file purger) the SST is deleted and scans of the source fail.
#[tokio::test]
async fn rpfault_a_drop_new_region_after_import_keeps_source_files() {
    for gc in [true, false] {
        let test = if gc { "a-gc-on" } else { "a-gc-off" };
        let mut env = TestEnv::with_prefix("rpfault-a").await;
        let mut config = MitoConfig::default();
        config.gc.enable = gc;
        let (engine, _) = split_fixture(&mut env, config).await;
        enter_staging(&engine, SOURCE, &kept_expr()).await;
        enter_staging(&engine, TARGET, &moved_expr()).await;
        let paths = remap_split(&engine).await;
        apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
        let before = scan(&engine, SOURCE).await;
        let store = env.get_object_store().unwrap();
        let parquet_before = list_parquet(&store).await;
        obs(test, "parquet files before drop", parquet_before.join("\n"));

        engine
            .handle_request(
                TARGET,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: true,
                    partial_drop: true,
                }),
            )
            .await
            .unwrap();
        // Give the background removal a chance to run.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let parquet_after = list_parquet(&store).await;
        obs(test, "parquet files after drop", parquet_after.join("\n"));
        let after = try_scan(&engine, SOURCE).await;
        obs(test, "source after drop", &after);
        obs(test, "source rows unchanged", before == after);
        obs(
            test,
            "parquet files unchanged",
            parquet_before == parquet_after,
        );
        // Reads that still work may come from open handles; reopen to be sure.
        engine
            .set_region_role(SOURCE, store_api::region_engine::RegionRole::Leader)
            .unwrap();
        reopen_region(&engine, SOURCE, "test".to_string(), true, HashMap::new()).await;
        let reopened = try_scan(&engine, SOURCE).await;
        obs(test, "source after drop and reopen", &reopened);
        obs(
            test,
            "source rows unchanged after reopen",
            before == reopened,
        );
        if gc {
            assert!(before == after && before == reopened && parquet_before == parquet_after);
        } else {
            assert!(parquet_after.len() < parquet_before.len());
            assert!(after.starts_with("scan failed") && reopened.starts_with("scan failed"));
        }
    }
}

/// Like [scan], but returns the error text instead of panicking.
async fn try_scan(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = match engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
    {
        Ok(stream) => stream,
        Err(e) => return format!("scan failed: {e:?}"),
    };
    match RecordBatches::try_collect(stream).await {
        Ok(batches) => batches.pretty_print().unwrap(),
        Err(e) => format!("scan failed: {e:?}"),
    }
}

async fn list_parquet(store: &object_store::ObjectStore) -> Vec<String> {
    let mut lister = store.lister_with("/").recursive(true).await.unwrap();
    let mut paths = Vec::new();
    while let Some(entry) = lister.try_next().await.unwrap() {
        if entry.path().ends_with(".parquet") {
            paths.push(entry.path().to_string());
        }
    }
    paths.sort();
    paths
}

fn state(engine: &MitoEngine, region_id: RegionId) -> String {
    let region = engine.get_region(region_id).unwrap();
    format!(
        "staging={} writable={} committed_expr={:?}",
        region.is_staging(),
        region.is_writable(),
        region.metadata().partition_expr
    )
}

/// A `Leader` grant takes a staging region out of staging without committing;
/// a `StagingLeader` grant leaves a staging region as it is.
#[tokio::test]
async fn rpfault_e1_leader_grant_aborts_staging_without_commit() {
    let test = "e1";
    let mut env = TestEnv::with_prefix("rpfault-e1").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    obs(test, "source in staging", state(&engine, SOURCE));

    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::StagingLeader)
        .unwrap();
    obs(test, "after StagingLeader grant", state(&engine, SOURCE));
    assert!(engine.get_region(SOURCE).unwrap().is_staging());

    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Leader)
        .unwrap();
    obs(test, "after Leader grant", state(&engine, SOURCE));
    let region = engine.get_region(SOURCE).unwrap();
    assert!(!region.is_staging() && region.is_writable());
    assert_eq!(Some(source_expr()), region.metadata().partition_expr);

    let old_rule = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "a", 50),
        version_of(&source_expr()),
    )
    .await;
    obs(
        test,
        "old-version write after the abort",
        format!("{old_rule:?}"),
    );
    assert!(old_rule.is_ok());
    let source = scan(&engine, SOURCE).await;
    obs(test, "source after the abort", &source);
    assert!(source.contains("| a     | 50.0") && source.contains("| x     | 3.0"));
}

/// A staging region that becomes a follower (restart or
/// lease expiry) loses staging, a `StagingLeader` grant does not bring it
/// back, and it refuses to enter staging.
#[tokio::test]
async fn rpfault_e2_follower_loses_staging_and_cannot_reenter() {
    let test = "e2";
    let mut env = TestEnv::with_prefix("rpfault-e2").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;

    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Follower)
        .unwrap();
    obs(test, "after Follower", state(&engine, SOURCE));
    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::StagingLeader)
        .unwrap();
    obs(
        test,
        "after StagingLeader grant on the follower",
        state(&engine, SOURCE),
    );
    let region = engine.get_region(SOURCE).unwrap();
    assert!(!region.is_staging() && !region.is_writable());

    let write = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "x", 9),
        version_of(&kept_expr()),
    )
    .await;
    obs(test, "write to the follower", format!("{write:?}"));
    assert!(write.is_err());
    let reenter = engine
        .handle_request(
            SOURCE,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(kept_expr()),
            }),
        )
        .await;
    obs(
        test,
        "enter staging on the follower",
        format!("{:?}", reenter.as_ref().map_err(|e| format!("{e:?}"))),
    );
    assert!(reenter.is_err());
}

/// A region with no partition expression expects version 0, and
/// stops accepting it once an import gave it an expression. A write with no
/// version is accepted in both cases.
#[tokio::test]
async fn rpfault_e3_version_zero_on_an_unpartitioned_region() {
    let test = "e3";
    let mut env = TestEnv::with_prefix("rpfault-e3").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let template = create(&engine, SOURCE, None).await;
    put_rows(&engine, SOURCE, rows(&template, "a", 1)).await;
    flush(&engine, SOURCE).await;
    create(&engine, TARGET, Some(moved_expr())).await;

    let zero = put_versioned(&engine, SOURCE, rows(&template, "x", 3), Some(0)).await;
    let other = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "x", 4),
        version_of(&kept_expr()),
    )
    .await;
    obs(
        test,
        "version 0 before the repartition",
        format!("{zero:?}"),
    );
    obs(
        test,
        "another version before the repartition",
        format!("{other:?}"),
    );
    assert!(zero.is_ok() && other.is_err());

    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let zero = put_versioned(&engine, SOURCE, rows(&template, "b", 5), Some(0)).await;
    obs(test, "version 0 while staging", format!("{zero:?}"));
    assert!(zero.is_err());

    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    let zero = put_versioned(&engine, SOURCE, rows(&template, "b", 6), Some(0)).await;
    let none = put_versioned(&engine, SOURCE, rows(&template, "b", 7), None).await;
    obs(test, "version 0 after the import", format!("{zero:?}"));
    obs(test, "no version after the import", format!("{none:?}"));
    assert!(none.is_ok());
    assert!(zero.is_err());
    obs(test, "source after the import", scan(&engine, SOURCE).await);
    obs(test, "target after the import", scan(&engine, TARGET).await);
}

/// A merge source that keeps its `StagingLeader` grant stays reject-all, also
/// for writes with no version; a `Leader` grant makes it accept old-rule writes again.
#[tokio::test]
async fn rpfault_e4_reject_all_source_under_grants() {
    let test = "e4";
    let mut env = TestEnv::with_prefix("rpfault-e4").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let template = create(&engine, SOURCE, Some(kept_expr())).await;
    put_rows(&engine, SOURCE, rows(&template, "x", 3)).await;
    flush(&engine, SOURCE).await;
    engine
        .handle_request(
            SOURCE,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::StagingLeader)
        .unwrap();
    let kept = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "y", 9),
        version_of(&kept_expr()),
    )
    .await;
    let none = put_versioned(&engine, SOURCE, rows(&template, "y", 9), None).await;
    obs(
        test,
        "old-version write under StagingLeader",
        format!("{kept:?}"),
    );
    obs(
        test,
        "version-less write under StagingLeader",
        format!("{none:?}"),
    );
    assert!(none.is_err());
    assert!(kept.is_err());

    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Leader)
        .unwrap();
    let released = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "y", 9),
        version_of(&kept_expr()),
    )
    .await;
    obs(
        test,
        "old-version write after a Leader grant",
        format!("{released:?}"),
    );
    assert!(released.is_ok());
}

/// A `Leader` grant issued before the region entered
/// staging takes it out of staging, and a reject-all source accepts writes
/// again. Only a downgrade is compared with the grant's time.
#[tokio::test]
async fn rpfault_g1_leader_grant_issued_before_staging_exits_staging() {
    let test = "g1";
    let mut env = TestEnv::with_prefix("rpfault-g1").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    let issued_before_staging = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    obs(test, "source in staging", state(&engine, SOURCE));

    let region = engine.get_region(SOURCE).unwrap();
    let promoted = region
        .manifest_ctx
        .promote_unless_downgrade_started_after(issued_before_staging, SOURCE);
    obs(test, "grant issued before staging was applied", promoted);
    obs(test, "source after that grant", state(&engine, SOURCE));
    obs(test, "source left staging", !region.is_staging());
    let old_rule = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "a", 50),
        version_of(&source_expr()),
    )
    .await;
    obs(
        test,
        "old-version write after that grant",
        format!("{old_rule:?}"),
    );
    assert!(promoted && !region.is_staging() && old_rule.is_ok());

    // The same for a reject-all merge source.
    engine
        .handle_request(
            TARGET,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    let before = put_versioned(
        &engine,
        TARGET,
        rows(&template, "b", 7),
        version_of(&moved_expr()),
    )
    .await;
    let target = engine.get_region(TARGET).unwrap();
    target
        .manifest_ctx
        .promote_unless_downgrade_started_after(issued_before_staging, TARGET);
    let after = put_versioned(
        &engine,
        TARGET,
        rows(&template, "b", 7),
        version_of(&moved_expr()),
    )
    .await;
    obs(
        test,
        "write to reject-all region before the stale grant",
        format!("{before:?}"),
    );
    obs(
        test,
        "write to reject-all region after the stale grant",
        format!("{after:?}"),
    );
    assert!(before.is_err() && after.is_ok());
}

/// After a merge import, dropping the merge source the way DeallocateRegion
/// does. With `gc.enable` on the reused region keeps reading the merge source's
/// rows; with it off the referenced SST is deleted and scans fail.
#[tokio::test]
async fn rpfault_a2_drop_merge_source_after_import() {
    for gc in [true, false] {
        let test = if gc { "a2-gc-on" } else { "a2-gc-off" };
        let mut env = TestEnv::with_prefix("rpfault-a2").await;
        let mut config = MitoConfig::default();
        config.gc.enable = gc;
        let engine = env.create_engine(config).await;
        let reused = SOURCE;
        let dropped = TARGET;
        let template = create(&engine, reused, Some(moved_expr())).await;
        create(&engine, dropped, Some(kept_expr())).await;
        put_rows(&engine, reused, rows(&template, "a", 1)).await;
        put_rows(&engine, dropped, rows(&template, "x", 3)).await;
        flush(&engine, reused).await;
        flush(&engine, dropped).await;
        enter_staging(&engine, reused, &source_expr()).await;
        engine
            .handle_request(
                dropped,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
            .unwrap();
        let paths = engine
            .remap_manifests(RemapManifestsRequest {
                region_id: reused,
                input_regions: vec![reused, dropped],
                region_mapping: [(reused, vec![reused]), (dropped, vec![reused])]
                    .into_iter()
                    .collect(),
                new_partition_exprs: [(reused, source_expr())].into_iter().collect(),
            })
            .await
            .unwrap()
            .manifest_paths;
        apply(&engine, reused, reused, &source_expr(), &paths).await;
        let before = scan(&engine, reused).await;
        let store = env.get_object_store().unwrap();
        let parquet_before = list_parquet(&store).await;

        engine
            .handle_request(
                dropped,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: true,
                    partial_drop: true,
                }),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let parquet_after = list_parquet(&store).await;
        obs(test, "parquet files before drop", parquet_before.join("\n"));
        obs(test, "parquet files after drop", parquet_after.join("\n"));
        obs(
            test,
            "parquet files unchanged",
            parquet_before == parquet_after,
        );
        let after = try_scan(&engine, reused).await;
        obs(test, "reused region after the drop", &after);
        reopen_region(&engine, reused, "test".to_string(), true, HashMap::new()).await;
        let reopened = try_scan(&engine, reused).await;
        obs(test, "reused region after the drop and reopen", &reopened);
        obs(
            test,
            "rows unchanged",
            before == after && before == reopened,
        );
        if gc {
            assert!(before == after && before == reopened && parquet_before == parquet_after);
        } else {
            assert!(parquet_after.len() < parquet_before.len());
            assert!(after.starts_with("scan failed") && reopened.starts_with("scan failed"));
        }
    }
}

/// A reject-all merge source that became a follower cannot be dropped.
#[tokio::test]
async fn rpfault_e5_follower_merge_source_drop() {
    let test = "e5";
    let mut env = TestEnv::with_prefix("rpfault-e5").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    create(&engine, SOURCE, Some(kept_expr())).await;
    engine
        .handle_request(
            SOURCE,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Follower)
        .unwrap();
    let dropped = engine
        .handle_request(
            SOURCE,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: true,
                partial_drop: true,
            }),
        )
        .await;
    obs(
        test,
        "drop of a follower merge source",
        format!("{:?}", dropped.as_ref().map_err(|e| format!("{e:?}"))),
    );
    assert!(dropped.is_err());
}

/// On an append-mode table a row that the new region
/// acknowledged before staging and that is also in the source's SST is
/// returned twice by the new region after the import.
#[tokio::test]
async fn rpfault_b4_append_mode_duplicate_after_double_write() {
    let test = "b4";
    let mut env = TestEnv::with_prefix("rpfault-b4").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(source_expr()))
        .insert_option("append_mode", "true")
        .build();
    let template = Rows {
        schema: rows_schema(&request),
        rows: vec![],
    };
    engine
        .handle_request(SOURCE, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            TARGET,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .partition_expr_json(Some(moved_expr()))
                    .insert_option("append_mode", "true")
                    .build(),
            ),
        )
        .await
        .unwrap();
    // The same batch row lands in both regions between allocation and the switch.
    put_rows(&engine, SOURCE, rows(&template, "a", 1)).await;
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "a", 1),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    let target = scan(&engine, TARGET).await;
    obs(test, "new region after the plan", &target);
    obs(
        test,
        "copies of the row in the new region",
        target.matches("| a ").count(),
    );
    assert_eq!(2, target.matches("| a ").count());
    obs(test, "source after the plan", scan(&engine, SOURCE).await);
}

/// Entering staging while a compaction of the region is frozen after its
/// merge (by a test listener, at a point with no cancellation check) returns
/// only once the compaction is released; writes sent meanwhile are accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpfault_r22_enter_staging_while_compaction_is_held() {
    let test = "r22";
    let mut env = TestEnv::with_prefix("rpfault-r22").await;
    let listener = std::sync::Arc::new(crate::engine::listener::CompactionListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            SOURCE.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(source_expr()))
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let template = Rows {
        schema: rows_schema(&request),
        rows: vec![],
    };
    engine
        .handle_request(SOURCE, RegionRequest::Create(request))
        .await
        .unwrap();
    for (key, value) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
        put_rows(&engine, SOURCE, rows(&template, key, value)).await;
        flush(&engine, SOURCE).await;
    }
    let held = tokio::time::timeout(Duration::from_secs(20), listener.wait_handle_finished())
        .await
        .is_ok();
    obs(test, "a compaction is held before its commit", held);
    assert!(held);

    let timer = std::time::Instant::now();
    // A writer that keeps sending old-rule rows while enter staging waits.
    let writer_engine = engine.clone();
    let writer_template = template.clone();
    let stop_writer = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = stop_writer.clone();
    let writer = tokio::spawn(async move {
        let mut log = Vec::new();
        let mut value = 100;
        while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
            let sent = timer.elapsed().as_millis();
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                put_versioned(
                    &writer_engine,
                    SOURCE,
                    rows(&writer_template, "w", value),
                    version_of(&source_expr()),
                ),
            )
            .await;
            let outcome = match result {
                Ok(Ok(())) => "accepted".to_string(),
                Ok(Err(e)) => format!("rejected: {}", e.chars().take(60).collect::<String>()),
                Err(_) => "no reply in 30s".to_string(),
            };
            log.push(format!(
                "sent@{sent}ms took {}ms {outcome}",
                timer.elapsed().as_millis() - sent
            ));
            value += 1;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        log
    });
    let staging_engine = engine.clone();
    let mut request = tokio::spawn(async move {
        staging_engine
            .handle_request(
                SOURCE,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::UpdatePartitionExpr(kept_expr()),
                }),
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    });
    match tokio::time::timeout(Duration::from_secs(5), &mut request).await {
        Ok(result) => panic!(
            "enter staging returned while the compaction was held: {:?} after {:?}",
            result.unwrap(),
            timer.elapsed()
        ),
        Err(_) => {
            obs(
                test,
                "enter staging did not return within",
                "5s while the compaction was held",
            );
            listener.wake();
            let result = tokio::time::timeout(Duration::from_secs(20), &mut request).await;
            obs(
                test,
                "enter staging after the compaction was released",
                format!(
                    "{:?} after {:?} in total",
                    result.map(|r| r.unwrap()),
                    timer.elapsed()
                ),
            );
        }
    }
    listener.wake();
    stop_writer.store(true, std::sync::atomic::Ordering::Relaxed);
    let writes = writer.await.unwrap();
    obs(
        test,
        "writes sent while enter staging waited",
        writes.join("\n"),
    );
    // Every write sent in the first 4 s, while the compaction was held, was accepted.
    assert!(
        writes
            .iter()
            .take(15)
            .all(|line| line.ends_with("accepted"))
    );
    obs(test, "source", state(&engine, SOURCE));
}

/// A reused region that acknowledged rows while staging and then lost staging
/// through a `Follower` and then a `Leader` role change can go through enter
/// staging, remap and import again without losing those rows.
#[tokio::test]
async fn rpfault_h1_second_pass_after_reused_region_lost_staging() {
    let test = "h1";
    let mut env = TestEnv::with_prefix("rpfault-h1").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    // After the route switch frontends have the new routes; both regions acknowledge.
    put_versioned(
        &engine,
        SOURCE,
        rows(&template, "x", 77),
        version_of(&kept_expr()),
    )
    .await
    .unwrap();
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "c", 9),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();

    // The first attempt fails here; the source loses staging.
    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Follower)
        .unwrap();
    engine
        .set_region_role(SOURCE, store_api::region_engine::RegionRole::Leader)
        .unwrap();
    obs(test, "source after losing staging", state(&engine, SOURCE));
    obs(
        test,
        "source rows after losing staging",
        scan(&engine, SOURCE).await,
    );
    let new_rule = put_versioned(
        &engine,
        SOURCE,
        rows(&template, "x", 78),
        version_of(&kept_expr()),
    )
    .await;
    obs(
        test,
        "new-rule write to the source that lost staging",
        format!("{new_rule:?}"),
    );

    // Second attempt: the whole sequence again.
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    apply(&engine, SOURCE, SOURCE, &kept_expr(), &paths).await;
    let source = scan(&engine, SOURCE).await;
    let target = scan(&engine, TARGET).await;
    obs(test, "source after the second attempt", &source);
    obs(test, "target after the second attempt", &target);
    flush(&engine, SOURCE).await;
    flush(&engine, TARGET).await;
    reopen_region(&engine, SOURCE, "test".to_string(), true, HashMap::new()).await;
    reopen_region(&engine, TARGET, "test".to_string(), true, HashMap::new()).await;
    let source_reopened = scan(&engine, SOURCE).await;
    let target_reopened = scan(&engine, TARGET).await;
    obs(test, "source after flush and reopen", &source_reopened);
    obs(test, "target after flush and reopen", &target_reopened);
    assert!(source.contains("| x     | 77.0") && source_reopened.contains("| x     | 77.0"));
    assert!(target.contains("| c     | 9.0") && target_reopened.contains("| c     | 9.0"));
    assert!(target_reopened.contains("| a     | 1.0") && target_reopened.contains("| b     | 2.0"));
}

/// Importing a second time into a region that already imported: the imported
/// file gets a new sequence and shadows a row the region acknowledged after
/// the first import, also after reopen.
#[tokio::test]
async fn rpfault_h2_second_import_into_an_imported_region() {
    let test = "h2";
    let mut env = TestEnv::with_prefix("rpfault-h2").await;
    let (engine, template) = split_fixture(&mut env, MitoConfig::default()).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    // The new region imported; the source did not (first attempt failed there).
    put_versioned(
        &engine,
        TARGET,
        rows(&template, "a", 99),
        version_of(&moved_expr()),
    )
    .await
    .unwrap();
    obs(
        test,
        "target after its first import and a new write",
        scan(&engine, TARGET).await,
    );
    let sequences = |engine: &MitoEngine| {
        let engine = engine.clone();
        async move {
            let region = engine.get_region(TARGET).unwrap();
            let manifest = region.manifest_ctx.manifest().await;
            let mut files = manifest
                .files
                .values()
                .map(|file| format!("{}:{:?}", file.region_id, file.sequence))
                .collect::<Vec<_>>();
            files.sort();
            format!(
                "files={files:?} last_sequence={}",
                region.version_control.current().last_sequence
            )
        }
    };
    obs(
        test,
        "target manifest before the second import",
        sequences(&engine).await,
    );

    // Second attempt: enter staging again, remap again, import again.
    enter_staging(&engine, TARGET, &moved_expr()).await;
    let paths = remap_split(&engine).await;
    apply(&engine, TARGET, SOURCE, &moved_expr(), &paths).await;
    obs(
        test,
        "target manifest after the second import",
        sequences(&engine).await,
    );
    let after = scan(&engine, TARGET).await;
    obs(test, "target after the second import", &after);
    reopen_region(&engine, TARGET, "test".to_string(), true, HashMap::new()).await;
    let reopened = scan(&engine, TARGET).await;
    obs(test, "target after the second import and reopen", &reopened);
    obs(
        test,
        "row acknowledged after the first import survived",
        after.contains("| a     | 99.0") && reopened.contains("| a     | 99.0"),
    );
    assert!(after.contains("| a     | 1.0") && reopened.contains("| a     | 1.0"));
}

/// A region left in staging does not flush and, with a small write buffer,
/// rejects writes once the buffer is full.
#[tokio::test]
async fn rpfault_h3_staging_region_rejects_writes_at_the_memory_limit() {
    let test = "h3";
    let mut env = TestEnv::with_prefix("rpfault-h3").await;
    let config = MitoConfig {
        global_write_buffer_size: common_base::readable_size::ReadableSize::mb(8),
        global_write_buffer_reject_size: common_base::readable_size::ReadableSize::mb(16),
        ..Default::default()
    };
    let (engine, template) = split_fixture(&mut env, config).await;
    enter_staging(&engine, SOURCE, &kept_expr()).await;
    let version = version_of(&kept_expr());
    let mut first_rejection = None;
    for round in 0..400 {
        let batch = Rows {
            schema: template.schema.clone(),
            rows: crate::test_util::build_rows(
                1_000_000 + round * 5_000,
                1_000_000 + (round + 1) * 5_000,
            ),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            put_versioned(&engine, SOURCE, batch, version),
        )
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_rejection = Some((round, e));
                break;
            }
            Err(_) => {
                first_rejection = Some((round, "no reply within 10s".to_string()));
                break;
            }
        }
    }
    let region = engine.get_region(SOURCE).unwrap();
    obs(
        test,
        "memtable bytes when the loop ended",
        region.version().memtables.mutable_usage(),
    );
    obs(test, "region still staging", region.is_staging());
    assert!(region.is_staging() && first_rejection.is_some());
    obs(
        test,
        "first rejected batch (5,000 rows each)",
        format!("{first_rejection:?}")
            .chars()
            .take(400)
            .collect::<String>(),
    );
}

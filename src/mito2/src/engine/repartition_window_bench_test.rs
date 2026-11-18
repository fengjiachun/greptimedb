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

//! Timing of the Mito steps a repartition group runs. Ignored by default.
//!
//! Times the Mito steps of a split (one source into itself and one new
//! region): flush, enter staging, remap, import into the new region, import
//! into the reused region. Knobs are environment variables:
//!
//! - `RPBENCH_FILES`: SST entries in the source manifest (default 1000). The
//!   entries are fabricated; no SST exists behind them.
//! - `RPBENCH_REAL_FILES`: `1` writes and flushes that many small SSTs instead.
//! - `RPBENCH_LATENCY_MS`: delay added to every object-store read and to every
//!   write close (default 0), once the source is populated.
//! - `RPBENCH_MEMTABLE_ROWS`: rows in the source memtable when staging starts
//!   (default 10000).
//! - `RPBENCH_PREFLUSH`: `1` flushes the source before it enters staging.
//! - `RPBENCH_RUNS`: repetitions (default 3).
//! - `RPBENCH_WRITER`: `1` keeps a task writing old-version rows to the source
//!   from before the pre-flush until after the imports.
//! - `RPBENCH_MERGE`: `1` measures a merge of two sources instead of a split.
//! - `RPBENCH_LOG`: `1` turns unit-test logging on.
//! - `RPBENCH_CKPT_DISTANCE`: manifest checkpoint distance (default 10). With 1,
//!   every manifest update, the import commits included, starts a checkpoint.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::v1::Rows;
use common_time::Timestamp;
use datatypes::value::Value;
use object_store::layers::mock::{
    self, Buffer, Metadata, MockLayer, MockLayerBuilder, Result as MockResult, Writer,
};
use partition::expr::{PartitionExpr, col};
use store_api::region_engine::{RegionEngine, RemapManifestsRequest};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, RegionFlushRequest, RegionRequest,
    StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId};

use crate::config::MitoConfig;
use crate::manifest::action::RegionEdit;
use crate::sst::file::FileMeta;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[derive(Clone, Default)]
struct StoreStats {
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    lists: Arc<AtomicUsize>,
    deletes: Arc<AtomicUsize>,
    write_paths: Arc<Mutex<Vec<String>>>,
    delay_ms: Arc<AtomicU64>,
}

impl StoreStats {
    async fn delay(&self) {
        let delay_ms = self.delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    /// Returns (reads, writes, lists, deletes) since the last call.
    fn take(&self) -> (usize, usize, usize, usize) {
        (
            self.reads.swap(0, Ordering::Relaxed),
            self.writes.swap(0, Ordering::Relaxed),
            self.lists.swap(0, Ordering::Relaxed),
            self.deletes.swap(0, Ordering::Relaxed),
        )
    }

    fn take_write_paths(&self) -> Vec<String> {
        std::mem::take(&mut *self.write_paths.lock().unwrap())
    }
}

struct DelayReader {
    inner: mock::oio::Reader,
    stats: StoreStats,
}

impl mock::oio::Read for DelayReader {
    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, Box<dyn mock::oio::ReadStreamDyn>)> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.stats.delay().await;
        self.inner.open(range).await
    }

    async fn read(
        &self,
        range: mock::BytesRange,
    ) -> object_store::Result<(mock::RpRead, object_store::Buffer)> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.stats.delay().await;
        self.inner.read(range).await
    }
}

struct DelayWriter {
    inner: Writer,
    stats: StoreStats,
}

impl mock::Write for DelayWriter {
    async fn write(&mut self, bs: Buffer) -> MockResult<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats.delay().await;
        self.inner.close().await
    }

    async fn abort(&mut self) -> MockResult<()> {
        self.inner.abort().await
    }
}

struct CountLister {
    inner: mock::oio::Lister,
    first: bool,
    stats: StoreStats,
}

impl mock::oio::List for CountLister {
    async fn next(&mut self) -> MockResult<Option<mock::oio::Entry>> {
        if self.first {
            self.first = false;
            self.stats.lists.fetch_add(1, Ordering::Relaxed);
            self.stats.delay().await;
        }
        self.inner.next().await
    }
}

struct CountDeleter {
    inner: mock::oio::Deleter,
    stats: StoreStats,
}

impl mock::oio::Delete for CountDeleter {
    async fn delete(&mut self, path: &str, args: mock::OpDelete) -> MockResult<()> {
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> MockResult<()> {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.stats.delay().await;
        self.inner.close().await
    }
}

fn delay_layer(stats: &StoreStats) -> MockLayer {
    let list_stats = stats.clone();
    let delete_stats = stats.clone();
    let read_stats = stats.clone();
    let write_stats = stats.clone();
    MockLayerBuilder::default()
        .reader_factory(Arc::new(move |_, _, inner| {
            Box::new(DelayReader {
                inner,
                stats: read_stats.clone(),
            })
        }))
        .lister_factory(Arc::new(move |_, _, inner| {
            Box::new(CountLister {
                inner,
                first: true,
                stats: list_stats.clone(),
            })
        }))
        .deleter_factory(Arc::new(move |inner| {
            Box::new(CountDeleter {
                inner,
                stats: delete_stats.clone(),
            })
        }))
        .writer_factory(Arc::new(move |path, _, inner| {
            write_stats
                .write_paths
                .lock()
                .unwrap()
                .push(path.to_string());
            Box::new(DelayWriter {
                inner,
                stats: write_stats.clone(),
            })
        }))
        .build()
        .unwrap()
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn range_expr(start: i64, end: i64) -> PartitionExpr {
    col("tag_0")
        .gt_eq(Value::Int64(start))
        .and(col("tag_0").lt(Value::Int64(end)))
}

fn fabricated_edit(region_id: RegionId, start: usize, count: usize) -> RegionEdit {
    RegionEdit {
        files_to_add: (start..start + count)
            .map(|i| FileMeta {
                region_id,
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(i as i64 * 1000),
                    Timestamp::new_millisecond(i as i64 * 1000 + 999),
                ),
                file_size: 64 * 1024 * 1024,
                num_rows: 1_000_000,
                num_row_groups: 10,
                ..Default::default()
            })
            .collect(),
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: Default::default(),
    }
}

async fn enter_staging(engine: &crate::engine::MitoEngine, region_id: RegionId, expr: &str) {
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

async fn run_once(
    run: usize,
    files: usize,
    real_files: bool,
    latency: Duration,
    memtable_rows: usize,
    preflush: bool,
) {
    let stats = StoreStats::default();
    let mut env = TestEnv::with_prefix("rpfault-window")
        .await
        .with_mock_layer(delay_layer(&stats));
    let engine = env
        .create_engine(MitoConfig {
            // Fabricated SSTs must never be picked by compaction.
            min_compaction_interval: Duration::from_secs(24 * 3600),
            manifest_checkpoint_distance: env_or("RPBENCH_CKPT_DISTANCE", 10) as u64,
            ..Default::default()
        })
        .await;

    let source = RegionId::new(1, 1);
    let new_region = RegionId::new(1, 2);
    let source_expr = range_expr(0, 100).as_json_str().unwrap();
    let reused_expr = range_expr(0, 50).as_json_str().unwrap();
    let new_expr = range_expr(50, 100).as_json_str().unwrap();

    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(source_expr))
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "1000000")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            new_region,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .partition_expr_json(Some(new_expr.clone()))
                    .build(),
            ),
        )
        .await
        .unwrap();

    // Rows of the real SSTs sit after the memtable rows on the time axis.
    let real_rows_start = memtable_rows.max(1) * 2;
    let mut added = 0;
    while added < files {
        if real_files {
            let start = real_rows_start + added * 10;
            put_rows(
                &engine,
                source,
                Rows {
                    schema: column_schemas.clone(),
                    rows: build_rows(start, start + 10),
                },
            )
            .await;
            engine
                .handle_request(source, RegionRequest::Flush(RegionFlushRequest::default()))
                .await
                .unwrap();
            added += 1;
        } else {
            let count = (files - added).min(1000);
            engine
                .edit_region(source, fabricated_edit(source, added, count))
                .await
                .unwrap();
            added += count;
        }
    }
    let source_files = engine
        .get_region(source)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await
        .files
        .len();
    stats
        .delay_ms
        .store(latency.as_millis() as u64, Ordering::Relaxed);

    let mut put = 0;
    while put < memtable_rows {
        let count = (memtable_rows - put).min(50_000);
        put_rows(
            &engine,
            source,
            Rows {
                schema: column_schemas.clone(),
                rows: build_rows(put, put + count),
            },
        )
        .await;
        put += count;
    }
    let memtable_bytes = engine
        .get_region(source)
        .unwrap()
        .version()
        .memtables
        .mutable_usage();

    let stop_writer = Arc::new(AtomicBool::new(false));
    let writer_log = Arc::new(Mutex::new(Vec::<(u128, u128, bool)>::new()));
    let writer_started = Instant::now();
    let writer = (env_or("RPBENCH_WRITER", 0) == 1).then(|| {
        let engine = engine.clone();
        let stop = stop_writer.clone();
        let log = writer_log.clone();
        let schema = column_schemas.clone();
        let version = common_function::utils::partition_expr_version(Some(
            &range_expr(0, 100).as_json_str().unwrap(),
        ));
        tokio::spawn(async move {
            let mut next = 50_000_000;
            while !stop.load(Ordering::Relaxed) {
                let sent = writer_started.elapsed().as_millis();
                let timer = Instant::now();
                let result = engine
                    .handle_request(
                        source,
                        RegionRequest::Put(store_api::region_request::RegionPutRequest {
                            skip_wal: false,
                            rows: Rows {
                                schema: schema.clone(),
                                rows: build_rows(next, next + 10),
                            },
                            hint: None,
                            partition_expr_version: Some(version),
                        }),
                    )
                    .await;
                log.lock()
                    .unwrap()
                    .push((sent, timer.elapsed().as_millis(), result.is_ok()));
                next += 10;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    });
    if writer.is_some() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let mut preflush_elapsed = Duration::ZERO;
    if preflush {
        let timer = Instant::now();
        engine
            .handle_request(source, RegionRequest::Flush(RegionFlushRequest::default()))
            .await
            .unwrap();
        preflush_elapsed = timer.elapsed();
    }
    stats.take();
    stats.take_write_paths();

    let enter_source_at = writer_started.elapsed().as_millis();
    let timer = Instant::now();
    enter_staging(&engine, source, &reused_expr).await;
    let enter_source = timer.elapsed();
    let enter_source_ops = stats.take();
    let enter_source_paths = stats.take_write_paths();
    let staged_at = writer_started.elapsed().as_millis();

    let timer = Instant::now();
    enter_staging(&engine, new_region, &new_expr).await;
    let enter_new = timer.elapsed();
    let enter_new_ops = stats.take();

    let timer = Instant::now();
    let remap = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: source,
            input_regions: vec![source],
            region_mapping: [(source, vec![source, new_region])].into_iter().collect(),
            new_partition_exprs: [
                (source, reused_expr.clone()),
                (new_region, new_expr.clone()),
            ]
            .into_iter()
            .collect(),
        })
        .await
        .unwrap();
    let remap_elapsed = timer.elapsed();
    let remap_ops = stats.take();

    let timer = Instant::now();
    engine
        .handle_request(
            new_region,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: new_expr,
                central_region_id: source,
                manifest_path: remap.manifest_paths[&new_region].clone(),
            }),
        )
        .await
        .unwrap();
    let import_new = timer.elapsed();
    let import_new_ops = stats.take();

    let timer = Instant::now();
    engine
        .handle_request(
            source,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: reused_expr,
                central_region_id: source,
                manifest_path: remap.manifest_paths[&source].clone(),
            }),
        )
        .await
        .unwrap();
    let import_reused = timer.elapsed();
    let import_reused_ops = stats.take();

    let window = enter_new + remap_elapsed + import_new + import_reused;
    stop_writer.store(true, Ordering::Relaxed);
    if let Some(writer) = writer {
        writer.await.unwrap();
        let log = writer_log.lock().unwrap();
        let accepted = log.iter().filter(|w| w.2).count();
        let rejected = log.len() - accepted;
        let slowest_accepted = log.iter().filter(|w| w.2).map(|w| w.1).max().unwrap_or(0);
        let slowest_rejected = log.iter().filter(|w| !w.2).map(|w| w.1).max().unwrap_or(0);
        let first_rejected = log
            .iter()
            .find(|w| !w.2)
            .map(|w| w.0 as i128 - enter_source_at as i128);
        let last_accepted = log
            .iter()
            .rev()
            .find(|w| w.2)
            .map(|w| w.0 as i128 - enter_source_at as i128);
        // Writes sent before the region was staging that were answered with a rejection.
        let queued_then_rejected = log
            .iter()
            .filter(|w| !w.2 && w.0 >= enter_source_at && w.0 < staged_at)
            .count();
        println!(
            "BENCH writer run={run} accepted={accepted} rejected={rejected} slowest_accepted_ms={slowest_accepted} slowest_rejected_ms={slowest_rejected} \
             first_rejected_ms_after_enter={first_rejected:?} last_accepted_sent_ms_after_enter={last_accepted:?} \
             sent_during_enter_and_rejected={queued_then_rejected}"
        );
    }
    println!(
        "BENCH writes run={run} writes during enter_source: {:?}",
        enter_source_paths
            .iter()
            .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
            .collect::<Vec<_>>()
    );
    println!(
        "BENCH split run={run} files={files} real_files={real_files} source_files={source_files} memtable_bytes={memtable_bytes} ckpt_distance={} latency_ms={} memtable_rows={memtable_rows} preflush={preflush} \
         preflush_ms={} enter_source_ms={} enter_new_ms={} remap_ms={} import_new_ms={} \
         import_reused_ms={} window_without_enter_source_ms={} \
         ops(reads,writes,lists,deletes): enter_source={enter_source_ops:?} enter_new={enter_new_ops:?} \
         remap={remap_ops:?} import_new={import_new_ops:?} import_reused={import_reused_ops:?}",
        env_or("RPBENCH_CKPT_DISTANCE", 10),
        latency.as_millis(),
        preflush_elapsed.as_millis(),
        enter_source.as_millis(),
        enter_new.as_millis(),
        remap_elapsed.as_millis(),
        import_new.as_millis(),
        import_reused.as_millis(),
        window.as_millis(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing measurement, run explicitly"]
async fn rpbench_repartition_window() {
    if env_or("RPBENCH_LOG", 0) == 1 {
        common_telemetry::init_default_ut_logging();
    }
    let files = env_or("RPBENCH_FILES", 1000);
    let real_files = env_or("RPBENCH_REAL_FILES", 0) == 1;
    let latency = Duration::from_millis(env_or("RPBENCH_LATENCY_MS", 0) as u64);
    let memtable_rows = env_or("RPBENCH_MEMTABLE_ROWS", 10_000);
    let preflush = env_or("RPBENCH_PREFLUSH", 0) == 1;
    for run in 0..env_or("RPBENCH_RUNS", 3) {
        if env_or("RPBENCH_MERGE", 0) == 1 {
            run_merge_once(run, files, latency).await;
        } else {
            run_once(run, files, real_files, latency, memtable_rows, preflush).await;
        }
    }
}

/// A merge of two sources into the first: the second is flushed and rejects
/// all writes, the first enters staging with the merged expression.
async fn run_merge_once(run: usize, files: usize, latency: Duration) {
    let stats = StoreStats::default();
    let mut env = TestEnv::with_prefix("rpfault-merge")
        .await
        .with_mock_layer(delay_layer(&stats));
    let engine = env
        .create_engine(MitoConfig {
            min_compaction_interval: Duration::from_secs(24 * 3600),
            manifest_checkpoint_distance: env_or("RPBENCH_CKPT_DISTANCE", 10) as u64,
            ..Default::default()
        })
        .await;
    let reused = RegionId::new(1, 1);
    let dropped = RegionId::new(1, 2);
    let merged_expr = range_expr(0, 100).as_json_str().unwrap();
    for (region_id, expr) in [(reused, range_expr(0, 50)), (dropped, range_expr(50, 100))] {
        let request = CreateRequestBuilder::new()
            .partition_expr_json(Some(expr.as_json_str().unwrap()))
            .insert_option("compaction.type", "twcs")
            .insert_option("compaction.twcs.trigger_file_num", "1000000")
            .build();
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        let mut added = 0;
        while added < files {
            let count = (files - added).min(1000);
            engine
                .edit_region(region_id, fabricated_edit(region_id, added, count))
                .await
                .unwrap();
            added += count;
        }
    }
    stats
        .delay_ms
        .store(latency.as_millis() as u64, Ordering::Relaxed);
    stats.take();

    let timer = Instant::now();
    enter_staging(&engine, reused, &merged_expr).await;
    engine
        .handle_request(
            dropped,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    let enter = timer.elapsed();
    let enter_ops = stats.take();

    let timer = Instant::now();
    let remap = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: reused,
            input_regions: vec![reused, dropped],
            region_mapping: [(reused, vec![reused]), (dropped, vec![reused])]
                .into_iter()
                .collect(),
            new_partition_exprs: [(reused, merged_expr.clone())].into_iter().collect(),
        })
        .await
        .unwrap();
    let remap_elapsed = timer.elapsed();
    let remap_ops = stats.take();

    let timer = Instant::now();
    engine
        .handle_request(
            reused,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: merged_expr,
                central_region_id: reused,
                manifest_path: remap.manifest_paths[&reused].clone(),
            }),
        )
        .await
        .unwrap();
    let import = timer.elapsed();
    let import_ops = stats.take();
    println!(
        "BENCH merge run={run} files_per_source={files} latency_ms={} enter_both_ms={} remap_ms={} import_ms={} \
         window_ms={} ops(reads,writes,lists,deletes): enter={enter_ops:?} remap={remap_ops:?} import={import_ops:?}",
        latency.as_millis(),
        enter.as_millis(),
        remap_elapsed.as_millis(),
        import.as_millis(),
        (remap_elapsed + import).as_millis(),
    );
}

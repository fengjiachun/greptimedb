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

//! Tests of write routing around a repartition (GreptimeTeam/greptimedb#10066).

use std::sync::Arc;

use common_meta::rpc::router::{Region, RegionRoute, WriteRoutePolicy};
use datatypes::arrow::array::Int64Array;
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use store_api::storage::RegionId;

use crate::PartitionRule;
use crate::cache::create_partitions_with_version_from_region_routes;
use crate::expr::{PartitionExpr, col};
use crate::manager::create_partitions_from_region_routes;
use crate::multi_dim::MultiDimPartitionRule;

fn range(start: i64, end: i64) -> PartitionExpr {
    col("x")
        .gt_eq(Value::Int64(start))
        .and(col("x").lt(Value::Int64(end)))
}

fn route(table_id: u32, number: u32, expr: &PartitionExpr, ignore_writes: bool) -> RegionRoute {
    RegionRoute {
        region: Region {
            id: RegionId::new(table_id, number),
            partition_expr: expr.as_json_str().unwrap(),
            ..Default::default()
        },
        write_route_policy: ignore_writes.then_some(WriteRoutePolicy::IgnoreAllWrites),
        ..Default::default()
    }
}

/// Routes as they are between AllocateRegion and the switch: the source still
/// has its old, wider expression and the new region already has its target one.
fn rule_from_routes(routes: &[RegionRoute]) -> MultiDimPartitionRule {
    let partitions = create_partitions_with_version_from_region_routes(1024, routes).unwrap();
    MultiDimPartitionRule::try_new(
        vec!["x".to_string()],
        partitions
            .iter()
            .map(|partition| partition.id.region_number())
            .collect(),
        partitions
            .iter()
            .map(|partition| partition.partition_expr.clone().unwrap())
            .collect(),
        false,
    )
    .unwrap()
}

fn batch(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

/// A record batch is split to every region whose expression matches, so a
/// row of the moved range goes to the source and to the new region. A row
/// insert goes to the first match only.
#[test]
fn rpfault_b1_batch_write_goes_to_both_overlapping_regions() {
    let routes = vec![
        route(1024, 1, &range(0, 100), false),
        route(1024, 2, &range(50, 100), false),
    ];
    let rule = rule_from_routes(&routes);
    let masks = rule.split_record_batch(&batch(&[10, 70])).unwrap();
    println!(
        "OBSERVED b1 batch rows per region: source={} new={}",
        masks[&1].selected_rows(),
        masks[&2].selected_rows()
    );
    assert_eq!(2, masks[&1].selected_rows());
    assert_eq!(1, masks[&2].selected_rows());

    let region = rule.find_region(&[Value::Int64(70)]).unwrap();
    println!("OBSERVED b1 row insert of x=70 goes to region {region}");
    assert_eq!(1, region);
}

/// With `IgnoreAllWrites` on the new region's route the write rule does
/// not contain it, and the batch goes to the source only.
#[test]
fn rpfault_b2_ignore_all_writes_route_gets_no_batch_rows() {
    let routes = vec![
        route(1024, 1, &range(0, 100), false),
        route(1024, 2, &range(50, 100), true),
    ];
    let rule = rule_from_routes(&routes);
    let masks = rule.split_record_batch(&batch(&[10, 70])).unwrap();
    println!("OBSERVED b2 regions that receive rows: {:?}", masks.keys());
    assert_eq!(1, masks.len());
    assert_eq!(2, masks[&1].selected_rows());
}

/// The partitions used to plan reads contain every route, whatever its
/// write policy.
#[test]
fn rpfault_c3_read_partitions_include_ignore_all_writes_routes() {
    let routes = vec![
        route(1024, 1, &range(0, 100), false),
        route(1024, 2, &range(50, 100), true),
    ];
    let read = create_partitions_from_region_routes(1024, &routes).unwrap();
    let write = create_partitions_with_version_from_region_routes(1024, &routes).unwrap();
    println!(
        "OBSERVED c3 read partitions={} write partitions={}",
        read.len(),
        write.len()
    );
    assert_eq!(2, read.len());
    assert_eq!(1, write.len());
}

/// When the source is the unpartitioned default region,
/// a row in the new region's range goes to the new region only.
#[test]
fn rpfault_b3_unpartitioned_source_rows_go_to_new_region_only() {
    let above = col("x").gt_eq(Value::Int64(50));
    let rule = MultiDimPartitionRule::try_new(
        vec!["x".to_string()],
        vec![1, 0],
        vec![above.clone()],
        false,
    )
    .unwrap();
    let region = rule.find_region(&[Value::Int64(70)]).unwrap();
    let masks = rule.split_record_batch(&batch(&[10, 70])).unwrap();
    println!(
        "OBSERVED b3 row x=70 goes to region {region}; batch rows: default={:?} new={:?}",
        masks.get(&0).map(|mask| mask.selected_rows()),
        masks.get(&1).map(|mask| mask.selected_rows())
    );
    assert_eq!(1, region);
    assert_eq!(Some(1), masks.get(&0).map(|mask| mask.selected_rows()));
    assert_eq!(Some(1), masks.get(&1).map(|mask| mask.selected_rows()));

    // The same routes through the write-rule builder, with and without the flag.
    let unpartitioned = RegionRoute {
        region: Region {
            id: RegionId::new(1024, 0),
            ..Default::default()
        },
        ..Default::default()
    };
    for ignore_writes in [false, true] {
        let routes = vec![unpartitioned.clone(), route(1024, 1, &above, ignore_writes)];
        let partitions = create_partitions_with_version_from_region_routes(1024, &routes).unwrap();
        println!(
            "OBSERVED b3 ignore_writes={ignore_writes} write partitions: {:?}",
            partitions
                .iter()
                .map(|p| (
                    p.id.region_number(),
                    p.partition_expr.is_some(),
                    p.partition_expr_version
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(if ignore_writes { 1 } else { 2 }, partitions.len());
    }
}

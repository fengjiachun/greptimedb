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

use std::any::Any;
use std::collections::{HashMap, HashSet};

use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::debug;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ensure};

use crate::error::{self, Result};
use crate::procedure::repartition::group::sync_region::SyncRegion;
use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
use crate::procedure::repartition::group::{
    Context, GroupId, GroupPrepareResult, State, region_routes,
};
use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};

#[derive(Debug, Serialize, Deserialize)]
pub struct RepartitionStart;

/// Ensures that the partition expression of the source region route matches the source descriptor.
fn ensure_source_region_route_expr_match(
    region_route: &RegionRoute,
    source: &SourceRegionDescriptor,
) -> Result<RegionRoute> {
    let actual = region_route.region.partition_expr();
    let expected = source.route_expr_for_rollback()?;
    ensure!(
        actual == expected,
        error::PartitionExprMismatchSnafu {
            region_id: region_route.region.id,
            expected,
            actual,
        }
    );
    Ok(region_route.clone())
}

impl RepartitionStart {
    /// Ensures that both source and target regions are present in the region routes.
    ///
    /// Both source and target regions must be present in the region routes (target regions should be allocated before repartitioning).
    fn ensure_route_present(
        group_id: GroupId,
        region_routes: &[RegionRoute],
        sources: &[SourceRegionDescriptor],
        targets: &[TargetRegionDescriptor],
    ) -> Result<GroupPrepareResult> {
        ensure!(
            !sources.is_empty(),
            error::UnexpectedSnafu {
                violated: "Sources are empty"
            }
        );

        let region_routes_map = region_routes
            .iter()
            .map(|r| (r.region.id, r))
            .collect::<HashMap<_, _>>();
        let source_region_routes = sources
            .iter()
            .map(|s| {
                region_routes_map
                    .get(&s.region_id())
                    .context(error::RepartitionSourceRegionMissingSnafu {
                        group_id,
                        region_id: s.region_id(),
                    })
                    .and_then(|r| ensure_source_region_route_expr_match(r, s))
            })
            .collect::<Result<Vec<_>>>()?;
        let target_region_routes = targets
            .iter()
            .map(|t| {
                region_routes_map
                    .get(&t.region_id)
                    .context(error::RepartitionTargetRegionMissingSnafu {
                        group_id,
                        region_id: t.region_id,
                    })
                    .map(|r| (*r).clone())
            })
            .collect::<Result<Vec<_>>>()?;
        for target_region_route in &target_region_routes {
            ensure!(
                target_region_route.leader_peer.is_some(),
                error::UnexpectedSnafu {
                    violated: format!(
                        "Leader peer is not set for region: {}",
                        target_region_route.region.id
                    ),
                }
            );
        }
        let central_region = sources[0].region_id();
        let central_region_datanode = source_region_routes[0]
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "Leader peer is not set for central region: {}",
                    central_region
                ),
            })?
            .clone();

        Ok(GroupPrepareResult {
            source_routes: source_region_routes,
            target_routes: target_region_routes,
            central_region,
            central_region_datanode,
        })
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for RepartitionStart {
    /// Captures the group prepare result.
    ///
    /// Retry:
    /// - Failed to get the table route.
    ///
    /// Abort
    /// - Table route not found.
    /// - Table route is not physical.
    /// - Failed to ensure the route is present.
    /// - Failed to capture the group prepare result.
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.persistent_ctx.group_prepare_result.is_some() {
            return Ok((
                Box::new(UpdateMetadata::ApplyStaging),
                Status::executing(true),
            ));
        }
        let table_id = ctx.persistent_ctx.table_id;
        let group_id = ctx.persistent_ctx.group_id;
        let table_route_value = ctx.get_table_route_value().await?.into_inner();
        let region_routes = region_routes(table_id, &table_route_value)?;
        let group_prepare_result = Self::ensure_route_present(
            group_id,
            region_routes,
            &ctx.persistent_ctx.sources,
            &ctx.persistent_ctx.targets,
        )?;
        ctx.persistent_ctx.group_prepare_result = Some(group_prepare_result);
        debug!(
            "Repartition group {}: captured {} sources, {} targets",
            group_id,
            ctx.persistent_ctx.sources.len(),
            ctx.persistent_ctx.targets.len()
        );

        if ctx.persistent_ctx.sync_region {
            let prepare_result = ctx.persistent_ctx.group_prepare_result.as_ref().unwrap();
            let allocated_region_ids: HashSet<_> = ctx
                .persistent_ctx
                .allocated_region_ids
                .iter()
                .copied()
                .collect();
            let region_routes: Vec<_> = prepare_result
                .target_routes
                .iter()
                .filter(|route| allocated_region_ids.contains(&route.region.id))
                .cloned()
                .collect();
            if !region_routes.is_empty() {
                return Ok((
                    Box::new(SyncRegion { region_routes }),
                    Status::executing(true),
                ));
            }
        }

        Ok((
            Box::new(UpdateMetadata::ApplyStaging),
            Status::executing(true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;
    use uuid::Uuid;

    use crate::error::Error;
    use crate::procedure::repartition::group::State;
    use crate::procedure::repartition::group::repartition_start::RepartitionStart;
    use crate::procedure::repartition::group::update_metadata::UpdateMetadata;
    use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr, test_region_route,
    };

    /// A group whose deadline has already passed still writes the route switch
    /// and fails on the next step.
    #[tokio::test]
    async fn rpfault_s1_group_past_its_deadline_still_switches() {
        use crate::procedure::repartition::group::enter_staging_region::EnterStagingRegion;

        let env = TestingEnv::new();
        let table_id = 1024;
        let source_id = RegionId::new(table_id, 1);
        let new_target_id = RegionId::new(table_id, 2);
        env.create_physical_table_metadata(
            table_id,
            vec![
                test_region_route(source_id, &range_expr("x", 0, 100).as_json_str().unwrap()),
                test_region_route(
                    new_target_id,
                    &range_expr("x", 50, 100).as_json_str().unwrap(),
                ),
            ],
        )
        .await;
        let sources = vec![SourceRegionDescriptor::partitioned(
            source_id,
            range_expr("x", 0, 100),
        )];
        let targets = vec![
            TargetRegionDescriptor {
                region_id: source_id,
                partition_expr: range_expr("x", 0, 50),
            },
            TargetRegionDescriptor {
                region_id: new_target_id,
                partition_expr: range_expr("x", 50, 100),
            },
        ];
        let mut persistent_ctx = new_persistent_context(table_id, sources, targets);
        persistent_ctx.timeout = std::time::Duration::ZERO;
        let mut ctx = env.create_context(persistent_ctx);
        let procedure_ctx = TestingEnv::procedure_context();
        assert!(ctx.next_operation_timeout().is_none());

        RepartitionStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        // The switch is written although the deadline has passed.
        UpdateMetadata::ApplyStaging
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let table_route = ctx.get_table_route_value().await.unwrap();
        let routes = table_route.region_routes().unwrap();
        let source_route = routes.iter().find(|r| r.region.id == source_id).unwrap();
        assert_eq!(
            range_expr("x", 0, 50).as_json_str().unwrap(),
            source_route.region.partition_expr
        );
        assert!(source_route.is_leader_staging());
        // The next step fails with a non-retryable error.
        let err = EnterStagingRegion
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap_err();
        println!("OBSERVED s1 step after the switch: {err}");
        assert_matches!(err, Error::ExceededDeadline { .. });
    }

    /// The group's lock keys cover its sources only, not newly allocated targets.
    #[test]
    fn rpfault_s3_group_lock_keys_do_not_cover_new_targets() {
        let table_id = 1024;
        let source_id = RegionId::new(table_id, 1);
        let new_target_id = RegionId::new(table_id, 2);
        let persistent_ctx = new_persistent_context(
            table_id,
            vec![SourceRegionDescriptor::partitioned(
                source_id,
                range_expr("x", 0, 100),
            )],
            vec![
                TargetRegionDescriptor {
                    region_id: source_id,
                    partition_expr: range_expr("x", 0, 50),
                },
                TargetRegionDescriptor {
                    region_id: new_target_id,
                    partition_expr: range_expr("x", 50, 100),
                },
            ],
        );
        let keys = format!("{:?}", persistent_ctx.lock_key());
        println!("OBSERVED s3 group lock keys: {keys}");
        assert!(keys.contains(&source_id.as_u64().to_string()));
        assert!(!keys.contains(&new_target_id.as_u64().to_string()));
    }

    /// The parent persists nothing after Dispatch, so after a metasrv restart
    /// it re-dispatches every group with a fresh context. A group that already
    /// ran ApplyStaging must still get past RepartitionStart when re-dispatched.
    #[tokio::test]
    async fn rpfault_f2_redispatched_group_after_switch_is_rejected() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let source_id = RegionId::new(table_id, 1);
        let new_target_id = RegionId::new(table_id, 2);
        env.create_physical_table_metadata(
            table_id,
            vec![
                test_region_route(source_id, &range_expr("x", 0, 100).as_json_str().unwrap()),
                test_region_route(
                    new_target_id,
                    &range_expr("x", 50, 100).as_json_str().unwrap(),
                ),
            ],
        )
        .await;
        // Split: the source keeps [0, 50), the new region takes [50, 100).
        let sources = vec![SourceRegionDescriptor::partitioned(
            source_id,
            range_expr("x", 0, 100),
        )];
        let targets = vec![
            TargetRegionDescriptor {
                region_id: source_id,
                partition_expr: range_expr("x", 0, 50),
            },
            TargetRegionDescriptor {
                region_id: new_target_id,
                partition_expr: range_expr("x", 50, 100),
            },
        ];
        let procedure_ctx = TestingEnv::procedure_context();

        // First dispatch: RepartitionStart, then ApplyStaging commits the new routes.
        let mut ctx = env.create_context(new_persistent_context(
            table_id,
            sources.clone(),
            targets.clone(),
        ));
        let (next, _) = RepartitionStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        assert!(next.as_any().is::<UpdateMetadata>());
        UpdateMetadata::ApplyStaging
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        // Metasrv restarts and Dispatch re-creates the group with a fresh context.
        let mut ctx = env.create_context(new_persistent_context(table_id, sources, targets));
        let result = RepartitionStart.next(&mut ctx, &procedure_ctx).await;
        // On main the re-dispatched group is rejected, which ends in the parent rollback.
        println!(
            "OBSERVED f2 re-dispatched group: {:?}",
            result.as_ref().map(|_| ()).map_err(|e| e.to_string())
        );
        assert_matches!(result.unwrap_err(), Error::PartitionExprMismatch { .. });
    }

    #[test]
    fn test_ensure_route_present_missing_source_region() {
        let source_region =
            SourceRegionDescriptor::partitioned(RegionId::new(1024, 1), range_expr("x", 0, 100));
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 2),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::RepartitionSourceRegionMissing { .. });
    }

    #[test]
    fn test_ensure_route_present_partition_expr_mismatch() {
        let source_region =
            SourceRegionDescriptor::partitioned(RegionId::new(1024, 1), range_expr("x", 0, 100));
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 5).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::PartitionExprMismatch { .. });
    }

    #[test]
    fn test_ensure_route_present_default_source_matches_empty_partition_expr() {
        let source_region = SourceRegionDescriptor::Default {
            region_id: RegionId::new(1024, 1),
        };
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: String::new(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];

        let result = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_ensure_route_present_default_source_rejects_non_empty_partition_expr() {
        let source_region = SourceRegionDescriptor::Default {
            region_id: RegionId::new(1024, 1),
        };
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 1),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];

        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();

        assert_matches!(err, Error::PartitionExprMismatch { .. });
    }

    #[test]
    fn test_ensure_route_present_missing_target_region() {
        let source_region =
            SourceRegionDescriptor::partitioned(RegionId::new(1024, 1), range_expr("x", 0, 100));
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let region_routes = vec![RegionRoute {
            region: Region {
                id: RegionId::new(1024, 1),
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }];
        let err = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        )
        .unwrap_err();
        assert_matches!(err, Error::RepartitionTargetRegionMissing { .. });
    }

    #[test]
    fn test_ensure_route_present_legacy_partition_expr_source() {
        let source_region =
            SourceRegionDescriptor::partitioned(RegionId::new(1024, 1), range_expr("x", 0, 100));
        let target_region = TargetRegionDescriptor {
            region_id: RegionId::new(1024, 2),
            partition_expr: range_expr("x", 0, 10),
        };
        let legacy_partition_expr = range_expr("x", 0, 100).as_json_str().unwrap();
        let legacy_region_json = serde_json::json!({
            "id": RegionId::new(1024, 1).as_u64(),
            "name": "",
            "partition": {
                "column_list": ["x"],
                "value_list": [legacy_partition_expr]
            },
            "partition_expr": "",
            "attrs": {}
        });

        let region_routes = vec![
            RegionRoute {
                region: serde_json::from_value(legacy_region_json).unwrap(),
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(1024, 2),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            },
        ];

        let result = RepartitionStart::ensure_route_present(
            Uuid::new_v4(),
            &region_routes,
            &[source_region],
            &[target_region],
        );
        assert!(result.is_ok());
    }
}

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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use api::v1::add_column_location::LocationType;
use api::v1::alter_expr::Kind;
use api::v1::meta::Partition;
use api::v1::region::region_request::{self, Body as PbRegionRequest};
use api::v1::region::{CreateRequest as PbCreateRegionRequest, RegionColumnDef};
use api::v1::{
    region, AddColumn, AddColumnLocation, AddColumns, AlterExpr, ColumnDataType,
    ColumnDef as PbColumnDef, DropColumn, DropColumns, SemanticType,
};
use client::client_manager::DatanodeClients;
use common_catalog::consts::MITO2_ENGINE;
use common_meta::datanode_manager::DatanodeManagerRef;
use common_meta::ddl::alter_table::AlterTableProcedure;
use common_meta::ddl::create_logical_tables::{CreateLogicalTablesProcedure, CreateTablesState};
use common_meta::ddl::create_table::*;
use common_meta::ddl::drop_table::executor::DropTableExecutor;
use common_meta::ddl::drop_table::DropTableProcedure;
use common_meta::ddl::test_util::columns::TestColumnDefBuilder;
use common_meta::ddl::test_util::create_table::{
    build_raw_table_info_from_expr, TestCreateTableExprBuilder,
};
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use common_meta::key::DeserializedValueWithBytes;
use common_meta::rpc::ddl::{AlterTableTask, CreateTableTask, DropTableTask};
use common_meta::rpc::router::{find_leaders, RegionRoute};
use common_meta::table_name::TableName;
use common_procedure::Status;
use store_api::storage::RegionId;

use crate::procedure::utils::mock::EchoRegionServer;
use crate::procedure::utils::test_data;

fn create_table_task(table_name: Option<&str>) -> CreateTableTask {
    let expr = TestCreateTableExprBuilder::default()
        .catalog_name("my_catalog")
        .schema_name("my_schema")
        .table_name(table_name.unwrap_or("my_table"))
        .desc("blabla")
        .column_defs([
            TestColumnDefBuilder::default()
                .name("ts")
                .data_type(ColumnDataType::TimestampMillisecond)
                .is_nullable(false)
                .semantic_type(SemanticType::Timestamp)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("my_tag1")
                .data_type(ColumnDataType::String)
                .is_nullable(true)
                .semantic_type(SemanticType::Tag)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("my_tag2")
                .data_type(ColumnDataType::String)
                .is_nullable(true)
                .semantic_type(SemanticType::Tag)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("my_field_column")
                .data_type(ColumnDataType::Int32)
                .is_nullable(true)
                .semantic_type(SemanticType::Field)
                .build()
                .unwrap()
                .into(),
        ])
        .time_index("ts")
        .primary_keys(vec!["my_tag2".into(), "my_tag1".into()])
        .build()
        .unwrap()
        .into();

    let table_info = build_raw_table_info_from_expr(&expr);
    CreateTableTask::new(
        expr,
        vec![Partition {
            column_list: vec![],
            value_list: vec![],
        }],
        table_info,
    )
}

#[test]
fn test_region_request_builder() {
    let mut procedure = CreateTableProcedure::new(
        1,
        create_table_task(None),
        test_data::new_ddl_context(Arc::new(DatanodeClients::default())),
    );

    procedure.set_allocated_metadata(
        1024,
        PhysicalTableRouteValue::new(test_data::new_region_routes()),
        HashMap::default(),
    );

    let template = procedure.new_region_request_builder(None).unwrap();

    let expected = PbCreateRegionRequest {
        region_id: 0,
        engine: MITO2_ENGINE.to_string(),
        column_defs: vec![
            RegionColumnDef {
                column_def: Some(PbColumnDef {
                    name: "ts".to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: String::new(),
                    ..Default::default()
                }),
                column_id: 0,
            },
            RegionColumnDef {
                column_def: Some(PbColumnDef {
                    name: "my_tag1".to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: String::new(),
                    ..Default::default()
                }),
                column_id: 1,
            },
            RegionColumnDef {
                column_def: Some(PbColumnDef {
                    name: "my_tag2".to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: String::new(),
                    ..Default::default()
                }),
                column_id: 2,
            },
            RegionColumnDef {
                column_def: Some(PbColumnDef {
                    name: "my_field_column".to_string(),
                    data_type: ColumnDataType::Int32 as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: String::new(),
                    ..Default::default()
                }),
                column_id: 3,
            },
        ],
        primary_key: vec![2, 1],
        path: String::new(),
        options: HashMap::new(),
    };
    assert_eq!(template.template(), &expected);
}

async fn new_datanode_manager(
    region_server: &EchoRegionServer,
    region_routes: &[RegionRoute],
) -> DatanodeManagerRef {
    let clients = DatanodeClients::default();

    let datanodes = find_leaders(region_routes);
    for datanode in datanodes {
        let client = region_server.new_client(&datanode);
        clients.insert_client(datanode, client).await;
    }

    Arc::new(clients)
}

#[tokio::test]
async fn test_on_datanode_create_regions() {
    let (region_server, mut rx) = EchoRegionServer::new();
    let region_routes = test_data::new_region_routes();
    let datanode_manager = new_datanode_manager(&region_server, &region_routes).await;

    let mut procedure = CreateTableProcedure::new(
        1,
        create_table_task(None),
        test_data::new_ddl_context(datanode_manager),
    );

    procedure.set_allocated_metadata(
        42,
        PhysicalTableRouteValue::new(test_data::new_region_routes()),
        HashMap::default(),
    );

    let expected_created_regions = Arc::new(Mutex::new(HashSet::from([
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 3),
    ])));
    let handle = tokio::spawn({
        let expected_created_regions = expected_created_regions.clone();
        let mut max_recv = expected_created_regions.lock().unwrap().len();
        async move {
            while let Some(PbRegionRequest::Create(request)) = rx.recv().await {
                let region_id = RegionId::from_u64(request.region_id);

                expected_created_regions.lock().unwrap().remove(&region_id);

                max_recv -= 1;
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure.on_datanode_create_regions().await.unwrap();
    assert!(matches!(status, Status::Executing { persist: true }));
    assert!(matches!(
        procedure.creator.data.state,
        CreateTableState::CreateMetadata
    ));

    handle.await.unwrap();

    assert!(expected_created_regions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_on_datanode_create_logical_regions() {
    let (region_server, mut rx) = EchoRegionServer::new();
    let region_routes = test_data::new_region_routes();
    let datanode_manager = new_datanode_manager(&region_server, &region_routes).await;
    let physical_table_route = TableRouteValue::physical(region_routes);
    let physical_table_id = 1;

    let task1 = create_table_task(Some("my_table1"));
    let task2 = create_table_task(Some("my_table2"));
    let task3 = create_table_task(Some("my_table3"));

    let ctx = test_data::new_ddl_context(datanode_manager);
    let kv_backend = ctx.table_metadata_manager.kv_backend();
    let physical_route_txn = ctx
        .table_metadata_manager
        .table_route_manager()
        .table_route_storage()
        .build_create_txn(physical_table_id, &physical_table_route)
        .unwrap()
        .0;
    let _ = kv_backend.txn(physical_route_txn).await.unwrap();
    let mut procedure =
        CreateLogicalTablesProcedure::new(1, vec![task1, task2, task3], physical_table_id, ctx);

    let expected_created_regions = Arc::new(Mutex::new(HashMap::from([(1, 3), (2, 3), (3, 3)])));

    let handle = tokio::spawn({
        let expected_created_regions = expected_created_regions.clone();
        let mut max_recv = expected_created_regions.lock().unwrap().len() * 3;
        async move {
            while let Some(PbRegionRequest::Creates(requests)) = rx.recv().await {
                for request in requests.requests {
                    let region_number = RegionId::from_u64(request.region_id).region_number();

                    let mut map = expected_created_regions.lock().unwrap();
                    let v = map.get_mut(&region_number).unwrap();
                    *v -= 1;
                    if *v == 0 {
                        map.remove(&region_number);
                    }

                    max_recv -= 1;
                    if max_recv == 0 {
                        break;
                    }
                }
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure.on_datanode_create_regions().await.unwrap();
    assert!(matches!(status, Status::Executing { persist: true }));
    assert!(matches!(
        procedure.data.state(),
        &CreateTablesState::CreateMetadata
    ));

    handle.await.unwrap();

    assert!(expected_created_regions.lock().unwrap().is_empty());

    let status = procedure.on_create_metadata().await.unwrap();
    assert!(status.is_done());
}

#[tokio::test]
async fn test_on_datanode_drop_regions() {
    let drop_table_task = DropTableTask {
        catalog: "my_catalog".to_string(),
        schema: "my_schema".to_string(),
        table: "my_table".to_string(),
        table_id: 42,
        drop_if_exists: false,
    };
    let drop_table_executor = DropTableExecutor::new(
        TableName::new("my_catalog", "my_schema", "my_table"),
        42,
        false,
    );
    let (region_server, mut rx) = EchoRegionServer::new();
    let region_routes = test_data::new_region_routes();
    let datanode_manager = new_datanode_manager(&region_server, &region_routes).await;

    let procedure = DropTableProcedure::new(
        1,
        drop_table_task,
        DeserializedValueWithBytes::from_inner(TableRouteValue::physical(region_routes)),
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(test_data::new_table_info())),
        test_data::new_ddl_context(datanode_manager),
    );

    let expected_dropped_regions = Arc::new(Mutex::new(HashSet::from([
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 3),
    ])));
    let handle = tokio::spawn({
        let expected_dropped_regions = expected_dropped_regions.clone();
        let mut max_recv = expected_dropped_regions.lock().unwrap().len();
        async move {
            while let Some(region_request::Body::Drop(request)) = rx.recv().await {
                let region_id = RegionId::from_u64(request.region_id);

                expected_dropped_regions.lock().unwrap().remove(&region_id);

                max_recv -= 1;
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure
        .on_datanode_drop_regions(&drop_table_executor)
        .await
        .unwrap();
    assert!(status.is_done());

    handle.await.unwrap();

    assert!(expected_dropped_regions.lock().unwrap().is_empty());
}

#[test]
fn test_create_alter_region_request() {
    let alter_table_task = AlterTableTask {
        alter_table: AlterExpr {
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "my_table".to_string(),
            kind: Some(Kind::AddColumns(AddColumns {
                add_columns: vec![AddColumn {
                    column_def: Some(PbColumnDef {
                        name: "my_tag3".to_string(),
                        data_type: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: b"hello".to_vec(),
                        semantic_type: SemanticType::Tag as i32,
                        comment: String::new(),
                        ..Default::default()
                    }),
                    location: Some(AddColumnLocation {
                        location_type: LocationType::After as i32,
                        after_column_name: "my_tag2".to_string(),
                    }),
                }],
            })),
        },
    };

    let procedure = AlterTableProcedure::new(
        1,
        alter_table_task,
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(test_data::new_table_info())),
        test_data::new_ddl_context(Arc::new(DatanodeClients::default())),
    )
    .unwrap();

    let region_id = RegionId::new(42, 1);
    let alter_region_request = procedure.create_alter_region_request(region_id).unwrap();
    assert_eq!(alter_region_request.region_id, region_id.as_u64());
    assert_eq!(alter_region_request.schema_version, 1);
    assert_eq!(
        alter_region_request.kind,
        Some(region::alter_request::Kind::AddColumns(
            region::AddColumns {
                add_columns: vec![region::AddColumn {
                    column_def: Some(RegionColumnDef {
                        column_def: Some(PbColumnDef {
                            name: "my_tag3".to_string(),
                            data_type: ColumnDataType::String as i32,
                            is_nullable: true,
                            default_constraint: b"hello".to_vec(),
                            semantic_type: SemanticType::Tag as i32,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        column_id: 3,
                    }),
                    location: Some(AddColumnLocation {
                        location_type: LocationType::After as i32,
                        after_column_name: "my_tag2".to_string(),
                    }),
                }]
            }
        ))
    );
}

#[tokio::test]
async fn test_submit_alter_region_requests() {
    let alter_table_task = AlterTableTask {
        alter_table: AlterExpr {
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "my_table".to_string(),
            kind: Some(Kind::DropColumns(DropColumns {
                drop_columns: vec![DropColumn {
                    name: "my_field_column".to_string(),
                }],
            })),
        },
    };

    let (region_server, mut rx) = EchoRegionServer::new();
    let region_routes = test_data::new_region_routes();
    let datanode_manager = new_datanode_manager(&region_server, &region_routes).await;

    let context = test_data::new_ddl_context(datanode_manager);
    let table_info = test_data::new_table_info();
    context
        .table_metadata_manager
        .create_table_metadata(
            table_info.clone(),
            TableRouteValue::physical(region_routes),
            HashMap::default(),
        )
        .await
        .unwrap();

    let mut procedure = AlterTableProcedure::new(
        1,
        alter_table_task,
        DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info)),
        context,
    )
    .unwrap();

    let expected_altered_regions = Arc::new(Mutex::new(HashSet::from([
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 3),
    ])));
    let handle = tokio::spawn({
        let expected_altered_regions = expected_altered_regions.clone();
        let mut max_recv = expected_altered_regions.lock().unwrap().len();
        async move {
            while let Some(region_request::Body::Alter(request)) = rx.recv().await {
                let region_id = RegionId::from_u64(request.region_id);

                expected_altered_regions.lock().unwrap().remove(&region_id);

                max_recv -= 1;
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure.submit_alter_region_requests().await.unwrap();
    assert!(matches!(status, Status::Executing { persist: true }));

    handle.await.unwrap();

    assert!(expected_altered_regions.lock().unwrap().is_empty());
}

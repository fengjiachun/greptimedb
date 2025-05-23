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

use std::fmt::{Display, Formatter};

use api::v1::TableName as PbTableName;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::error::InvalidTableNameSnafu;
use crate::table_reference::TableReference;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct TableName {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

impl Display for TableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&common_catalog::format_full_table_name(
            &self.catalog_name,
            &self.schema_name,
            &self.table_name,
        ))
    }
}

impl TableName {
    pub fn new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            schema_name: schema_name.into(),
            table_name: table_name.into(),
        }
    }

    pub fn table_ref(&self) -> TableReference<'_> {
        TableReference {
            catalog: &self.catalog_name,
            schema: &self.schema_name,
            table: &self.table_name,
        }
    }
}

impl From<TableName> for PbTableName {
    fn from(table_name: TableName) -> Self {
        Self {
            catalog_name: table_name.catalog_name,
            schema_name: table_name.schema_name,
            table_name: table_name.table_name,
        }
    }
}

impl From<PbTableName> for TableName {
    fn from(table_name: PbTableName) -> Self {
        Self {
            catalog_name: table_name.catalog_name,
            schema_name: table_name.schema_name,
            table_name: table_name.table_name,
        }
    }
}

impl From<TableReference<'_>> for TableName {
    fn from(table_ref: TableReference) -> Self {
        Self::new(table_ref.catalog, table_ref.schema, table_ref.table)
    }
}

impl TryFrom<Vec<String>> for TableName {
    type Error = error::Error;

    fn try_from(v: Vec<String>) -> Result<Self, Self::Error> {
        ensure!(
            !v.is_empty() && v.len() <= 3,
            InvalidTableNameSnafu {
                s: format!("{v:?}")
            }
        );
        let mut v = v.into_iter();
        match (v.next(), v.next(), v.next()) {
            (Some(catalog_name), Some(schema_name), Some(table_name)) => Ok(Self {
                catalog_name,
                schema_name,
                table_name,
            }),
            (Some(schema_name), Some(table_name), None) => Ok(Self {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name,
                table_name,
            }),
            (Some(table_name), None, None) => Ok(Self {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
            }),
            // Unreachable because it's ensured that "v" is not empty,
            // and its iterator will not yield `Some` after `None`.
            _ => unreachable!(),
        }
    }
}

// Copyright 2023 WHERE TRUE Technologies.
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

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    execution::context::SessionState,
    logical_expr::{
        Extension, LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
    },
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};

use crate::{
    exome::logical_plan::LogicalPlan,
    exome_catalog_manager::{CatalogName, LibraryName, SchemaName, TableName},
};

pub enum ExtensionType {
    CreateExomeCatalog,
    CreateExomeSchema,
    DropExomeCatalog,
    CreateExomeTable,
}

impl FromStr for ExtensionType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "CreateExomeCatalog" => Ok(ExtensionType::CreateExomeCatalog),
            "CreateExomeSchema" => Ok(ExtensionType::CreateExomeSchema),
            "DropExomeCatalog" => Ok(ExtensionType::DropExomeCatalog),
            "CreateExomeTable" => Ok(ExtensionType::CreateExomeTable),
            _ => Err(()),
        }
    }
}

pub trait DfExtensionNode: Sized + UserDefinedLogicalNodeCore {
    const NODE_NAME: &'static str;

    fn into_extension(self) -> Extension {
        Extension {
            node: Arc::new(self),
        }
    }

    fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::DataFusion(datafusion::logical_expr::LogicalPlan::Extension(
            self.into_extension(),
        ))
    }
}

pub struct ExomeExtensionPlanner {}

impl ExomeExtensionPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ExomeExtensionPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
/// ExtensionPlanner for Exome
///
/// This is responsible for mediating between the Exome logical plan and physical plan.
impl ExtensionPlanner for ExomeExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&DfLogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let extension_type = node.name().parse::<ExtensionType>().map_err(|_| {
            datafusion::error::DataFusionError::Internal(format!(
                "Unknown extension type {}",
                node.name()
            ))
        })?;

        match extension_type {
            ExtensionType::CreateExomeCatalog => {
                let create_catalog_logical_plan = node
                    .as_any()
                    .downcast_ref::<crate::exome::logical_plan::CreateExomeCatalog>()
                    .unwrap();

                let physical_plan = crate::exome::physical_plan::CreateCatalogExec::new(
                    create_catalog_logical_plan.name.clone(),
                    "example_library".to_string(),
                );

                return Ok(Some(Arc::new(physical_plan)));
            }
            ExtensionType::CreateExomeTable => {
                let create_table_logical_plan = node
                    .as_any()
                    .downcast_ref::<crate::exome::logical_plan::CreateExomeTable>()
                    .unwrap();

                let physical_plan = crate::exome::physical_plan::CreateTableExec::new(
                    TableName(create_table_logical_plan.name.clone()),
                    SchemaName(create_table_logical_plan.schema_name.clone()),
                    CatalogName(create_table_logical_plan.catalog_name.clone()),
                    LibraryName("example_library".to_string()),
                    create_table_logical_plan.location.clone(),
                    create_table_logical_plan.file_format.clone(),
                    create_table_logical_plan.is_listing,
                    create_table_logical_plan.compression_type.clone(),
                );

                return Ok(Some(Arc::new(physical_plan)));
            }
            ExtensionType::CreateExomeSchema => {
                let create_schema_logical_plan = node
                    .as_any()
                    .downcast_ref::<crate::exome::logical_plan::CreateExomeSchema>()
                    .unwrap();

                let physical_plan = crate::exome::physical_plan::CreateSchemaExec::new(
                    SchemaName(create_schema_logical_plan.name.clone()),
                    CatalogName(create_schema_logical_plan.catalog_name.clone()),
                );

                return Ok(Some(Arc::new(physical_plan)));
            }
            ExtensionType::DropExomeCatalog => {
                let drop_catalog_logical_plan = node
                    .as_any()
                    .downcast_ref::<crate::exome::logical_plan::DropExomeCatalog>()
                    .unwrap();

                let physical_plan = crate::exome::physical_plan::DropCatalogExec::new(
                    drop_catalog_logical_plan.name.clone(),
                    "public".to_string(),
                );

                return Ok(Some(Arc::new(physical_plan)));
            }
        }
    }
}

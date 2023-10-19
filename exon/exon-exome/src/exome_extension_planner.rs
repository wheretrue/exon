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
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};

pub enum ExtensionType {
    CreateCatalog,
}

impl FromStr for ExtensionType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "create_catalog" => Ok(ExtensionType::CreateCatalog),
            _ => Err(()),
        }
    }
}

pub struct ExomeExtensionPlanner {}

impl ExomeExtensionPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for ExomeExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
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
            ExtensionType::CreateCatalog => {
                let create_catalog_logical_plan = node
                    .as_any()
                    .downcast_ref::<crate::exome::logical_plan::CreateCatalog>()
                    .unwrap();

                let physical_plan = crate::exome::physical_plan::CreateCatalogExec::new(
                    create_catalog_logical_plan.name.clone(),
                    "00000000-0000-0000-0000-000000000000".to_string(),
                );

                return Ok(Some(Arc::new(physical_plan)));
            }
        }
    }
}

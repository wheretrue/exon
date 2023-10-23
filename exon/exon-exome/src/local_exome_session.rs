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

use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    logical_expr::{DdlStatement, LogicalPlan as DfLogicalPlan},
    physical_plan::{execute_stream, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::SessionContext,
};
use exon::{new_exon_config, ExonSessionExt};

use crate::{
    exome::{
        logical_plan::{CreateExomeCatalog, LogicalPlan},
        register_catalog, ExomeCatalogClient,
    },
    exome_extension_planner::DfExtensionNode,
    exon_client::ExonClient,
    ExomeCatalogManager, ExomeExtensionPlanner,
};

pub struct LocalExomeSession {
    pub(crate) session: SessionContext,
}

impl LocalExomeSession {
    pub async fn connect(
        url: String,
        organization_id: String,
        token: String,
    ) -> Result<Self, DataFusionError> {
        let client = ExomeCatalogClient::connect(url, organization_id, token)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Error connecting to Exome {}", e)))?;

        let extension_manager = ExomeCatalogManager::new(client.clone());

        let config = new_exon_config().with_extension(Arc::new(extension_manager));

        let session = SessionContext::with_config_exon(config);

        Ok(Self { session })
    }

    pub async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let state = self.session.state();
        let plan = state.optimize(&plan)?;

        let ddl_planner = ExomeExtensionPlanner::new();

        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ddl_planner)]);
        let plan = planner.create_physical_plan(&plan, &state).await?;

        Ok(plan)
    }

    pub async fn execute_logical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, DataFusionError> {
        match plan {
            DfLogicalPlan::Ddl(ddl_plan) => match ddl_plan {
                DdlStatement::CreateCatalog(create_catalog) => {
                    let create_exome_catalog = CreateExomeCatalog::from(create_catalog);
                    let create_exome_catalog_plan = match create_exome_catalog.into_logical_plan() {
                        LogicalPlan::DataFusion(plan) => plan,
                    };

                    let physical_plan =
                        self.create_physical_plan(create_exome_catalog_plan).await?;

                    let execution = execute_stream(physical_plan, self.session.task_ctx())?;

                    Ok(execution)
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "Unsupported DDL statement".to_string(),
                    ))
                }
            },
            _ => {
                let df = self.session.execute_logical_plan(plan).await?;
                df.execute_stream().await
            }
        }
    }
}

#[async_trait::async_trait]
impl ExonClient for LocalExomeSession {
    async fn create_catalog(
        &mut self,
        catalog_name: String,
        library_id: String,
    ) -> Result<(), DataFusionError> {
        let manager = self
            .session
            .state()
            .task_ctx()
            .session_config()
            .get_extension::<ExomeCatalogManager>()
            .unwrap();

        manager
            .client
            .clone()
            .create_catalog(catalog_name, library_id)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Error creating catalog {}", e)))?;

        Ok(())
    }

    async fn register_library(&mut self, library_id: String) -> Result<(), DataFusionError> {
        let manager = self
            .session
            .state()
            .task_ctx()
            .session_config()
            .get_extension::<ExomeCatalogManager>()
            .unwrap();

        let exome_catalogs = manager
            .client
            .clone()
            .get_catalogs(library_id)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Error getting catalogs for library {}", e))
            })?;

        for catalog in exome_catalogs {
            let catalog_name = catalog.name.clone();
            let catalog_id = catalog.id.clone();

            register_catalog(
                manager.client.clone(),
                Arc::new(self.session.clone()),
                catalog_name,
                catalog_id,
            )
            .await?;
        }

        Ok(())
    }

    async fn register_library_by_name(
        &mut self,
        library_name: String,
    ) -> Result<(), DataFusionError> {
        let manager = self
            .session
            .state()
            .task_ctx()
            .session_config()
            .get_extension::<ExomeCatalogManager>()
            .unwrap();

        let library = manager
            .client
            .clone()
            .get_library_by_name(library_name)
            .await
            .unwrap();

        match library {
            Some(library) => {
                let library_id = library.id.clone();

                self.register_library(library_id).await
            }
            None => Err(DataFusionError::Execution("Library not found".to_string())),
        }
    }
}

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
        logical_plan::{CreateExomeCatalog, CreateExomeSchema, CreateExomeTable, LogicalPlan},
        register_catalog, ExomeCatalogClient,
    },
    exome_catalog_manager::{CatalogName, LibraryName, OrganizationName},
    exome_extension_planner::DfExtensionNode,
    exon_client::ExonClient,
    ExomeCatalogManager, ExomeExtensionPlanner,
};

pub struct ExomeSession {
    pub session: SessionContext,
}

impl ExomeSession {
    pub async fn connect(
        url: String,
        organization_name: String,
        token: String,
    ) -> Result<Self, DataFusionError> {
        let client = ExomeCatalogClient::connect(url, organization_name, token)
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

                    #[allow(clippy::infallible_destructuring_match)]
                    let create_exome_catalog_plan = match create_exome_catalog.into_logical_plan() {
                        LogicalPlan::DataFusion(plan) => plan,
                    };

                    let physical_plan =
                        self.create_physical_plan(create_exome_catalog_plan).await?;

                    let execution = execute_stream(physical_plan, self.session.task_ctx())?;

                    Ok(execution)
                }
                DdlStatement::CreateExternalTable(create_external_table) => {
                    let create_exome_table = CreateExomeTable::try_from(create_external_table)
                        .map_err(|_| {
                            DataFusionError::Execution("Error creating table".to_string())
                        })?;

                    #[allow(clippy::infallible_destructuring_match)]
                    let create_exome_table_plan = match create_exome_table.into_logical_plan() {
                        LogicalPlan::DataFusion(plan) => plan,
                    };

                    let physical_plan = self.create_physical_plan(create_exome_table_plan).await?;

                    let execution = execute_stream(physical_plan, self.session.task_ctx())?;

                    Ok(execution)
                }
                DdlStatement::CreateCatalogSchema(create_schema) => {
                    let create_exome_schema =
                        CreateExomeSchema::try_from(create_schema).map_err(|_| {
                            DataFusionError::Execution("Error creating schema".to_string())
                        })?;

                    #[allow(clippy::infallible_destructuring_match)]
                    let create_exome_schema_plan = match create_exome_schema.into_logical_plan() {
                        LogicalPlan::DataFusion(plan) => plan,
                    };

                    let physical_plan = self.create_physical_plan(create_exome_schema_plan).await?;

                    let execution = execute_stream(physical_plan, self.session.task_ctx())?;

                    Ok(execution)
                }
                _ => Err(DataFusionError::Execution(
                    "Unsupported DDL statement".to_string(),
                )),
            },
            _ => {
                let df = self.session.execute_logical_plan(plan).await?;
                df.execute_stream().await
            }
        }
    }
}

impl From<ExomeCatalogClient> for ExomeSession {
    fn from(client: ExomeCatalogClient) -> Self {
        let extension_manager = ExomeCatalogManager::new(client.clone());

        let config = new_exon_config().with_extension(Arc::new(extension_manager));

        let session = SessionContext::with_config_exon(config);

        Self { session }
    }
}

#[async_trait::async_trait]
impl ExonClient for ExomeSession {
    async fn create_catalog(
        &mut self,
        catalog_name: CatalogName,
        library_name: LibraryName,
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
            .create_catalog(
                catalog_name,
                library_name,
                OrganizationName(manager.client.organization_name.clone()),
            )
            .await
            .map_err(|e| DataFusionError::Execution(format!("Error creating catalog {}", e)))?;

        Ok(())
    }

    async fn register_library(
        &mut self,
        organization_name: OrganizationName,
        library_name: LibraryName,
    ) -> Result<(), DataFusionError> {
        let manager = self
            .session
            .state()
            .task_ctx()
            .session_config()
            .get_extension::<ExomeCatalogManager>()
            .unwrap();

        tracing::info!(
            "Registering library {} for organization {}",
            library_name,
            organization_name
        );

        let exome_catalogs = manager
            .client
            .clone()
            .get_catalogs(organization_name, library_name)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Error getting catalogs for library {}", e))
            })?;

        for catalog in exome_catalogs {
            let catalog_name = catalog.name.clone();
            let library_name = catalog.library_name.clone();
            let organization_name = catalog.organization_name.clone();

            tracing::info!(
                "Registering catalog {} for library {}",
                catalog_name,
                library_name
            );

            register_catalog(
                manager.client.clone(),
                Arc::new(self.session.clone()),
                organization_name,
                library_name,
                catalog_name,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use crate::ExomeSession;

    #[tokio::test]
    async fn test_exome_create_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let exome_session = ExomeSession::connect(
            "http://localhost:50051".to_string(),
            "public".to_string(),
            "token".to_string(),
        )
        .await?;

        // let sql = "CREATE DATABASE test_catalog;";

        // let df_logical_plan = exome_session
        //     .session
        //     .state()
        //     .create_logical_plan(sql)
        //     .await?;

        // let execution_result = exome_session.execute_logical_plan(df_logical_plan).await?;
        // let results = execution_result.try_collect::<Vec<_>>().await?;

        // assert_eq!(results.len(), 1);

        // // Let's try to create a catalog with the same name, it should fail
        // let sql = "CREATE DATABASE test_catalog;";
        // let df_logical_plan = exome_session
        //     .session
        //     .state()
        //     .create_logical_plan(sql)
        //     .await?;

        // let execution_result = exome_session.execute_logical_plan(df_logical_plan).await?;
        // let results = execution_result.try_collect::<Vec<_>>().await;

        // assert!(results.is_err());

        // Now create a schema
        // let sql = "CREATE SCHEMA test_catalog.test_schema;";
        // let df_logical_plan = exome_session
        //     .session
        //     .state()
        //     .create_logical_plan(sql)
        //     .await?;

        // let execution_result = exome_session.execute_logical_plan(df_logical_plan).await?;
        // let results = execution_result.try_collect::<Vec<_>>().await?;

        // assert_eq!(results.len(), 1);

        // Now create a table
        let sql = "CREATE EXTERNAL TABLE test_catalog.test_schema.test STORED AS FASTA COMPRESSION TYPE GZIP LOCATION 's3://test/test.fasta';";
        let df_logical_plan = exome_session
            .session
            .state()
            .create_logical_plan(sql)
            .await?;

        let execution_result = exome_session.execute_logical_plan(df_logical_plan).await?;
        let results = execution_result.try_collect::<Vec<_>>().await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }
}

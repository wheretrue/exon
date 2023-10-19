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
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::SessionContext,
};
use exon::ExonSessionExt;

use crate::{
    exome::{register_catalog, ExomeCatalogClient},
    ExomeExtensionPlanner,
};

pub struct ExomeSession {
    pub(crate) session: SessionContext,
}

impl Default for ExomeSession {
    fn default() -> Self {
        Self {
            session: SessionContext::new_exon(),
        }
    }
}

impl ExomeSession {
    pub async fn register_library(
        &mut self,
        library_id: String,
        client: &mut ExomeCatalogClient,
    ) -> Result<(), DataFusionError> {
        let exome_catalogs = client.get_catalogs(library_id).await.map_err(|e| {
            DataFusionError::Execution(format!("Error getting catalogs for library {}", e))
        })?;

        for catalog in exome_catalogs {
            let catalog_name = catalog.name.clone();
            let catalog_id = catalog.id.clone();

            register_catalog(
                client.clone(),
                Arc::new(self.session.clone()),
                catalog_name,
                catalog_id,
            )
            .await?;
        }

        Ok(())
    }

    pub async fn register_library_by_name(
        &mut self,
        library_name: String,
        client: &mut ExomeCatalogClient,
    ) -> Result<(), DataFusionError> {
        let library = client.get_library_by_name(library_name).await.unwrap();

        match library {
            Some(library) => {
                let library_id = library.id.clone();

                self.register_library(library_id, client).await
            }
            None => Err(DataFusionError::Execution("Library not found".to_string())),
        }
    }

    pub async fn create_physical_plan(
        &self,
        plan: LogicalPlan,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let state = self.session.state();
        let plan = state.optimize(&plan)?;

        let ddl_planner = ExomeExtensionPlanner::new();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ddl_planner)]);

        let plan = planner.create_physical_plan(&plan, &state).await?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use crate::{exome::ExomeCatalogClient, ExomeSession};

    // Test the client
    #[tokio::test]
    async fn test_exome_catalog_client() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = ExomeCatalogClient::connect(
            "http://localhost:50051".to_string(),
            "00000000-0000-0000-0000-000000000000".to_string(),
            "token".to_string(),
        )
        .await?;

        // let mut session = SessionContext::new_exon();
        let mut exome_session = ExomeSession::default();

        let catalog_name = "test_catalog";
        let schema_name = "test_schema";
        let table_name = "test_table";

        exome_session
            .register_library(
                "00000000-0000-0000-0000-000000000000".to_string(),
                &mut client,
            )
            .await?;

        let catalog_names = exome_session.session.catalog_names();

        assert!(catalog_names.contains(&catalog_name.to_string()));

        let sql = format!(
            "SELECT * FROM {}.{}.{}",
            catalog_name, schema_name, table_name
        );

        let df = exome_session.session.sql(&sql).await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 1);

        let first_batch = results.first().unwrap();
        assert_eq!(first_batch.num_rows(), 1);

        Ok(())
    }
}

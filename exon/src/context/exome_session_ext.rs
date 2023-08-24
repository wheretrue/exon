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

use datafusion::{error::DataFusionError, prelude::SessionContext};

use crate::exome::{register_catalog, ExomeCatalogClient};

use async_trait::async_trait;

/// Extension trait for [`SessionContext`] that adds methods for working with
/// Exome.
#[async_trait]
pub trait ExomeSessionExt {
    /// Registers the library on the SessionContext.
    async fn register_library(&mut self, library_id: String) -> Result<(), DataFusionError>;

    /// Registers the library on the SessionContext by name.
    async fn register_library_by_name(
        &mut self,
        library_name: String,
    ) -> Result<(), DataFusionError>;
}

#[async_trait]
impl ExomeSessionExt for SessionContext {
    async fn register_library(&mut self, library_id: String) -> Result<(), DataFusionError> {
        let mut client = ExomeCatalogClient::from_env().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create ExomeCatalogClient from environment: {}",
                e
            ))
        })?;

        let exome_catalogs = client.get_catalogs(library_id).await.unwrap();

        for catalog in exome_catalogs {
            let catalog_name = catalog.name.clone();
            let catalog_id = catalog.id.clone();

            register_catalog(
                client.clone(),
                self.clone().into(),
                catalog_name,
                catalog_id,
            )
            .await
            .unwrap();
        }

        Ok(())
    }

    async fn register_library_by_name(
        &mut self,
        library_name: String,
    ) -> Result<(), DataFusionError> {
        let mut client = ExomeCatalogClient::from_env().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create ExomeCatalogClient from environment: {}",
                e
            ))
        })?;

        let library = client.get_library_by_name(library_name).await.unwrap();

        match library {
            Some(library) => {
                let library_id = library.id.clone();

                self.register_library(library_id).await
            }
            None => return Err(DataFusionError::Execution("Library not found".to_string())),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "exome")]
#[allow(unused_imports)]
mod tests {
    use datafusion::prelude::SessionContext;

    use crate::{context::ExomeSessionExt, exome::ExomeCatalogClient, ExonSessionExt};

    // Test the client
    #[tokio::test]
    async fn test_exome_catalog_client() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = ExomeCatalogClient::connect(
            "http://localhost:50051".to_string(),
            "00000000-0000-0000-0000-000000000000".to_string(),
        )
        .await?;

        client.health_check().await?;

        let mut session = SessionContext::new_exon();

        let catalog_name = "test_catalog";
        let schema_name = "test_schema";
        let table_name = "test_table";

        session
            .register_library("00000000-0000-0000-0000-000000000000".to_string())
            .await?;

        let catalog_names = session.catalog_names();

        assert!(catalog_names.contains(&catalog_name.to_string()));

        let sql = format!(
            "SELECT * FROM {}.{}.{}",
            catalog_name, schema_name, table_name
        );

        let df = session.sql(&sql).await?;

        let results = df.collect().await?;

        assert_eq!(results.len(), 1);

        let first_batch = results.first().unwrap();
        assert_eq!(first_batch.num_rows(), 1);

        Ok(())
    }
}

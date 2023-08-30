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
    catalog::{CatalogProvider, MemoryCatalogProvider},
    error::DataFusionError,
    prelude::SessionContext,
};

mod catalog;

mod proto;

pub use catalog::ExomeCatalogClient;

/// Registers a given catalog into the session context.
pub async fn register_catalog(
    client: ExomeCatalogClient,
    session: Arc<SessionContext>,
    catalog_name: String,
    exome_catalog_id: String,
) -> Result<(), DataFusionError> {
    let memory_catalog = MemoryCatalogProvider::new();

    let mut client = client.clone();
    let schemas = client.get_schemas(exome_catalog_id).await.map_err(|e| {
        DataFusionError::Execution(format!("Error getting schemas for catalog {}", e))
    })?;

    for schema in schemas {
        let schema_name = schema.name.clone();

        let schema = catalog::Schema::new(schema, session.clone(), client.clone())
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Error creating schema for catalog {}", e))
            })?;

        memory_catalog
            .register_schema(&schema_name, Arc::new(schema))
            .map_err(|e| {
                DataFusionError::Execution(format!("Error registering schema for catalog {}", e))
            })?;
    }

    session.register_catalog(catalog_name, Arc::new(memory_catalog));

    Ok(())
}

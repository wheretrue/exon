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
    datasource::{
        datasource::TableProviderFactory,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};

use crate::datasources::ExonFileType;

/// A `ListingTableFactory` that adapts Exon FileFormats to `TableProvider`s.
#[derive(Debug, Clone, Default)]
pub struct ExonListingTableFactory {}

#[async_trait]
impl TableProviderFactory for ExonListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let file_compression_type = cmd.file_compression_type.into();

        let file_type = ExonFileType::from_str(&cmd.file_type).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "Unsupported file type: {}",
                &cmd.file_type,
            ))
        })?;

        let file_format = file_type.get_file_format(file_compression_type);

        let options = ListingOptions::new(file_format);
        let table_path = ListingTableUrl::parse(&cmd.location)?;
        let resolved_schema = options.infer_schema(state, &table_path).await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use datafusion::{
        catalog::{
            catalog::{CatalogProvider, MemoryCatalogProvider},
            listing_schema::ListingSchemaProvider,
        },
        error::DataFusionError,
        execution::runtime_env::{RuntimeConfig, RuntimeEnv},
        prelude::{SessionConfig, SessionContext},
    };
    use object_store::local::LocalFileSystem;

    use crate::datasources::ExonListingTableFactory;

    fn create_runtime_env() -> Result<RuntimeEnv, DataFusionError> {
        let rn_config = RuntimeConfig::new();
        RuntimeEnv::new(rn_config)
    }

    #[tokio::test]
    async fn test_in_catalog() {
        let mem_catalog: MemoryCatalogProvider = MemoryCatalogProvider::new();
        let object_store = Arc::new(LocalFileSystem::new());

        let cargo_manifest_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());

        let schema = ListingSchemaProvider::new(
            "file://".to_string(),
            cargo_manifest_path
                .join("test-data")
                .join("datasources")
                .join("sam")
                .to_str()
                .unwrap()
                .into(),
            Arc::new(ExonListingTableFactory::default()),
            object_store,
            "SAM".to_string(),
            false,
        );

        mem_catalog
            .register_schema("exon", Arc::new(schema))
            .unwrap();

        let session_config = SessionConfig::from_env().unwrap();
        let runtime_env = create_runtime_env().unwrap();
        let ctx = SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));

        ctx.register_catalog("exon", Arc::new(mem_catalog));
        ctx.refresh_catalogs().await.unwrap();

        let gotten_catalog = ctx.catalog("exon").unwrap();
        let schema_names = gotten_catalog.schema_names();
        assert_eq!(schema_names, vec!["exon"]);

        let new_schema = gotten_catalog.schema("exon").unwrap();
        let tables = new_schema.table_names();
        assert_eq!(tables, vec!["test"]);
    }
}

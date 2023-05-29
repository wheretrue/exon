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

use crate::datasources::TCAFileType;

/// A `ListingTableFactory` that adapts TCA FileFormats to `TableProvider`s.
#[derive(Debug, Clone, Default)]
pub struct TCAListingTableFactory {}

#[async_trait]
impl TableProviderFactory for TCAListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let file_compression_type = cmd.file_compression_type.into();

        eprintln!("file_compression_type: {:?}", file_compression_type);

        let file_type = TCAFileType::from_str(&cmd.file_type).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "Unsupported file type: {}",
                &cmd.file_type,
            ))
        })?;

        let file_format = file_type.get_file_format(file_compression_type)?;

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

    use crate::datasources::TCAListingTableFactory;

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
            Arc::new(TCAListingTableFactory::default()),
            object_store,
            "SAM".to_string(),
            false,
        );

        mem_catalog
            .register_schema("tca", Arc::new(schema))
            .unwrap();

        let session_config = SessionConfig::from_env().unwrap();
        let runtime_env = create_runtime_env().unwrap();
        let ctx = SessionContext::with_config_rt(session_config.clone(), Arc::new(runtime_env));

        ctx.register_catalog("tca", Arc::new(mem_catalog));
        ctx.refresh_catalogs().await.unwrap();

        let gotten_catalog = ctx.catalog("tca").unwrap();
        let schema_names = gotten_catalog.schema_names();
        assert_eq!(schema_names, vec!["tca"]);

        let new_schema = gotten_catalog.schema("tca").unwrap();
        let tables = new_schema.table_names();
        assert_eq!(tables, vec!["test"]);
    }
}

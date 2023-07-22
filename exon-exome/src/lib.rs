use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogList, CatalogProvider, MemoryCatalogProvider},
    datasource::{
        file_format::file_type::FileCompressionType,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableProvider,
    },
    error::DataFusionError,
    execution::context::SessionState,
    prelude::SessionContext,
};
use exon::{datasources::ExonFileType, ExonRuntimeEnvExt, ExonSessionExt};
use proto::GetSchemaByNameRequest;

mod proto;

struct ExomeCatalogClient {
    url: String,
    token: String,
    organization_id: String,

    catalog_service_client:
        proto::catalog_service_client::CatalogServiceClient<tonic::transport::Channel>,
}

impl ExomeCatalogClient {
    async fn connect(
        url: String,
        token: String,
        organization_id: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let catalog_service_client =
            proto::catalog_service_client::CatalogServiceClient::connect(url.clone()).await?;

        let mut s = Self {
            url,
            token,
            organization_id,
            catalog_service_client,
        };

        Ok(s)
    }

    async fn get_libraries(&mut self) -> Result<Vec<proto::Library>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::ListLibrariesRequest {});

        let response = self
            .catalog_service_client
            .list_libraries(request)
            .await?
            .into_inner();

        Ok(response.libraries)
    }

    async fn get_library_by_name(
        &mut self,
        name: String,
    ) -> Result<Option<proto::Library>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::GetLibraryByNameRequest {
            name,
            organization_id: self.organization_id.clone(),
        });

        let response = self
            .catalog_service_client
            .get_library_by_name(request)
            .await?
            .into_inner();

        Ok(response.library)
    }

    async fn get_catalogs(
        &mut self,
        library_id: String,
    ) -> Result<Vec<proto::Catalog>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::ListCatalogsRequest {
            library_id: library_id,
        });

        let response = self
            .catalog_service_client
            .list_catalogs(request)
            .await?
            .into_inner();

        Ok(response.catalogs)
    }

    async fn get_schemas(
        &mut self,
        catalog_id: String,
    ) -> Result<Vec<proto::Schema>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::ListSchemasRequest {
            catalog_id: catalog_id,
        });

        let response = self
            .catalog_service_client
            .list_schemas(request)
            .await?
            .into_inner();

        Ok(response.schemas)
    }

    async fn get_catalog(
        &mut self,
        catalog_id: String,
    ) -> Result<Option<proto::Catalog>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::GetCatalogRequest { id: catalog_id });

        let response = self
            .catalog_service_client
            .get_catalog(request)
            .await?
            .into_inner();

        Ok(response.catalog)
    }

    async fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let url = std::env::var("EXON_EXOME_URL")?;
        let url = url::Url::parse(&url)?;

        let token = std::env::var("EXON_EXOME_TOKEN")?;

        let organization_id = std::env::var("EXON_EXOME_ORGANIZATION_ID")?;

        let client = Self::connect(url.to_string(), token, organization_id).await?;

        Ok(client)
    }

    async fn health_check(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::HealthCheckRequest {});

        let response = self.catalog_service_client.health_check(request).await?;

        Ok(())
    }
}

struct Schema {
    inner: proto::Schema,
    catalog_service_client:
        proto::catalog_service_client::CatalogServiceClient<tonic::transport::Channel>,
    session_context: Arc<SessionContext>,
    tables: HashMap<String, proto::Table>,
}

// Create a debug implementation for Schema
impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Schema {
    pub async fn new(
        proto_schema: proto::Schema,
        session_context: Arc<SessionContext>,
        catalog_service_client: &mut proto::catalog_service_client::CatalogServiceClient<
            tonic::transport::Channel,
        >,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut s = Self {
            inner: proto_schema,
            catalog_service_client: catalog_service_client.clone(),
            session_context,
            tables: HashMap::new(),
        };

        s.refresh().await?;

        Ok(s)
    }

    pub async fn refresh(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::ListTablesRequest {
            schema_id: self.inner.id.clone(),
        });

        let response = self
            .catalog_service_client
            .list_tables(request)
            .await?
            .into_inner();

        self.tables.clear();

        for table in response.tables {
            self.tables.insert(table.name.clone(), table);
        }

        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for Schema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|k| k.clone()).collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let proto_table = self.tables.get(name)?;

        self.session_context
            .runtime_env()
            .exon_register_object_store_uri(&proto_table.location.clone())
            .await
            .unwrap();

        let table_path = ListingTableUrl::parse(proto_table.location.as_str()).unwrap();
        let file_compression_type = FileCompressionType::UNCOMPRESSED;

        let file_type = ExonFileType::from_str(&proto_table.file_format).unwrap();
        let file_format = file_type.get_file_format(file_compression_type);

        let lo = ListingOptions::new(file_format);
        let resolved_schema = lo
            .infer_schema(&self.session_context.state(), &table_path)
            .await
            .unwrap();

        let listing_table_config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(listing_table_config).unwrap();

        Some(Arc::new(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

// Test the client
#[tokio::test]
async fn test_exome_catalog_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ExomeCatalogClient::connect(
        "http://localhost:50051".to_string(),
        "".to_string(),
        "00000000-0000-0000-0000-000000000000".to_string(),
    )
    .await?;

    client.health_check().await?;

    let session = Arc::new(SessionContext::new_exon());

    // try to run a select statement from the table
    let catalog_name = "test_catalog";
    let schema_name = "test_schema";
    let table_name = "test_table";

    let library = client
        .get_library_by_name("test_library".to_string())
        .await
        .unwrap()
        .unwrap();

    let exome_catalogs = client.get_catalogs(library.id).await.unwrap();

    for catalog in exome_catalogs {
        let in_mem_catalog = MemoryCatalogProvider::new();

        let schemas = client.get_schemas(catalog.id).await.unwrap();

        for schema in schemas {
            let mut schema =
                Schema::new(schema, session.clone(), &mut client.catalog_service_client)
                    .await
                    .unwrap();

            schema.refresh().await.unwrap();

            in_mem_catalog
                .register_schema(schema_name, Arc::new(schema))
                .unwrap();
        }

        session.register_catalog(catalog_name, Arc::new(in_mem_catalog));
    }

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

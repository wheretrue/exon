mod schema;

use std::str::FromStr;

pub use schema::Schema;

use super::proto;

// Create a type alias for the catalog service client.
type CatalogServiceClient =
    proto::catalog_service_client::CatalogServiceClient<tonic::transport::Channel>;

/// ExomeCatalogClient is a client for interacting with the Exome Catalog service.
#[derive(Clone)]
pub struct ExomeCatalogClient {
    organization_id: String,
    catalog_service_client: CatalogServiceClient,
    token: String,
}

/// Implement Debug for ExomeCatalogClient.

impl ExomeCatalogClient {
    /// Connects to the Exome Catalog service at the given URL with the specified organization ID.
    /// Returns an instance of ExomeCatalogClient upon successful connection.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the Exome Catalog service.
    /// * `organization_id` - The organization ID associated with the client.
    ///
    /// # Returns
    ///
    /// An instance of ExomeCatalogClient on success, or a boxed error on failure.
    pub async fn connect(
        url: String,
        organization_id: String,
        token: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let catalog_service_client =
            proto::catalog_service_client::CatalogServiceClient::connect(url.clone()).await?;

        let s = Self {
            organization_id,
            catalog_service_client,
            token,
        };

        Ok(s)
    }

    /// Make a request to the Exome service.
    ///
    /// # Arguments
    ///
    /// * `request_body` - The request to make.
    fn make_request<T>(&self, request_body: T) -> Result<tonic::Request<T>, tonic::Status> {
        let mut request = tonic::Request::new(request_body);

        let metadata_value =
            tonic::metadata::MetadataValue::from_str(&self.token).map_err(|e| {
                tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    format!("Error creating metadata value: {}", e),
                )
            })?;

        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        Ok(request)
    }

    /// Retrieves a library by its name from the Exome Catalog service.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the library to retrieve.
    ///
    /// # Returns
    ///
    /// An option containing the retrieved library if found, or None if not found. Returns a boxed error on failure.
    pub async fn get_library_by_name(
        &mut self,
        name: String,
    ) -> Result<Option<proto::Library>, Box<dyn std::error::Error>> {
        let request = self.make_request(proto::GetLibraryByNameRequest {
            name,
            organization_id: self.organization_id.clone(),
        })?;

        let response = self
            .catalog_service_client
            .get_library_by_name(request)
            .await?
            .into_inner();

        Ok(response.library)
    }

    /// Retrieves a list of catalogs associated with a specific library from the Exome Catalog service.
    ///
    /// # Arguments
    ///
    /// * `library_id` - The ID of the library to retrieve catalogs for.
    ///
    /// # Returns
    ///
    /// A vector containing the retrieved catalogs on success, or a boxed error on failure.
    pub async fn get_catalogs(
        &mut self,
        library_id: String,
    ) -> Result<Vec<proto::Catalog>, Box<dyn std::error::Error>> {
        let request = self.make_request(proto::ListCatalogsRequest { library_id })?;

        let response = self
            .catalog_service_client
            .list_catalogs(request)
            .await?
            .into_inner();

        Ok(response.catalogs)
    }

    /// Retrieves a list of schemas associated with a specific catalog from the Exome Catalog service.
    ///
    /// # Arguments
    ///
    /// * `catalog_id` - The ID of the catalog to retrieve schemas for.
    ///
    /// # Returns
    ///
    /// A vector containing the retrieved schemas on success, or a boxed error on failure.
    pub async fn get_schemas(
        &mut self,
        catalog_id: String,
    ) -> Result<Vec<proto::Schema>, Box<dyn std::error::Error>> {
        let request = self.make_request(proto::ListSchemasRequest { catalog_id })?;

        let response = self
            .catalog_service_client
            .list_schemas(request)
            .await?
            .into_inner();

        Ok(response.schemas)
    }

    /// Retrieves a catalog by its ID from the Exome Catalog service.
    ///
    /// # Arguments
    ///
    /// * `catalog_id` - The ID of the catalog to retrieve.
    ///
    /// # Returns
    ///
    /// An option containing the retrieved catalog if found, or None if not found. Returns a boxed error on failure.
    #[allow(dead_code)]
    pub async fn get_catalog(
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

    /// Creates an instance of ExomeCatalogClient by reading configuration from environment variables.
    ///
    /// # Returns
    ///
    /// An instance of ExomeCatalogClient on success, or a boxed error on failure.
    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let url = std::env::var("EXON_EXOME_URL")?;
        let url = url::Url::parse(&url)?;

        let token = std::env::var("EXON_EXOME_TOKEN")?;

        let organization_id = std::env::var("EXON_EXOME_ORGANIZATION_ID")?;

        let client = Self::connect(url.to_string(), organization_id, token).await?;

        Ok(client)
    }

    /// Performs a health check on the Exome Catalog service to ensure it is responsive.
    ///
    /// # Returns
    ///
    /// An empty result on success, or a boxed error on failure.
    #[allow(dead_code)]
    pub async fn health_check(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::HealthCheckRequest {});

        let _ = self.catalog_service_client.health_check(request).await?;

        Ok(())
    }

    /// Lists the tables associated with a specific schema.
    pub async fn get_tables(
        &self,
        schema_id: String,
    ) -> Result<Vec<proto::Table>, Box<dyn std::error::Error>> {
        let request = self.make_request(proto::ListTablesRequest { schema_id })?;

        let mut client = self.catalog_service_client.clone();
        let response = client.list_tables(request).await?.into_inner();

        Ok(response.tables)
    }
}

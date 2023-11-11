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

mod schema;

pub use schema::Schema;

use crate::{
    error::ExomeError,
    exome_catalog_manager::{CatalogName, LibraryName, OrganizationName, SchemaName, TableName},
};

use super::proto::{self, health_client::HealthClient, HealthCheckRequest};

// Create a type alias for the catalog service client.
type CatalogServiceClient =
    proto::catalog_service_client::CatalogServiceClient<tonic::transport::Channel>;

/// ExomeCatalogClient is a client for interacting with the Exome Catalog service.
#[derive(Clone)]
pub struct ExomeCatalogClient {
    pub(crate) organization_name: String,
    pub(crate) catalog_service_client: CatalogServiceClient,
    pub(crate) token: String,
}

/// Implement Debug for ExomeCatalogClient.
impl ExomeCatalogClient {
    /// Connects to the Exome Catalog service at the given URL with the specified organization ID.
    /// Returns an instance of ExomeCatalogClient upon successful connection.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the Exome Catalog service.
    /// * `organization_name` - The organization name associated with the client.
    /// * `token` - The token to use for authentication.
    ///
    /// # Returns
    ///
    /// An instance of ExomeCatalogClient on success, or a boxed error on failure.
    pub async fn connect_with_tls(
        url: String,
        organization_name: String,
        token: String,
    ) -> Result<Self, ExomeError> {
        let tls = tonic::transport::ClientTlsConfig::new();

        let channel = tonic::transport::Channel::from_shared(url)
            .map_err(|e| {
                tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    format!("Error creating channel: {}", e),
                )
            })?
            .tls_config(tls)?
            .connect()
            .await?;

        let catalog_service_client =
            proto::catalog_service_client::CatalogServiceClient::new(channel.clone());

        let mut health_check_client = HealthClient::new(channel);
        let health_check_request = HealthCheckRequest {
            service: "exome".to_string(),
        };

        let _ = health_check_client.check(health_check_request).await?;

        let s = Self {
            organization_name,
            catalog_service_client,
            token,
        };

        Ok(s)
    }

    /// Connects to the Exome Catalog service at the given URL with the specified organization ID without TLS.
    /// Returns an instance of ExomeCatalogClient upon successful connection.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the Exome Catalog service.
    /// * `organization_name` - The organization ID associated with the client.
    /// * `token` - The token to use for authentication.
    ///
    /// # Returns
    ///
    /// An instance of ExomeCatalogClient on success, or a boxed error on failure.
    pub async fn connect(
        url: String,
        organization_name: String,
        token: String,
    ) -> Result<Self, ExomeError> {
        let channel = tonic::transport::Channel::from_shared(url)
            .map_err(|e| {
                tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    format!("Error creating channel: {}", e),
                )
            })?
            .connect()
            .await?;

        let catalog_service_client =
            proto::catalog_service_client::CatalogServiceClient::new(channel);

        let s = Self {
            organization_name,
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
    fn make_request<T>(&self, request_body: T) -> Result<tonic::Request<T>, ExomeError> {
        let mut request = tonic::Request::new(request_body);

        let metadata_value =
            tonic::metadata::MetadataValue::try_from(&self.token).map_err(|e| {
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
        organization_name: OrganizationName,
        library_name: LibraryName,
    ) -> Result<Vec<proto::Catalog>, ExomeError> {
        let request = self.make_request(proto::ListCatalogsRequest {
            organization_name: organization_name.to_string(),
            library_name: library_name.to_string(),
        })?;

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
        organization_name: String,
        library_name: String,
        catalog_name: String,
    ) -> Result<Vec<proto::Schema>, ExomeError> {
        let request = self.make_request(proto::ListSchemasRequest {
            organization_name,
            library_name,
            catalog_name,
        })?;

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
    pub async fn get_catalog(
        &mut self,
        name: String,
        library_name: String,
    ) -> Result<Option<proto::Catalog>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(proto::GetCatalogRequest {
            name,
            library_name,
            organization_name: self.organization_name.clone(),
        });

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
    pub async fn from_env() -> Result<Self, ExomeError> {
        let url = std::env::var("EXON_EXOME_URL")?;
        let url = url::Url::parse(&url).map_err(|e| {
            tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("Error parsing URL: {}", e),
            )
        })?;

        let token = std::env::var("EXON_EXOME_TOKEN")?;
        let organization_id = std::env::var("EXON_EXOME_ORGANIZATION_ID")?;

        let use_tls = std::env::var("EXON_EXOME_USE_TLS")?;

        if use_tls == "true" {
            Self::connect_with_tls(url.to_string(), organization_id, token).await
        } else {
            Self::connect(url.to_string(), organization_id, token).await
        }
    }

    /// Lists the tables associated with a specific schema.
    pub async fn get_tables(
        &self,
        schema_name: String,
        catalog_name: String,
        library_name: String,
    ) -> Result<Vec<proto::Table>, ExomeError> {
        let request = self.make_request(proto::ListTablesRequest {
            schema_name,
            catalog_name,
            library_name,
            organization_name: self.organization_name.clone(),
        })?;

        let mut client = self.catalog_service_client.clone();
        let response = client.list_tables(request).await?.into_inner();

        Ok(response.tables)
    }

    /// Create a catalog, returning its ID.
    pub async fn create_catalog(
        &self,
        name: CatalogName,
        library_name: LibraryName,
        organization_name: OrganizationName,
    ) -> Result<String, ExomeError> {
        let request = self.make_request(proto::CreateCatalogRequest {
            name: name.to_string(),
            library_name: library_name.to_string(),
            organization_name: organization_name.to_string(),
        })?;

        let mut client = self.catalog_service_client.clone();
        let response = client.create_catalog(request).await?.into_inner();

        Ok(response.name)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_table(
        &self,
        name: TableName,
        schema_name: SchemaName,
        catalog_name: CatalogName,
        library_name: LibraryName,
        location: String,
        file_format: String,
        is_listing: bool,
        compression_type: String,
        table_partition_cols: Vec<String>,
    ) -> Result<String, ExomeError> {
        let request = self.make_request(proto::CreateTableRequest {
            name: name.to_string(),
            schema_name: schema_name.to_string(),
            catalog_name: catalog_name.to_string(),
            library_name: library_name.to_string(),
            location,
            file_format,
            is_listing,
            compression_type,
            partition_cols: table_partition_cols,
        })?;

        let mut client = self.catalog_service_client.clone();
        let response = client.create_table(request).await?.into_inner();

        Ok(response.name)
    }

    /// Delete a catalog.
    pub async fn drop_catalog(&self, name: String, library_name: String) -> Result<(), ExomeError> {
        let request = self.make_request(proto::DropCatalogRequest { name, library_name })?;

        let mut client = self.catalog_service_client.clone();
        client.drop_catalog(request).await?;

        Ok(())
    }

    pub async fn create_schema(
        &self,
        name: SchemaName,
        catalog_name: CatalogName,
        library_name: LibraryName,
    ) -> Result<String, ExomeError> {
        let request = self.make_request(proto::CreateSchemaRequest {
            name: name.to_string(),
            catalog_name: catalog_name.to_string(),
            library_name: library_name.to_string(),
            organization_name: self.organization_name.clone(),
        })?;

        let mut client = self.catalog_service_client.clone();
        let response = client.create_schema(request).await?.into_inner();

        Ok(response.name)
    }
}

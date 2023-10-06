/// Auth
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTokenRequest {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub password: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTokenResponse {
    #[prost(string, tag = "1")]
    pub token: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunQueryRequest {
    #[prost(string, tag = "1")]
    pub query: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub library_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunQueryResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub ipc_response: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Organization {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub updated_at: ::prost::alloc::string::String,
}
/// Association between a use and an organization.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserOrganization {
    #[prost(message, optional, tag = "1")]
    pub organization: ::core::option::Option<Organization>,
    #[prost(string, tag = "2")]
    pub role: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrganizationRequest {
    #[prost(string, tag = "1")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOrganizationResponse {
    #[prost(message, optional, tag = "1")]
    pub organization: ::core::option::Option<Organization>,
}
/// User Organizations
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserOrganizationsRequest {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserOrganizationsResponse {
    #[prost(message, repeated, tag = "1")]
    pub organizations: ::prost::alloc::vec::Vec<UserOrganization>,
}
/// User
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct User {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub first_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub last_name: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub updated_at: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateUserRequest {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub first_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub last_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateUserResponse {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateUserAuthProviderRequest {
    #[prost(string, tag = "1")]
    pub auth_service: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub auth_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub user_email: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateUserAuthProviderResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserRequest {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserResponse {
    #[prost(message, optional, tag = "1")]
    pub user: ::core::option::Option<User>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserByEmailRequest {
    #[prost(string, tag = "1")]
    pub email: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserByEmailResponse {
    #[prost(message, optional, tag = "1")]
    pub user: ::core::option::Option<User>,
}
/// File Format
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListFileFormatsRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListFileFormatsResponse {
    #[prost(message, repeated, tag = "1")]
    pub file_formats: ::prost::alloc::vec::Vec<FileFormat>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileFormat {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub updated_at: ::prost::alloc::string::String,
}
/// Library
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Library {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub updated_at: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateLibraryRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateLibraryResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListLibrariesRequest {
    #[prost(string, tag = "1")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListLibrariesResponse {
    #[prost(message, repeated, tag = "1")]
    pub libraries: ::prost::alloc::vec::Vec<Library>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLibraryRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLibraryResponse {
    #[prost(message, optional, tag = "1")]
    pub library: ::core::option::Option<Library>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLibraryByNameRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLibraryByNameResponse {
    #[prost(message, optional, tag = "1")]
    pub library: ::core::option::Option<Library>,
}
/// Catalog
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Catalog {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub library_id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub updated_at: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub library_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogResponse {
    #[prost(message, optional, tag = "1")]
    pub catalog: ::core::option::Option<Catalog>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCatalogsRequest {
    #[prost(string, tag = "1")]
    pub library_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCatalogsResponse {
    #[prost(message, repeated, tag = "1")]
    pub catalogs: ::prost::alloc::vec::Vec<Catalog>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogByNameRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub library_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogByNameResponse {
    #[prost(message, optional, tag = "1")]
    pub catalog: ::core::option::Option<Catalog>,
}
/// Schema
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub authority: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub path: ::prost::alloc::string::String,
    #[prost(bool, tag = "6")]
    pub is_listing: bool,
    #[prost(string, tag = "7")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub updated_at: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub catalog_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSchemaRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub authority: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub path: ::prost::alloc::string::String,
    #[prost(bool, tag = "5")]
    pub is_listing: bool,
    #[prost(string, tag = "6")]
    pub catalog_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSchemaResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSchemasRequest {
    #[prost(string, tag = "1")]
    pub catalog_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSchemasResponse {
    #[prost(message, repeated, tag = "1")]
    pub schemas: ::prost::alloc::vec::Vec<Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSchemaRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSchemaResponse {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSchemaByNameRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub catalog_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSchemaByNameResponse {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<Schema>,
}
/// Table
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Table {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub location: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub schema_id: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub file_format: ::prost::alloc::string::String,
    #[prost(bool, tag = "7")]
    pub is_listing: bool,
    #[prost(string, tag = "8")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub updated_at: ::prost::alloc::string::String,
    #[prost(string, tag = "10")]
    pub compression_type_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTableRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub location: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub schema_id: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub file_format: ::prost::alloc::string::String,
    #[prost(bool, tag = "6")]
    pub is_listing: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTableResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableResponse {
    #[prost(message, optional, tag = "1")]
    pub table: ::core::option::Option<Table>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListTablesRequest {
    #[prost(string, tag = "1")]
    pub schema_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListTablesResponse {
    #[prost(message, repeated, tag = "1")]
    pub tables: ::prost::alloc::vec::Vec<Table>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableByNameRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub schema_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTableByNameResponse {
    #[prost(message, optional, tag = "1")]
    pub table: ::core::option::Option<Table>,
}
/// Audit Log
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserAuditLog {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub email: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub organization_id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub role: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub action: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub target_entity_type: ::prost::alloc::string::String,
    #[prost(string, tag = "7")]
    pub target_entity_id: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub created_at: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub updated_at: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserAuditLogRequest {
    #[prost(string, tag = "1")]
    pub organization_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetUserAuditLogResponse {
    #[prost(message, repeated, tag = "1")]
    pub logs: ::prost::alloc::vec::Vec<UserAuditLog>,
}
/// Generated client implementations.
pub mod catalog_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CatalogServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CatalogServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CatalogServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CatalogServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CatalogServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Library
        pub async fn create_library(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateLibraryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateLibraryResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateLibrary",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "CreateLibrary"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_libraries(
            &mut self,
            request: impl tonic::IntoRequest<super::ListLibrariesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListLibrariesResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/ListLibraries",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "ListLibraries"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_library(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLibraryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLibraryResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetLibrary",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetLibrary"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_library_by_name(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLibraryByNameRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLibraryByNameResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetLibraryByName",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetLibraryByName"));
            self.inner.unary(req, path, codec).await
        }
        /// Catalog
        pub async fn create_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateCatalogRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateCatalogResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateCatalog",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "CreateCatalog"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCatalogRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetCatalogResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetCatalog",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetCatalog"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_catalogs(
            &mut self,
            request: impl tonic::IntoRequest<super::ListCatalogsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListCatalogsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/ListCatalogs",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "ListCatalogs"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_catalog_by_name(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCatalogByNameRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetCatalogByNameResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetCatalogByName",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetCatalogByName"));
            self.inner.unary(req, path, codec).await
        }
        /// Schema
        pub async fn create_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSchemaRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSchemaResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateSchema",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "CreateSchema"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_schemas(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSchemasRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSchemasResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/ListSchemas",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "ListSchemas"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSchemaRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetSchemaResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetSchema",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetSchema"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_schema_by_name(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSchemaByNameRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetSchemaByNameResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetSchemaByName",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetSchemaByName"));
            self.inner.unary(req, path, codec).await
        }
        /// Table
        pub async fn create_table(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateTableRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateTableResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateTable",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "CreateTable"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_tables(
            &mut self,
            request: impl tonic::IntoRequest<super::ListTablesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListTablesResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/ListTables",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "ListTables"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_table(
            &mut self,
            request: impl tonic::IntoRequest<super::GetTableRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetTableResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetTable",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetTable"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_table_by_name(
            &mut self,
            request: impl tonic::IntoRequest<super::GetTableByNameRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetTableByNameResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetTableByName",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetTableByName"));
            self.inner.unary(req, path, codec).await
        }
        /// File Formats
        pub async fn list_file_formats(
            &mut self,
            request: impl tonic::IntoRequest<super::ListFileFormatsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListFileFormatsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/ListFileFormats",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "ListFileFormats"));
            self.inner.unary(req, path, codec).await
        }
        /// User information.
        pub async fn create_user(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateUserRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateUserResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateUser",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "CreateUser"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_user_auth_provider(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateUserAuthProviderRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateUserAuthProviderResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/CreateUserAuthProvider",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("exome.v1.CatalogService", "CreateUserAuthProvider"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_token(
            &mut self,
            request: impl tonic::IntoRequest<super::GetTokenRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetTokenResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetToken",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetToken"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_user(
            &mut self,
            request: impl tonic::IntoRequest<super::GetUserRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetUserResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetUser",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetUser"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_user_by_email(
            &mut self,
            request: impl tonic::IntoRequest<super::GetUserByEmailRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetUserByEmailResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetUserByEmail",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetUserByEmail"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_user_organizations(
            &mut self,
            request: impl tonic::IntoRequest<super::GetUserOrganizationsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetUserOrganizationsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetUserOrganizations",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("exome.v1.CatalogService", "GetUserOrganizations"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_organization(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOrganizationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetOrganizationResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetOrganization",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetOrganization"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_user_audit_log(
            &mut self,
            request: impl tonic::IntoRequest<super::GetUserAuditLogRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetUserAuditLogResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/GetUserAuditLog",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "GetUserAuditLog"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn run_query(
            &mut self,
            request: impl tonic::IntoRequest<super::RunQueryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RunQueryResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/exome.v1.CatalogService/RunQuery",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("exome.v1.CatalogService", "RunQuery"));
            self.inner.unary(req, path, codec).await
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    pub service: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckResponse {
    #[prost(enumeration = "health_check_response::ServingStatus", tag = "1")]
    pub status: i32,
}
/// Nested message and enum types in `HealthCheckResponse`.
pub mod health_check_response {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum ServingStatus {
        Unknown = 0,
        Serving = 1,
        NotServing = 2,
        /// Used only by the Watch method.
        ServiceUnknown = 3,
    }
    impl ServingStatus {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                ServingStatus::Unknown => "UNKNOWN",
                ServingStatus::Serving => "SERVING",
                ServingStatus::NotServing => "NOT_SERVING",
                ServingStatus::ServiceUnknown => "SERVICE_UNKNOWN",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "SERVING" => Some(Self::Serving),
                "NOT_SERVING" => Some(Self::NotServing),
                "SERVICE_UNKNOWN" => Some(Self::ServiceUnknown),
                _ => None,
            }
        }
    }
}
/// Generated client implementations.
pub mod health_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct HealthClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HealthClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> HealthClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> HealthClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            HealthClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn check(
            &mut self,
            request: impl tonic::IntoRequest<super::HealthCheckRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HealthCheckResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/exome.v1.Health/Check");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("exome.v1.Health", "Check"));
            self.inner.unary(req, path, codec).await
        }
    }
}

from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetTokenRequest(_message.Message):
    __slots__ = ["email", "password"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    email: str
    password: str
    def __init__(self, email: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class GetTokenResponse(_message.Message):
    __slots__ = ["token"]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: str
    def __init__(self, token: _Optional[str] = ...) -> None: ...

class RunQueryRequest(_message.Message):
    __slots__ = ["query", "library_id", "organization_id"]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    query: str
    library_id: str
    organization_id: str
    def __init__(self, query: _Optional[str] = ..., library_id: _Optional[str] = ..., organization_id: _Optional[str] = ...) -> None: ...

class RunQueryResponse(_message.Message):
    __slots__ = ["ipcResponse"]
    IPCRESPONSE_FIELD_NUMBER: _ClassVar[int]
    ipcResponse: bytes
    def __init__(self, ipcResponse: _Optional[bytes] = ...) -> None: ...

class Organization(_message.Message):
    __slots__ = ["id", "name", "created_at", "updated_at"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    created_at: str
    updated_at: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class GetOrganizationRequest(_message.Message):
    __slots__ = ["organization_id"]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    organization_id: str
    def __init__(self, organization_id: _Optional[str] = ...) -> None: ...

class GetOrganizationResponse(_message.Message):
    __slots__ = ["organization"]
    ORGANIZATION_FIELD_NUMBER: _ClassVar[int]
    organization: Organization
    def __init__(self, organization: _Optional[_Union[Organization, _Mapping]] = ...) -> None: ...

class GetUserOrganizationsRequest(_message.Message):
    __slots__ = ["email"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class GetUserOrganizationsResponse(_message.Message):
    __slots__ = ["organizations"]
    ORGANIZATIONS_FIELD_NUMBER: _ClassVar[int]
    organizations: _containers.RepeatedCompositeFieldContainer[Organization]
    def __init__(self, organizations: _Optional[_Iterable[_Union[Organization, _Mapping]]] = ...) -> None: ...

class User(_message.Message):
    __slots__ = ["email", "first_name", "last_name", "created_at", "updated_at"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    FIRST_NAME_FIELD_NUMBER: _ClassVar[int]
    LAST_NAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    email: str
    first_name: str
    last_name: str
    created_at: str
    updated_at: str
    def __init__(self, email: _Optional[str] = ..., first_name: _Optional[str] = ..., last_name: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class CreateUserRequest(_message.Message):
    __slots__ = ["email", "first_name", "last_name"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    FIRST_NAME_FIELD_NUMBER: _ClassVar[int]
    LAST_NAME_FIELD_NUMBER: _ClassVar[int]
    email: str
    first_name: str
    last_name: str
    def __init__(self, email: _Optional[str] = ..., first_name: _Optional[str] = ..., last_name: _Optional[str] = ...) -> None: ...

class CreateUserResponse(_message.Message):
    __slots__ = ["email"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class CreateUserAuthProviderRequest(_message.Message):
    __slots__ = ["auth_service", "auth_id", "user_email"]
    AUTH_SERVICE_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    USER_EMAIL_FIELD_NUMBER: _ClassVar[int]
    auth_service: str
    auth_id: str
    user_email: str
    def __init__(self, auth_service: _Optional[str] = ..., auth_id: _Optional[str] = ..., user_email: _Optional[str] = ...) -> None: ...

class CreateUserAuthProviderResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetUserRequest(_message.Message):
    __slots__ = ["email"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class GetUserResponse(_message.Message):
    __slots__ = ["user"]
    USER_FIELD_NUMBER: _ClassVar[int]
    user: User
    def __init__(self, user: _Optional[_Union[User, _Mapping]] = ...) -> None: ...

class GetUserByEmailRequest(_message.Message):
    __slots__ = ["email"]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    email: str
    def __init__(self, email: _Optional[str] = ...) -> None: ...

class GetUserByEmailResponse(_message.Message):
    __slots__ = ["user"]
    USER_FIELD_NUMBER: _ClassVar[int]
    user: User
    def __init__(self, user: _Optional[_Union[User, _Mapping]] = ...) -> None: ...

class ListFileFormatsRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListFileFormatsResponse(_message.Message):
    __slots__ = ["file_formats"]
    FILE_FORMATS_FIELD_NUMBER: _ClassVar[int]
    file_formats: _containers.RepeatedCompositeFieldContainer[FileFormat]
    def __init__(self, file_formats: _Optional[_Iterable[_Union[FileFormat, _Mapping]]] = ...) -> None: ...

class FileFormat(_message.Message):
    __slots__ = ["name", "created_at", "updated_at"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    name: str
    created_at: str
    updated_at: str
    def __init__(self, name: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class Library(_message.Message):
    __slots__ = ["id", "name", "description", "created_at", "updated_at"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    created_at: str
    updated_at: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class CreateLibraryRequest(_message.Message):
    __slots__ = ["name", "description", "organization_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    organization_id: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., organization_id: _Optional[str] = ...) -> None: ...

class CreateLibraryResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ListLibrariesRequest(_message.Message):
    __slots__ = ["organization_id"]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    organization_id: str
    def __init__(self, organization_id: _Optional[str] = ...) -> None: ...

class ListLibrariesResponse(_message.Message):
    __slots__ = ["libraries"]
    LIBRARIES_FIELD_NUMBER: _ClassVar[int]
    libraries: _containers.RepeatedCompositeFieldContainer[Library]
    def __init__(self, libraries: _Optional[_Iterable[_Union[Library, _Mapping]]] = ...) -> None: ...

class GetLibraryRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetLibraryResponse(_message.Message):
    __slots__ = ["library"]
    LIBRARY_FIELD_NUMBER: _ClassVar[int]
    library: Library
    def __init__(self, library: _Optional[_Union[Library, _Mapping]] = ...) -> None: ...

class GetLibraryByNameRequest(_message.Message):
    __slots__ = ["name", "organization_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    organization_id: str
    def __init__(self, name: _Optional[str] = ..., organization_id: _Optional[str] = ...) -> None: ...

class GetLibraryByNameResponse(_message.Message):
    __slots__ = ["library"]
    LIBRARY_FIELD_NUMBER: _ClassVar[int]
    library: Library
    def __init__(self, library: _Optional[_Union[Library, _Mapping]] = ...) -> None: ...

class Catalog(_message.Message):
    __slots__ = ["id", "name", "library_id", "created_at", "updated_at"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    library_id: str
    created_at: str
    updated_at: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., library_id: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class CreateCatalogRequest(_message.Message):
    __slots__ = ["name", "library_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    library_id: str
    def __init__(self, name: _Optional[str] = ..., library_id: _Optional[str] = ...) -> None: ...

class CreateCatalogResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetCatalogRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetCatalogResponse(_message.Message):
    __slots__ = ["catalog"]
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    catalog: Catalog
    def __init__(self, catalog: _Optional[_Union[Catalog, _Mapping]] = ...) -> None: ...

class ListCatalogsRequest(_message.Message):
    __slots__ = ["library_id"]
    LIBRARY_ID_FIELD_NUMBER: _ClassVar[int]
    library_id: str
    def __init__(self, library_id: _Optional[str] = ...) -> None: ...

class ListCatalogsResponse(_message.Message):
    __slots__ = ["catalogs"]
    CATALOGS_FIELD_NUMBER: _ClassVar[int]
    catalogs: _containers.RepeatedCompositeFieldContainer[Catalog]
    def __init__(self, catalogs: _Optional[_Iterable[_Union[Catalog, _Mapping]]] = ...) -> None: ...

class GetCatalogByNameRequest(_message.Message):
    __slots__ = ["name", "library_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    LIBRARY_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    library_id: str
    def __init__(self, name: _Optional[str] = ..., library_id: _Optional[str] = ...) -> None: ...

class GetCatalogByNameResponse(_message.Message):
    __slots__ = ["catalog"]
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    catalog: Catalog
    def __init__(self, catalog: _Optional[_Union[Catalog, _Mapping]] = ...) -> None: ...

class Schema(_message.Message):
    __slots__ = ["id", "name", "description", "authority", "path", "is_listing", "created_at", "updated_at", "catalog_id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    IS_LISTING_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    CATALOG_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    authority: str
    path: str
    is_listing: bool
    created_at: str
    updated_at: str
    catalog_id: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., authority: _Optional[str] = ..., path: _Optional[str] = ..., is_listing: bool = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ..., catalog_id: _Optional[str] = ...) -> None: ...

class CreateSchemaRequest(_message.Message):
    __slots__ = ["name", "description", "authority", "path", "is_listing", "catalog_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    IS_LISTING_FIELD_NUMBER: _ClassVar[int]
    CATALOG_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    authority: str
    path: str
    is_listing: bool
    catalog_id: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., authority: _Optional[str] = ..., path: _Optional[str] = ..., is_listing: bool = ..., catalog_id: _Optional[str] = ...) -> None: ...

class CreateSchemaResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ListSchemasRequest(_message.Message):
    __slots__ = ["catalog_id"]
    CATALOG_ID_FIELD_NUMBER: _ClassVar[int]
    catalog_id: str
    def __init__(self, catalog_id: _Optional[str] = ...) -> None: ...

class ListSchemasResponse(_message.Message):
    __slots__ = ["schemas"]
    SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    schemas: _containers.RepeatedCompositeFieldContainer[Schema]
    def __init__(self, schemas: _Optional[_Iterable[_Union[Schema, _Mapping]]] = ...) -> None: ...

class GetSchemaRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetSchemaResponse(_message.Message):
    __slots__ = ["schema"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...

class GetSchemaByNameRequest(_message.Message):
    __slots__ = ["name", "catalog_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATALOG_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    catalog_id: str
    def __init__(self, name: _Optional[str] = ..., catalog_id: _Optional[str] = ...) -> None: ...

class GetSchemaByNameResponse(_message.Message):
    __slots__ = ["schema"]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: Schema
    def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...

class Table(_message.Message):
    __slots__ = ["id", "name", "description", "location", "schema_id", "file_format", "is_listing", "created_at", "updated_at", "compression_type_id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_FORMAT_FIELD_NUMBER: _ClassVar[int]
    IS_LISTING_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    location: str
    schema_id: str
    file_format: str
    is_listing: bool
    created_at: str
    updated_at: str
    compression_type_id: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., location: _Optional[str] = ..., schema_id: _Optional[str] = ..., file_format: _Optional[str] = ..., is_listing: bool = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ..., compression_type_id: _Optional[str] = ...) -> None: ...

class CreateTableRequest(_message.Message):
    __slots__ = ["name", "description", "location", "schema_id", "file_format", "is_listing"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_FORMAT_FIELD_NUMBER: _ClassVar[int]
    IS_LISTING_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    location: str
    schema_id: str
    file_format: str
    is_listing: bool
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., location: _Optional[str] = ..., schema_id: _Optional[str] = ..., file_format: _Optional[str] = ..., is_listing: bool = ...) -> None: ...

class CreateTableResponse(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetTableRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetTableResponse(_message.Message):
    __slots__ = ["table"]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: Table
    def __init__(self, table: _Optional[_Union[Table, _Mapping]] = ...) -> None: ...

class ListTablesRequest(_message.Message):
    __slots__ = ["schema_id"]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    def __init__(self, schema_id: _Optional[str] = ...) -> None: ...

class ListTablesResponse(_message.Message):
    __slots__ = ["tables"]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[Table]
    def __init__(self, tables: _Optional[_Iterable[_Union[Table, _Mapping]]] = ...) -> None: ...

class GetTableByNameRequest(_message.Message):
    __slots__ = ["name", "schema_id"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    name: str
    schema_id: str
    def __init__(self, name: _Optional[str] = ..., schema_id: _Optional[str] = ...) -> None: ...

class GetTableByNameResponse(_message.Message):
    __slots__ = ["table"]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: Table
    def __init__(self, table: _Optional[_Union[Table, _Mapping]] = ...) -> None: ...

class UserAuditLog(_message.Message):
    __slots__ = ["id", "email", "organization_id", "role", "action", "target_entity_type", "target_entity_id", "created_at", "updated_at"]
    ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    TARGET_ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    email: str
    organization_id: str
    role: str
    action: str
    target_entity_type: str
    target_entity_id: str
    created_at: str
    updated_at: str
    def __init__(self, id: _Optional[str] = ..., email: _Optional[str] = ..., organization_id: _Optional[str] = ..., role: _Optional[str] = ..., action: _Optional[str] = ..., target_entity_type: _Optional[str] = ..., target_entity_id: _Optional[str] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class GetUserAuditLogRequest(_message.Message):
    __slots__ = ["organization_id"]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    organization_id: str
    def __init__(self, organization_id: _Optional[str] = ...) -> None: ...

class GetUserAuditLogResponse(_message.Message):
    __slots__ = ["logs"]
    LOGS_FIELD_NUMBER: _ClassVar[int]
    logs: _containers.RepeatedCompositeFieldContainer[UserAuditLog]
    def __init__(self, logs: _Optional[_Iterable[_Union[UserAuditLog, _Mapping]]] = ...) -> None: ...

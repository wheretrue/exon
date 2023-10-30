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

use std::fmt;

use crate::exome::ExomeCatalogClient;

macro_rules! impl_display_for {
    ($($t:ty),+) => {
        $(
            impl fmt::Display for $t {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        )+
    };
}

#[derive(Debug, Clone)]
pub struct TableName(pub String);

#[derive(Debug, Clone)]
pub struct SchemaName(pub String);

#[derive(Debug, Clone)]
pub struct CatalogName(pub String);

#[derive(Debug, Clone)]
pub struct LibraryName(pub String);

#[derive(Debug, Clone)]
pub struct OrganizationName(pub String);

impl_display_for!(
    SchemaName,
    CatalogName,
    LibraryName,
    OrganizationName,
    TableName
);

/// ExomeCatalogManager is a manager for the Exome Catalog service.

/// CreateCatalog is a change to create a catalog.
pub struct CreateCatalog {
    name: CatalogName,
    library_name: LibraryName,
}

impl CreateCatalog {
    pub fn new(name: String, library_name: String) -> Self {
        Self {
            name: CatalogName(name),
            library_name: LibraryName(library_name),
        }
    }
}

/// CreateSchema is a change to create a schema.
pub struct CreateSchema {
    name: SchemaName,
    catalog_name: CatalogName,
    library_name: LibraryName,
}

impl CreateSchema {
    pub fn new(name: SchemaName, catalog_name: CatalogName, library_name: LibraryName) -> Self {
        Self {
            name,
            catalog_name,
            library_name,
        }
    }
}

pub struct CreateTable {
    name: TableName,
    schema_name: SchemaName,
    catalog_name: CatalogName,
    library_name: LibraryName,
    location: String,
    file_format: String,
    is_listing: bool,
    compression_type: String,
}

impl CreateTable {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: TableName,
        schema_name: SchemaName,
        catalog_name: CatalogName,
        library_name: LibraryName,
        location: String,
        file_format: String,
        is_listing: bool,
        compression_type: String,
    ) -> Self {
        Self {
            name,
            schema_name,
            catalog_name,
            library_name,
            location,
            file_format,
            is_listing,
            compression_type,
        }
    }
}

pub struct DropCatalog {
    name: String,
    library_id: String,
}

/// Change is a change to apply to the Exome Catalog service.
pub enum Change {
    CreateCatalog(CreateCatalog),
    DropCatalog(DropCatalog),
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
}

/// ExomeCatalogManager is a manager for the Exome Catalog service.
///
/// It is used as an extension to Exon.
pub struct ExomeCatalogManager {
    pub(crate) client: ExomeCatalogClient,
}

impl ExomeCatalogManager {
    pub fn new(client: ExomeCatalogClient) -> Self {
        Self { client }
    }

    pub async fn apply_changes(
        &self,
        change_set: impl IntoIterator<Item = Change>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for change in change_set {
            match change {
                Change::CreateCatalog(create_catalog) => {
                    self.client
                        .create_catalog(
                            create_catalog.name,
                            create_catalog.library_name,
                            OrganizationName(self.client.organization_name.clone()),
                        )
                        .await?;
                }
                Change::DropCatalog(drop_catalog) => {
                    self.client
                        .drop_catalog(drop_catalog.name, drop_catalog.library_id)
                        .await?;
                }
                Change::CreateTable(create_table) => {
                    self.client
                        .create_table(
                            create_table.name,
                            create_table.schema_name,
                            create_table.catalog_name,
                            create_table.library_name,
                            create_table.location,
                            create_table.file_format,
                            create_table.is_listing,
                            create_table.compression_type,
                        )
                        .await?;
                }
                Change::CreateSchema(create_schema) => {
                    self.client
                        .create_schema(
                            create_schema.name,
                            create_schema.catalog_name,
                            create_schema.library_name,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}

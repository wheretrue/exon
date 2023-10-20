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

use crate::exome::ExomeCatalogClient;

/// ExomeCatalogManager is a manager for the Exome Catalog service.

/// CreateCatalog is a change to create a catalog.
pub struct CreateCatalog {
    name: String,
    library_id: String,
}

impl CreateCatalog {
    pub fn new(name: String, library_id: String) -> Self {
        Self { name, library_id }
    }
}

/// CreateSchema is a change to create a schema.
pub struct CreateSchema {
    name: String,
    description: String,
    authority: String,
    path: String,
    is_listing: bool,
    catalog_id: String,
}

/// Change is a change to apply to the Exome Catalog service.
pub enum Change {
    CreateCatalog(CreateCatalog),
    CreateSchema(CreateSchema),
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
                        .create_catalog(create_catalog.name, create_catalog.library_id)
                        .await?;
                }
                Change::CreateSchema(create_schema) => {
                    self.client
                        .create_schema(
                            create_schema.name,
                            create_schema.description,
                            create_schema.authority,
                            create_schema.path,
                            create_schema.is_listing,
                            create_schema.catalog_id,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}

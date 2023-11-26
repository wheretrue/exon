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

use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::{file_format::file_compression_type::FileCompressionType, TableProvider},
    prelude::SessionContext,
};
use exon::{
    datasources::{ExonFileType, ExonListingTableFactory},
    ExonRuntimeEnvExt,
};

use crate::{error::ExomeResult, exome::proto};

use super::ExomeCatalogClient;

pub struct Schema {
    inner: proto::Schema,
    exome_client: ExomeCatalogClient,
    session_context: Arc<SessionContext>,
    tables: HashMap<String, proto::Table>,
}

// Create a debug implementation for Schema
impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema")
            .field("inner", &self.inner)
            .field("tables", &self.tables)
            .finish()
    }
}

impl Schema {
    pub async fn new(
        proto_schema: proto::Schema,
        session_context: Arc<SessionContext>,
        client: ExomeCatalogClient,
    ) -> ExomeResult<Self> {
        let mut s = Self {
            inner: proto_schema,
            exome_client: client,
            session_context,
            tables: HashMap::new(),
        };

        s.refresh().await?;

        Ok(s)
    }

    pub async fn refresh(&mut self) -> ExomeResult<()> {
        let library_name = self.inner.library_name.clone();
        let catalog_name = self.inner.catalog_name.clone();
        let schema_name = self.inner.name.clone();
        let organization_name = self.inner.organization_name.clone();

        let tables = self
            .exome_client
            .get_tables(schema_name, catalog_name, library_name, organization_name)
            .await?;

        self.tables.clear();

        for table in tables {
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
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let proto_table = self.tables.get(name)?;

        self.session_context
            .runtime_env()
            .exon_register_object_store_uri(&proto_table.location.clone())
            .await
            .unwrap();

        let file_compression_type =
            FileCompressionType::from_str(&proto_table.compression_type).unwrap();

        let file_type = ExonFileType::from_str(&proto_table.file_format).unwrap();

        let table_provider_factory = ExonListingTableFactory {};
        let table_provider = table_provider_factory
            .create_from_file_type(
                &self.session_context.state(),
                file_type,
                file_compression_type,
                proto_table.location.clone(),
                proto_table
                    .partition_cols
                    .iter()
                    .map(|s| Field::new(s, DataType::Utf8, true))
                    .collect(),
            )
            .await
            .unwrap();

        Some(table_provider)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

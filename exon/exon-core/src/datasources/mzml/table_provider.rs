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

use std::{any::Any, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_mzml::MzMLSchemaBuilder;
use futures::TryStreamExt;

use crate::{
    datasources::{
        exon_listing_table_options::{ExonListingConfig, ExonListingOptions},
        hive_partition::filter_matches_partition_cols,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::MzMLScan;

#[derive(Debug, Clone)]
/// Listing options for a MzML table
pub struct ListingMzMLTableOptions {
    /// The file extension for the table
    file_extension: String,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// The table partition columns
    table_partition_cols: Vec<Field>,
}

impl Default for ListingMzMLTableOptions {
    /// Create a new set of options
    fn default() -> Self {
        Self::new(FileCompressionType::UNCOMPRESSED)
    }
}

#[async_trait]
impl ExonListingOptions for ListingMzMLTableOptions {
    fn table_partition_cols(&self) -> &[Field] {
        &self.table_partition_cols
    }

    fn file_extension(&self) -> &str {
        &self.file_extension
    }

    fn file_compression_type(&self) -> FileCompressionType {
        self.file_compression_type
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = MzMLScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

impl ListingMzMLTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::MZML.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
        }
    }

    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Infer the schema for the table (i.e. the file and partition columns)
    pub async fn infer_schema(&self) -> datafusion::error::Result<TableSchema> {
        let mut schema_builder = MzMLSchemaBuilder::default();
        schema_builder.add_partition_fields(self.table_partition_cols.clone());

        let table_schema = schema_builder.build();

        Ok(table_schema)
    }
}

#[derive(Debug, Clone)]
/// A MzML listing table
pub struct ListingMzMLTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingMzMLTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonListingOptions + 'static> TableProvider for ListingMzMLTable<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| filter_matches_partition_cols(f, self.config.options.table_partition_cols()))
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let url = if let Some(url) = self.config.inner.table_paths.first() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.config.inner.table_paths[0],
            filters,
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config =
            FileScanConfigBuilder::new(url.object_store(), file_schema, vec![file_list])
                .projection_option(projection.cloned())
                .table_partition_cols(self.config.options.table_partition_cols().to_vec())
                .limit_option(limit)
                .build();

        let plan = self
            .config
            .options
            .create_physical_plan(file_scan_config)
            .await?;

        Ok(plan)
    }
}

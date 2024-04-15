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
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_bed::BEDSchemaBuilder;
use exon_common::TableSchema;
use futures::TryStreamExt;

use super::BEDScan;

#[derive(Debug, Clone)]
/// Listing options for a BED table
pub struct ListingBEDTableOptions {
    /// The file extension, including the compression type
    file_extension: String,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,
}

impl ExonListingOptions for ListingBEDTableOptions {
    fn table_partition_cols(&self) -> Vec<Field> {
        self.table_partition_cols()
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
        let scan = BEDScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

impl Default for ListingBEDTableOptions {
    fn default() -> Self {
        Self::new(FileCompressionType::UNCOMPRESSED)
    }
}

impl ListingBEDTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::BED.get_file_extension(file_compression_type);

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

    /// Infer the schema for the table
    pub fn infer_schema(&self) -> datafusion::error::Result<TableSchema> {
        let mut schema_builder = BEDSchemaBuilder::default();
        schema_builder.add_partition_fields(self.table_partition_cols.clone());

        Ok(schema_builder.build())
    }
}

#[derive(Debug, Clone)]
/// A BED listing table
pub struct ListingBEDTable<T: Send + ExonListingOptions> {
    config: ExonListingConfig<T>,

    table_schema: TableSchema,
}

impl<T: Send + ExonListingOptions> ListingBEDTable<T> {
    /// Create a new VCF listing table
    pub fn try_new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            config,
            table_schema,
        })
    }
}

#[async_trait]
impl<T: Send + Sync + ExonListingOptions> TableProvider for ListingBEDTable<T> {
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
            .map(|f| filter_matches_partition_cols(f, &self.config.options.table_partition_cols()))
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let first_path = if let Some(url) = self.config.inner.table_paths.first() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store_url = ObjectStoreUrl::parse(&first_path)?;
        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &first_path,
            filters,
            self.config.options.file_extension(),
            &self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url.clone(), file_schema, vec![file_list])
                .projection_option(projection.cloned())
                .limit_option(limit)
                .table_partition_cols(self.config.options.table_partition_cols())
                .build();

        let plan = self
            .config
            .options
            .create_physical_plan(file_scan_config)
            .await?;

        Ok(plan)
    }
}

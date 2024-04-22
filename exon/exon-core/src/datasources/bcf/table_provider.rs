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
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        physical_plan::FileScanConfig, TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use futures::TryStreamExt;
use noodles::{bcf, core::Region};
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use crate::{
    datasources::{
        exon_listing_table_options::{
            ExonIndexedListingOptions, ExonListingConfig, ExonListingOptions,
        },
        hive_partition::filter_matches_partition_cols,
        vcf::VCFSchemaBuilder,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::BCFScan;

#[derive(Debug, Clone)]
/// Listing options for a BCF table
pub struct ListingBCFTableOptions {
    file_extension: String,

    regions: Vec<Region>,

    table_partition_cols: Vec<Field>,
}

impl Default for ListingBCFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: ExonFileType::BCF.get_file_extension(FileCompressionType::UNCOMPRESSED),
            regions: Vec::new(),
            table_partition_cols: Vec::new(),
        }
    }
}

impl ListingBCFTableOptions {
    /// Set the region for the table options. This is used to filter the records
    pub fn with_regions(mut self, regions: Vec<Region>) -> Self {
        self.regions = regions;
        self
    }

    /// Set the file extension for the table options
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema<'a>(
        &self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> datafusion::error::Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let get_result = if table_path.to_string().ends_with('/') {
            let list = store.list(Some(table_path.prefix()));
            let collected_list = list.try_collect::<Vec<_>>().await?;
            let first = collected_list
                .first()
                .ok_or_else(|| DataFusionError::Execution("No files found".to_string()))?;

            store.get(&first.location).await?
        } else {
            store.get(table_path.prefix()).await?
        };

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let mut bcf_reader = bcf::AsyncReader::new(stream_reader);
        let header = bcf_reader.read_header().await?;

        let mut schema_builder = VCFSchemaBuilder::default()
            .with_header(header)
            .with_parse_formats(true)
            .with_parse_info(true)
            .with_partition_fields(self.table_partition_cols.clone());

        let table_schema = schema_builder.build()?;

        Ok(table_schema)
    }
}

#[async_trait]
impl ExonListingOptions for ListingBCFTableOptions {
    fn table_partition_cols(&self) -> &[Field] {
        &self.table_partition_cols
    }

    fn file_extension(&self) -> &str {
        &self.file_extension
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BCFScan::new(conf.clone());
        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingBCFTableOptions {
    fn indexed(&self) -> bool {
        !self.regions.is_empty()
    }

    fn regions(&self) -> &[Region] {
        &self.regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        region: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if region.is_empty() {
            return Err(DataFusionError::Execution(
                "No regions provided for indexed scan".to_string(),
            ));
        }

        if region.len() > 1 {
            return Err(DataFusionError::Execution(
                "Multiple regions provided for indexed scan".to_string(),
            ));
        }

        let mut scan = BCFScan::new(conf.clone());

        if let Some(region) = region.first() {
            scan = scan.with_region_filter(region.clone());
        }

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BCF listing table
pub struct ListingBCFTable<T> {
    /// The schema for the table
    table_schema: TableSchema,

    /// The options for the table
    config: ExonListingConfig<T>,
}

impl<T> ListingBCFTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonIndexedListingOptions + 'static> TableProvider for ListingBCFTable<T> {
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
        let url = if let Some(url) = self.config.first_table_path() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state
            .runtime_env()
            .object_store(url.object_store().clone())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            url,
            filters,
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_scan_config = FileScanConfigBuilder::new(
            url.object_store(),
            self.table_schema.file_schema()?,
            vec![file_list],
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.config.options.table_partition_cols().to_vec())
        .limit_option(limit)
        .build();

        if let Some(region) = self.config.options.regions().first() {
            let plan = self
                .config
                .options
                .create_physical_plan_with_regions(file_scan_config, vec![region.clone()])
                .await?;

            return Ok(plan);
        }

        let plan = self
            .config
            .options
            .create_physical_plan(file_scan_config)
            .await?;

        Ok(plan)
    }
}

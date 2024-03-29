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
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
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
        hive_partition::filter_matches_partition_cols, vcf::VCFSchemaBuilder, ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::BCFScan;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBCFTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingBCFTableOptions>,
}

impl ListingBCFTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingBCFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a BCF table
pub struct ListingBCFTableOptions {
    file_extension: String,

    region: Option<Region>,

    table_partition_cols: Vec<Field>,
}

impl Default for ListingBCFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: ExonFileType::BCF.get_file_extension(FileCompressionType::UNCOMPRESSED),
            region: None,
            table_partition_cols: Vec::new(),
        }
    }
}

impl ListingBCFTableOptions {
    /// Set the region for the table options. This is used to filter the records
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
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
        bcf_reader.read_file_format().await?;

        let header_str = bcf_reader.read_header().await?;
        let header = header_str
            .parse::<noodles::vcf::Header>()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let mut schema_builder = VCFSchemaBuilder::default()
            .with_header(header)
            .with_parse_formats(true)
            .with_parse_info(true)
            .with_partition_fields(self.table_partition_cols.clone());

        let table_schema = schema_builder.build()?;

        Ok(table_schema)
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut scan = BCFScan::new(conf.clone());

        if let Some(region_filter) = &self.region {
            scan = scan.with_region_filter(region_filter.clone());
        }

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BCF listing table
pub struct ListingBCFTable {
    /// The paths to the files
    table_paths: Vec<ListingTableUrl>,

    /// The schema for the table
    table_schema: TableSchema,

    /// The options for the table
    options: ListingBCFTableOptions,
}

impl ListingBCFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingBCFTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }
}

#[async_trait]
impl TableProvider for ListingBCFTable {
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
            .map(|f| filter_matches_partition_cols(f, &self.options.table_partition_cols))
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store_url = if let Some(url) = self.table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.table_paths[0],
            filters,
            self.options.file_extension.as_str(),
            &self.options.table_partition_cols,
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            self.table_schema.file_schema()?,
            vec![file_list],
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.options.table_partition_cols.clone())
        .limit_option(limit)
        .build();

        let plan = self
            .options
            .create_physical_plan(state, file_scan_config)
            .await?;

        Ok(plan)
    }
}

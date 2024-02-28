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

use std::{any::Any, fmt::Debug, sync::Arc};

use crate::{
    config::ExonConfigExtension,
    datasources::{
        hive_partition::filter_matches_partition_cols, indexed_file::fai::compute_fai_range,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};
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
use exon_fasta::FASTASchemaBuilder;
use futures::TryStreamExt;
use noodles::{core::Region, fasta::fai::Reader};
use object_store::path::Path;

use super::{indexed_scanner::IndexedFASTAScanner, FASTAScan};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingFASTATableConfig {
    inner: ListingTableConfig,

    options: Option<ListingFASTATableOptions>,
}

impl ListingFASTATableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingFASTATableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a FASTA table
pub struct ListingFASTATableOptions {
    /// The file extension for the table
    file_extension: String,

    /// The file compression type for the table
    file_compression_type: FileCompressionType,

    /// The partition columns for the table
    table_partition_cols: Vec<Field>,

    /// A region to optionally filter the table
    region: Option<Region>,
}

impl ListingFASTATableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::FASTA.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
            region: None,
        }
    }

    /// Set the region
    pub fn with_region(self, region: Region) -> Self {
        Self {
            region: Some(region),
            ..self
        }
    }

    /// Infer the base schema for the table
    pub async fn infer_schema(
        &self,
        state: &SessionState,
    ) -> datafusion::error::Result<TableSchema> {
        let exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(DataFusionError::Execution(
                "Exon settings must be configured.".to_string(),
            ))?;

        let mut fasta_schema_builder = FASTASchemaBuilder::default()
            .with_large_utf8(exon_settings.fasta_large_utf8)
            .with_partition_fields(self.table_partition_cols.clone());

        Ok(fasta_schema_builder.build())
    }

    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Set the file extension for the table
    pub fn with_file_extension(self, file_extension: String) -> Self {
        Self {
            file_extension,
            ..self
        }
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(DataFusionError::Execution(
                "Exon settings must be configured.".to_string(),
            ))?;

        let fasta_sequence_buffer_capacity = exon_settings.fasta_sequence_buffer_capacity;

        if self.region.is_some() {
            let scan = IndexedFASTAScanner::new(conf.clone(), fasta_sequence_buffer_capacity);

            Ok(Arc::new(scan))
        } else {
            let scan = FASTAScan::new(
                conf.clone(),
                self.file_compression_type,
                fasta_sequence_buffer_capacity,
            );

            Ok(Arc::new(scan))
        }
    }
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingFASTATable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingFASTATableOptions,
}

impl ListingFASTATable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingFASTATableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }

    fn resolve_region(&self, filters: &[Expr]) -> Result<Option<Region>> {
        let region = filters.iter().find_map(|f| match f {
            Expr::ScalarFunction(s) => {
                infer_region::infer_region_from_udf(s, "fasta_region_filter")
            }
            _ => None,
        });

        match &self.options.region {
            Some(region) => Ok(Some(region.clone())),
            None => {
                if let Some(region) = region {
                    Ok(Some(region))
                } else {
                    Ok(None)
                }
            }
        }

        // Ok(region)
    }
}

#[async_trait]
impl TableProvider for ListingFASTATable {
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
        let f = filters
            .iter()
            .map(|f| match f {
                Expr::ScalarFunction(s) if s.name() == "fasta_region_filter" => {
                    TableProviderFilterPushDown::Exact
                }
                _ => filter_matches_partition_cols(f, &self.options.table_partition_cols),
            })
            .collect();

        Ok(f)
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

        let region = self.resolve_region(filters)?;

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

        if let Some(region) = &region {
            // If there was a region, we need to create a set of file partitions augmented with the
            // byte offsets of the region in the file
            let mut file_partitions = Vec::new();

            for file in file_list {
                let file_name = file.clone().object_meta.location;
                // Add the .fai extension to the end of the file name
                let index_file_path = Path::from(format!("{}.fai", file_name));

                let index_bytes = object_store.get(&index_file_path).await?.bytes().await?;
                let cursor = std::io::Cursor::new(index_bytes);

                let index_records = Reader::new(cursor).read_index()?;

                // TODO: coalesce the regions into contiguous blocks
                for index_record in index_records {
                    if let Some(range) = compute_fai_range(region, &index_record) {
                        let mut indexed_partition = file.clone();
                        indexed_partition.extensions = Some(Arc::new(range));
                        file_partitions.push(indexed_partition);
                    }
                }
            }

            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
                vec![file_partitions],
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
        } else {
            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
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
}

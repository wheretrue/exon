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

use std::{any::Any, fmt::Debug, str::FromStr, sync::Arc, vec};

use crate::{
    config::ExonConfigExtension,
    datasources::{
        hive_partition::filter_matches_partition_cols, indexed_file::fai::compute_fai_range,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder,
        infer_region,
        object_store::{parse_url, pruned_partition_list, url_to_object_store_url},
    },
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::GetExt,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
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
use object_store::{path::Path, ObjectStore};

use super::{indexed_scanner::IndexedFASTAScanner, FASTAScan};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingFASTATableConfig {
    inner: ListingTableConfig,
    options: ListingFASTATableOptions,
}

impl ListingFASTATableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl, options: ListingFASTATableOptions) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
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

    /// The region to read from
    region: Option<Vec<Region>>,

    /// The region file to read from
    region_file: Option<String>,
}

impl Default for ListingFASTATableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from(".fasta"),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
            region: None,
            region_file: None,
        }
    }
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
            region_file: None,
        }
    }

    /// Get the extension when accounting for the compression type
    pub fn file_extension(&self) -> String {
        if self
            .file_extension
            .ends_with(&self.file_compression_type.get_ext())
        {
            self.file_extension.clone()
        } else {
            format!(
                "{}{}",
                self.file_extension,
                self.file_compression_type.get_ext()
            )
        }
    }

    /// Set the region for the table
    pub fn with_region(self, region: Vec<Region>) -> Self {
        Self {
            region: Some(region),
            ..self
        }
    }

    /// Set the region file for the table
    pub fn with_region_file(self, region_file: String) -> Self {
        Self {
            region_file: Some(region_file),
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
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingFASTATable {
    table_schema: TableSchema,

    config: ListingFASTATableConfig,
}

impl ListingFASTATable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingFASTATableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_schema,
            config,
        })
    }

    async fn resolve_region<'a>(
        &self,
        filters: &[Expr],
        session_context: &'a SessionState,
    ) -> Result<Option<Vec<Region>>> {
        let region = filters.iter().find_map(|f| match f {
            Expr::ScalarFunction(s) => {
                infer_region::infer_region_from_udf(s, "fasta_region_filter")
            }
            _ => None,
        });

        let attached_regions = match &self.config.options.region {
            Some(region) => Ok::<_, DataFusionError>(Some(region.to_vec())),
            None => {
                if let Some(region) = region {
                    Ok(Some(vec![region]))
                } else {
                    Ok(None)
                }
            }
        }?;

        let regions_from_file = if let Some(region_file) = &self.config.options.region_file {
            let region_url = parse_url(region_file)?;
            let object_store_url = url_to_object_store_url(&region_url)?;

            let object_store = session_context
                .runtime_env()
                .object_store(object_store_url)?;

            let region_bytes = object_store
                .get(&Path::from_url_path(region_url.path())?)
                .await?
                .bytes()
                .await?;

            // iterate through the lines of the region file and parse them into regions, assume one region per line
            let regions = std::str::from_utf8(&region_bytes)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error parsing region file: {}", e))
                })?
                .lines()
                .map(|line| {
                    // Strip any whitespace from the line
                    let line = line.trim();

                    let region = Region::from_str(line).unwrap();

                    Ok(region)
                })
                .collect::<Result<Vec<_>>>()?;

            Some(regions)
        } else {
            None
        };

        Ok(match (attached_regions, regions_from_file) {
            (Some(attached_regions), Some(regions_from_file)) => {
                let concatenated_regions = [attached_regions, regions_from_file].concat();

                Some(concatenated_regions)
            }
            (Some(attached_regions), None) => Some(attached_regions),
            (None, Some(regions_from_file)) => Some(regions_from_file),
            (None, None) => None,
        })
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
                _ => filter_matches_partition_cols(f, &self.config.options.table_partition_cols),
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
        let object_store_url = if let Some(url) = self.config.inner.table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let regions = self.resolve_region(filters, state).await?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.config.inner.table_paths[0],
            filters,
            &self.config.options.file_extension(),
            &self.config.options.table_partition_cols,
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(DataFusionError::Execution(
                "Exon settings must be configured.".to_string(),
            ))?;

        let fasta_sequence_buffer_capacity = exon_settings.fasta_sequence_buffer_capacity;

        if let Some(regions) = &regions {
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
                    for region in regions {
                        if let Some(range) = compute_fai_range(region, &index_record) {
                            let mut indexed_partition = file.clone();
                            indexed_partition.extensions = Some(Arc::new(range));
                            file_partitions.push(indexed_partition);
                        }
                    }
                }
            }

            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
                vec![file_partitions],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.config.options.table_partition_cols.clone())
            .limit_option(limit)
            .build();

            let scan =
                IndexedFASTAScanner::new(file_scan_config.clone(), fasta_sequence_buffer_capacity);

            Ok(Arc::new(scan))
        } else {
            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
                vec![file_list],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.config.options.table_partition_cols.clone())
            .limit_option(limit)
            .build();

            let scan = FASTAScan::new(
                file_scan_config,
                self.config.options.file_compression_type,
                fasta_sequence_buffer_capacity,
            );

            Ok(Arc::new(scan))
        }
    }
}

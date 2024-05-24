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

use std::{any::Any, fmt::Debug, sync::Arc, vec};

use crate::{
    config::ExonConfigExtension,
    datasources::{
        exon_file_type::get_file_extension_with_compression,
        exon_listing_table_options::{
            ExonFileIndexedListingOptions, ExonIndexedListingOptions, ExonListingConfig,
            ExonListingOptions, ExonSequenceDataTypeOptions,
        },
        hive_partition::filter_matches_partition_cols,
        indexed_file::{fai::compute_fai_range, region::RegionObjectStoreExtension},
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
    datasource::{file_format::file_compression_type::FileCompressionType, TableProvider},
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_fasta::{FASTASchemaBuilder, SequenceDataType};
use futures::TryStreamExt;
use noodles::{core::Region, fasta::fai::Reader};
use object_store::{path::Path, ObjectStore};

use super::{indexed_scanner::IndexedFASTAScanner, FASTAScan};

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
    regions: Vec<Region>,

    /// The region file to read from
    region_file: Option<String>,

    /// The sequence data type for the table
    sequence_data_type: SequenceDataType,
}

#[async_trait]
impl ExonSequenceDataTypeOptions for ListingFASTATableOptions {
    fn sequence_data_type(&self) -> &SequenceDataType {
        &self.sequence_data_type
    }
}

#[async_trait]
impl ExonListingOptions for ListingFASTATableOptions {
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
        conf: datafusion::datasource::physical_plan::FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = FASTAScan::new(
            conf,
            self.file_compression_type(),
            2000,
            self.sequence_data_type.clone(),
        );

        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingFASTATableOptions {
    fn indexed(&self) -> bool {
        !self.regions.is_empty() || self.region_file.is_some()
    }

    fn regions(&self) -> &[Region] {
        &self.regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: datafusion::datasource::physical_plan::FileScanConfig,
        _region: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedFASTAScanner::new(conf, self.file_compression_type(), 2000);

        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonFileIndexedListingOptions for ListingFASTATableOptions {
    fn region_file(&self) -> crate::Result<&str> {
        if let Some(f) = &self.region_file {
            Ok(f.as_str())
        } else {
            Err(crate::error::ExonError::ExecutionError(
                "Expected file indexed table to have a configured index file".to_string(),
            ))
        }
    }
}

impl Default for ListingFASTATableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from(".fasta"),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
            regions: Vec::new(),
            region_file: None,
            sequence_data_type: SequenceDataType::Utf8,
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
            regions: Vec::new(),
            region_file: None,
            sequence_data_type: SequenceDataType::Utf8,
        }
    }

    /// Set the sequence data type for the table
    pub fn with_sequence_data_type(self, sequence_data_type: SequenceDataType) -> Self {
        Self {
            sequence_data_type,
            ..self
        }
    }

    /// Set the file extension for the table
    pub fn with_some_file_extension(self, file_extension: Option<&str>) -> Self {
        let file_extension = if let Some(file_extension) = file_extension {
            get_file_extension_with_compression(file_extension, self.file_compression_type)
        } else {
            ExonFileType::FASTA.get_file_extension(self.file_compression_type)
        };

        Self {
            file_extension,
            ..self
        }
    }

    /// Set the region for the table
    pub fn with_regions(self, regions: Vec<Region>) -> Self {
        Self { regions, ..self }
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
        _state: &SessionState,
    ) -> datafusion::error::Result<TableSchema> {
        let mut fasta_schema_builder = FASTASchemaBuilder::default()
            .with_sequence_data_type(self.sequence_data_type.clone())
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
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingFASTATable<T> {
    config: ExonListingConfig<T>,

    table_schema: TableSchema,
}

impl<T: ExonFileIndexedListingOptions + ExonSequenceDataTypeOptions> ListingFASTATable<T> {
    /// Create a new VCF listing table
    pub fn try_new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            config,
            table_schema,
        })
    }

    async fn resolve_region<'a>(
        &self,
        filters: &[Expr],
        session_context: &'a SessionState,
    ) -> Result<Option<Vec<Region>>> {
        if !self.config.options.regions().is_empty() {
            return Ok(Some(self.config.options.regions().to_vec()));
        }

        let region_predicate = filters
            .iter()
            .map(|f| match f {
                Expr::ScalarFunction(s) => {
                    let r = infer_region::infer_region_from_udf(s, "fasta_region_filter");

                    Some(r)
                }
                _ => Some(Ok(None)),
            })
            .next()
            .flatten();

        let attached_regions = if !self.config.options.regions().is_empty() {
            Some(self.config.options.regions().to_vec())
        } else if let Some(Ok(Some(region))) = region_predicate {
            Some(vec![region])
        } else if let Some(Err(e)) = region_predicate {
            return Err(e.into());
        } else {
            None
        };

        let regions_from_file = if self.config.options.indexed() {
            self.config
                .options
                .get_regions_from_file(session_context.runtime_env())
                .await?
        } else {
            Vec::new()
        };

        if let Some(attached_regions) = attached_regions {
            let concatenated_regions = [attached_regions, regions_from_file].concat();

            Ok(Some(concatenated_regions))
        } else if !regions_from_file.is_empty() {
            Ok(Some(regions_from_file))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl<T: ExonFileIndexedListingOptions + ExonSequenceDataTypeOptions + 'static> TableProvider
    for ListingFASTATable<T>
{
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
                _ => filter_matches_partition_cols(f, self.config.options.table_partition_cols()),
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
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
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
            // If we have regions, for local files, just add the region extension to the ObjectMeta
            // for remote files, prequery the index and associate the faidx region

            let url: &url::Url = object_store_url.as_ref();

            let mut file_partitions = Vec::new();
            match url.scheme() {
                "file" => {
                    for file in file_list {
                        for region in regions {
                            let mut region_file = file.clone();
                            let region_extension = RegionObjectStoreExtension::from(region);

                            region_file.extensions = Some(Arc::new(region_extension));
                            file_partitions.push(region_file);
                        }
                    }
                }
                _ => {
                    // If there was a region, we need to create a set of file partitions augmented with the
                    // byte offsets of the region in the file
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
                }
            }

            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
                vec![file_partitions],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.config.options.table_partition_cols().to_vec())
            .limit_option(limit)
            .build();

            let scan = self
                .config
                .options
                .create_physical_plan_with_regions(file_scan_config, regions.to_vec())
                .await?;

            Ok(scan)
        } else {
            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&self.table_schema.file_schema()?),
                vec![file_list],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.config.options.table_partition_cols().to_vec())
            .limit_option(limit)
            .build();

            let scan = FASTAScan::new(
                file_scan_config,
                self.config.options.file_compression_type(),
                fasta_sequence_buffer_capacity,
                self.config.options.sequence_data_type().clone(),
            );

            Ok(Arc::new(scan))
        }
    }
}

// Copyright 2024 WHERE TRUE Technologies.
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
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_gff::new_gff_schema_builder;
use futures::{StreamExt, TryStreamExt};
use noodles::core::Region;

use crate::{
    datasources::{
        exon_file_type::get_file_extension_with_compression,
        exon_listing_table_options::{
            ExonIndexedListingOptions, ExonListingConfig, ExonListingOptions,
        },
        hive_partition::filter_matches_partition_cols,
        indexed_file::indexed_bgzf_file::{
            augment_partitioned_file_with_byte_range, IndexedBGZFFile,
        },
        ExonFileType,
    },
    error::Result as ExonResult,
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};

use super::{indexed_scanner::IndexedGffScanner, GFFScan};

#[derive(Debug, Clone)]
/// Listing options for a GFF table
pub struct ListingGFFTableOptions {
    /// The file extension
    file_extension: String,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// The table partition columns
    table_partition_cols: Vec<Field>,

    /// True if the file must be indexed
    indexed: bool,

    /// A region to filter the records
    regions: Vec<Region>,
}

impl Default for ListingGFFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: ExonFileType::GFF.get_file_extension(FileCompressionType::UNCOMPRESSED),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
            indexed: false,
            regions: Vec::new(),
        }
    }
}

#[async_trait]
impl ExonListingOptions for ListingGFFTableOptions {
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
        let scan = GFFScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingGFFTableOptions {
    fn indexed(&self) -> bool {
        self.indexed
    }

    fn regions(&self) -> &[Region] {
        &self.regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        region: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scanner =
            IndexedGffScanner::new(conf.clone(), Arc::new(region.first().unwrap().clone()))?;

        Ok(Arc::new(scanner))
    }
}

impl ListingGFFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::GFF.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
            indexed: false,
            regions: Vec::new(),
        }
    }

    /// Set the file extension
    pub fn with_file_extension(self, file_extension: Option<String>) -> Self {
        let file_extension = if let Some(file_extension) = file_extension {
            get_file_extension_with_compression(&file_extension, self.file_compression_type)
        } else {
            ExonFileType::GFF.get_file_extension(self.file_compression_type)
        };

        Self {
            file_extension,
            ..self
        }
    }

    /// Set if indexed
    pub fn with_indexed(self, indexed: bool) -> Self {
        Self { indexed, ..self }
    }

    /// Set the region
    pub fn with_region(self, region: Region) -> Self {
        Self {
            regions: vec![region],
            indexed: true,
            ..self
        }
    }

    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Infer the base schema for the table from the file schema
    pub async fn infer_schema(&self) -> datafusion::error::Result<TableSchema> {
        let schema = new_gff_schema_builder();
        let schema = schema.add_partition_fields(self.table_partition_cols.clone());

        Ok(schema.build())
    }
}

#[derive(Debug, Clone)]
/// A GFF listing table
pub struct ListingGFFTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingGFFTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonIndexedListingOptions + 'static> TableProvider for ListingGFFTable<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Listing tables return the file schema with the addition of the partition columns
        Arc::clone(&self.table_schema.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        tracing::info!("Pushing down region in gff filter: {:?}", filters);
        Ok(filters
            .iter()
            .map(|f| match f {
                Expr::ScalarFunction(s) if s.name() == "gff_region_filter" => {
                    tracing::info!("Pushing down region filter: {:?}", s);
                    TableProviderFilterPushDown::Exact
                }
                _ => filter_matches_partition_cols(f, self.config.options.table_partition_cols()),
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        tracing::info!("Scanning GFF table with filters: {:?}", filters);
        let url = if let Some(url) = self.config.inner.table_paths.first() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let regions = filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    let r = infer_region::infer_region_from_udf(s, "gff_region_filter")?;
                    Ok(r)
                } else {
                    Ok(None)
                }
            })
            .collect::<ExonResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let regions = self.config.options.coalesce_regions(regions);

        if regions.is_empty() && self.config.options.indexed() {
            return Err(DataFusionError::Plan(
                "INDEXED_GFF table type requires a region filter. See the 'gff_region_filter' function.".to_string(),
            ));
        }

        if self.config.options.indexed() && !regions.is_empty() {
            let mut file_list = pruned_partition_list(
                state,
                &object_store,
                url,
                filters,
                self.config.options.file_extension(),
                self.config.options.table_partition_cols(),
            )
            .await?;

            let mut file_partitions = Vec::new();

            let region = regions.first().unwrap();

            while let Some(f) = file_list.next().await {
                let f = f?;

                let file_byte_range = augment_partitioned_file_with_byte_range(
                    Arc::clone(&object_store),
                    &f,
                    region,
                    &IndexedBGZFFile::Gff,
                )
                .await?;

                file_partitions.extend(file_byte_range);
            }

            let file_scan_config = FileScanConfigBuilder::new(
                url.object_store(),
                self.table_schema.file_schema()?,
                vec![file_partitions],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.config.options.table_partition_cols().to_vec())
            .limit_option(limit)
            .build();

            let plan = self
                .config
                .options
                .create_physical_plan_with_regions(file_scan_config, vec![region.clone()])
                .await?;
            return Ok(plan);
        }

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

        let plan = self
            .config
            .options
            .create_physical_plan(file_scan_config)
            .await?;
        Ok(plan)
    }
}

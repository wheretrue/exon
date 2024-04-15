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
        exon_listing_table_options::{
            ExonIndexedListingOptions, ExonListingConfig, ExonListingOptions,
        },
        hive_partition::filter_matches_partition_cols,
        indexed_file::indexed_bgzf_file::{
            augment_partitioned_file_with_byte_range, IndexedBGZFFile,
        },
    },
    error::Result as ExonResult,
    physical_plan::{infer_region, object_store::pruned_partition_list},
};
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
    physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_sam::SAMSchemaBuilder;
use futures::{StreamExt, TryStreamExt};
use noodles::{core::Region, sam::alignment::RecordBuf};
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use super::{indexed_scanner::IndexedBAMScan, BAMScan};

#[derive(Debug, Clone)]
/// Listing options for a BAM table
pub struct ListingBAMTableOptions {
    /// The file extension for the BAM file
    file_extension: String,

    /// Whether the scan should use the index
    indexed: bool,

    /// Any regions to use for the scan
    region: Option<Region>,

    /// The table partition columns
    table_partition_cols: Vec<Field>,

    /// Whether to infer the schema from the tags
    tag_as_struct: bool,
}

impl Default for ListingBAMTableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from("bam"),
            table_partition_cols: Vec::new(),
            indexed: false,
            tag_as_struct: false,
            region: None,
        }
    }
}

#[async_trait]
impl ExonListingOptions for ListingBAMTableOptions {
    fn table_partition_cols(&self) -> Vec<Field> {
        self.table_partition_cols.clone()
    }

    fn file_extension(&self) -> String {
        self.file_extension.clone()
    }

    fn file_compression_type(&self) -> FileCompressionType {
        FileCompressionType::UNCOMPRESSED
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BAMScan::new(conf);
        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingBAMTableOptions {
    fn indexed(&self) -> bool {
        self.indexed
    }

    fn regions(&self) -> Vec<Region> {
        if let Some(region) = &self.region {
            vec![region.clone()]
        } else {
            Vec::new()
        }
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        regions: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if regions.is_empty() {
            return Err(DataFusionError::Execution(
                "Regions cannot be empty".to_string(),
            ));
        }

        if regions.len() > 1 {
            return Err(DataFusionError::Execution(
                "BAM currently only supports 1 region".to_string(),
            ));
        }

        let region = Arc::new(regions[0].clone());
        let scan = IndexedBAMScan::new(conf, region);
        Ok(Arc::new(scan))
    }
}

impl ListingBAMTableOptions {
    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Set the region for the table options. This is used to filter the records
    pub fn with_region(self, region: Option<Region>) -> Self {
        Self {
            region,
            indexed: true,
            ..self
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(
        &self,
        state: &SessionState,
        table_path: &ListingTableUrl,
    ) -> datafusion::error::Result<TableSchema> {
        if !self.tag_as_struct {
            let builder = SAMSchemaBuilder::default()
                .with_partition_fields(self.table_partition_cols.clone()); // TODO: get rid of clone
            let table_schema = builder.build();

            return Ok(table_schema);
        }

        let store = state.runtime_env().object_store(table_path)?;

        let mut files = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            self.file_extension.as_str(),
            None,
        )
        .await;

        let mut schema_builder = SAMSchemaBuilder::default();

        while let Some(f) = files.next().await {
            let f = f?;

            let get_result = store.get(&f.location).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);
            let mut reader = noodles::bam::AsyncReader::new(stream_reader);

            let header = reader.read_header().await?;

            let mut record = RecordBuf::default();

            reader.read_record_buf(&header, &mut record).await?;

            let data = record.data();
            schema_builder = schema_builder.with_tags_data_type_from_data(data)?;
        }

        schema_builder = schema_builder.with_partition_fields(self.table_partition_cols.clone()); // TODO: get rid of clone

        let table_schema = schema_builder.build();

        Ok(table_schema)
    }

    /// Update the indexed flag
    pub fn with_indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }

    /// Update the tag_as_struct flag
    pub fn with_tag_as_struct(mut self, tag_as_struct: bool) -> Self {
        self.tag_as_struct = tag_as_struct;
        self
    }
}

#[derive(Debug, Clone)]
/// A BAM listing table
pub struct ListingBAMTable<T> {
    config: ExonListingConfig<T>,
    table_schema: TableSchema,
}

impl<T> ListingBAMTable<T> {
    /// Create a new BAM listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            config,
            table_schema,
        }
    }
}

#[async_trait]
impl<T: ExonIndexedListingOptions> TableProvider for ListingBAMTable<T> {
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
            .map(|f| match f {
                Expr::ScalarFunction(s) if s.name() == "bam_region_filter" => {
                    if s.args.len() == 2 || s.args.len() == 4 {
                        tracing::debug!("Pushing down region filter");
                        TableProviderFilterPushDown::Exact
                    } else {
                        tracing::debug!("Unsupported number of arguments for region filter");
                        filter_matches_partition_cols(
                            f,
                            &self.config.options.table_partition_cols(),
                        )
                    }
                }
                _ => filter_matches_partition_cols(f, &self.config.options.table_partition_cols()),
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
        let url = if let Some(url) = self.config.first_table_path() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let regions = filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    let r = infer_region::infer_region_from_udf(s, "bam_region_filter")?;
                    Ok(r)
                } else {
                    Ok(None)
                }
            })
            .collect::<ExonResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let regions = if self.config.options.indexed() {
            if regions.len() == 1 {
                regions
            } else {
                self.config.options.regions()
            }
        } else {
            regions
        };

        if regions.is_empty() && self.config.options.indexed() {
            return Err(DataFusionError::Plan(
                "INDEXED_BAM table type requires a region filter. See the 'bam_region_filter' function.".to_string(),
            ));
        }

        if regions.len() > 1 {
            return Err(DataFusionError::Plan(
                "Only one region filter is supported".to_string(),
            ));
        }

        if regions.is_empty() {
            let file_list = pruned_partition_list(
                state,
                &object_store,
                url,
                filters,
                &self.config.options.file_extension(),
                &self.config.options.table_partition_cols(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

            let file_scan_config = FileScanConfig {
                object_store_url: url.object_store(),
                file_schema: self.table_schema.file_schema()?,
                file_groups: vec![file_list],
                statistics: Statistics::new_unknown(self.table_schema.file_schema()?.as_ref()),
                projection: projection.cloned(),
                limit,
                output_ordering: Vec::new(),
                table_partition_cols: self.config.options.table_partition_cols().to_vec(),
            };

            let plan = self
                .config
                .options
                .create_physical_plan(file_scan_config)
                .await?;

            return Ok(plan);
        }

        let file_extension = self.config.options.file_extension();
        let partition_cols = self.config.options.table_partition_cols();

        let mut file_list = pruned_partition_list(
            state,
            &object_store,
            url,
            filters,
            &file_extension,
            &partition_cols,
        )
        .await?;

        let mut file_partition_with_ranges = Vec::new();

        let region = regions[0].clone();

        while let Some(f) = file_list.next().await {
            let f = f?;

            let file_byte_range = augment_partitioned_file_with_byte_range(
                object_store.clone(),
                &f,
                &region,
                &IndexedBGZFFile::Bam,
            )
            .await?;

            file_partition_with_ranges.extend(file_byte_range);
        }

        let file_scan_config = FileScanConfig {
            object_store_url: url.object_store(),
            file_schema: self.table_schema.file_schema()?,
            file_groups: vec![file_partition_with_ranges],
            statistics: Statistics::new_unknown(self.table_schema.file_schema()?.as_ref()),
            projection: projection.cloned(),
            limit,
            output_ordering: Vec::new(),
            table_partition_cols: self.config.options.table_partition_cols().to_vec(),
        };

        let table = self
            .config
            .options
            .create_physical_plan_with_regions(file_scan_config, vec![region])
            .await?;

        return Ok(table);
    }
}

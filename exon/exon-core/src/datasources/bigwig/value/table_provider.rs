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

use crate::{
    datasources::{
        exon_listing_table_options::{
            ExonIndexedListingOptions, ExonListingConfig, ExonListingOptions,
        },
        hive_partition::filter_matches_partition_cols,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};
use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use exon_bigwig::value_batch_reader::SchemaBuilder;
use exon_common::TableSchema;
use futures::TryStreamExt;
use noodles::core::Region;

use super::scanner::Scanner;

#[derive(Debug, Clone)]
/// Listing options for a BigWig table
pub struct ListingTableOptions {
    /// The file extension, including the compression type
    file_extension: String,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,

    indexed: bool,

    regions: Vec<Region>,
}

impl Default for ListingTableOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExonListingOptions for ListingTableOptions {
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
        let scan = Scanner::new(conf.clone());

        Ok(Arc::new(scan))
    }
}

#[async_trait]
impl ExonIndexedListingOptions for ListingTableOptions {
    fn indexed(&self) -> bool {
        self.indexed
    }

    fn regions(&self) -> &[Region] {
        &self.regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        regions: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let region = regions.first().unwrap();
        let scan = Scanner::new(conf.clone()).with_some_region_filter(Some(region.clone()));

        Ok(Arc::new(scan))
    }
}

impl ListingTableOptions {
    /// Create a new set of options
    pub fn new() -> Self {
        let file_extension =
            ExonFileType::BigWigValue.get_file_extension(FileCompressionType::UNCOMPRESSED);

        Self {
            file_extension,
            table_partition_cols: Vec::new(),
            indexed: false,
            regions: Vec::new(),
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
        let mut schema_builder = SchemaBuilder::default();
        schema_builder.add_partition_fields(self.table_partition_cols.clone());

        Ok(schema_builder.build())
    }
}

#[derive(Debug, Clone)]
/// A BigWig listing table
pub struct ListingTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonIndexedListingOptions + 'static> TableProvider for ListingTable<T> {
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
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            // .map(|f| filter_matches_partition_cols(f, &self.options.table_partition_cols))
            .map(|f| match f {
                Expr::ScalarFunction(scalar) => {
                    if scalar.name() == "bigwig_region_filter" {
                        TableProviderFilterPushDown::Exact
                    } else {
                        filter_matches_partition_cols(f, self.config.options.table_partition_cols())
                    }
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let url = if let Some(url) = self.config.inner.table_paths.first() {
            url
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "No object store URL found".to_string(),
            ));
        };

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            self.config.inner.table_paths.first().unwrap(),
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
                .limit_option(limit)
                .table_partition_cols(self.config.options.table_partition_cols().to_vec())
                .build();

        let regions = filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    let r = infer_region::infer_region_from_udf(s, "bigwig_region_filter")?;
                    Ok(r)
                } else {
                    Ok(None)
                }
            })
            .collect::<crate::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if regions.is_empty() {
            let plan = self
                .config
                .options
                .create_physical_plan(file_scan_config)
                .await?;

            Ok(plan)
        } else if regions.len() == 1 {
            tracing::info!(
                "Creating physical plan with region: {:?}",
                regions.first().unwrap()
            );
            let plan = self
                .config
                .options
                .create_physical_plan_with_regions(file_scan_config.clone(), regions.to_vec())
                .await?;

            Ok(plan)
        } else {
            let mut plans = Vec::new();

            for region in regions {
                let plan = self
                    .config
                    .options
                    .create_physical_plan_with_regions(file_scan_config.clone(), vec![region])
                    .await?;

                plans.push(plan);
            }

            let plan = datafusion::physical_plan::union::UnionExec::new(plans);

            Ok(Arc::new(plan))
        }
    }
}

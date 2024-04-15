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
    datasources::{hive_partition::filter_matches_partition_cols, ExonFileType},
    error::Result,
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, infer_region,
        object_store::pruned_partition_list,
    },
};
use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig as DataFusionListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result as DataFusionResult,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use exon_bigwig::zoom_batch_reader::SchemaBuilder;
use exon_common::TableSchema;
use futures::TryStreamExt;
use noodles::core::Region;

use super::scanner::Scanner;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingTableConfig {
    inner: DataFusionListingTableConfig,

    options: ListingTableOptions,
}

impl ListingTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl, options: ListingTableOptions) -> Self {
        Self {
            inner: DataFusionListingTableConfig::new(table_path),
            options,
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a BigWig table
pub struct ListingTableOptions {
    /// The file extension, including the compression type
    file_extension: String,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,

    /// The reduction level for the BigWig scan
    reduction_level: u32,
}

impl ListingTableOptions {
    /// Create a new set of options
    pub fn new(reduction_level: u32) -> Self {
        let file_extension =
            ExonFileType::BigWigZoom.get_file_extension(FileCompressionType::UNCOMPRESSED);

        Self {
            file_extension,
            table_partition_cols: Vec::new(),
            reduction_level,
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

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let scan = Scanner::new(conf.clone(), self.reduction_level);

        Ok(Arc::new(scan))
    }

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
        region: Region,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let scan =
            Scanner::new(conf.clone(), self.reduction_level).with_some_region_filter(Some(region));

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BigWig listing table
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingTableOptions,
}

impl ListingTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config.options,
        })
    }
}

#[async_trait]
impl TableProvider for ListingTable {
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
                        filter_matches_partition_cols(f, &self.options.table_partition_cols)
                    }
                }
                _ => filter_matches_partition_cols(f, &self.options.table_partition_cols),
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
        let object_store_url = if let Some(url) = self.table_paths.first() {
            url.object_store()
        } else {
            todo!()
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

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url.clone(), file_schema, vec![file_list])
                .projection_option(projection.cloned())
                .limit_option(limit)
                .table_partition_cols(self.options.table_partition_cols.clone())
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
            let plan = self.options.create_physical_plan(file_scan_config).await?;

            Ok(plan)
        } else if regions.len() == 1 {
            tracing::info!(
                "Creating physical plan with region: {:?}",
                regions.first().unwrap()
            );
            let plan = self
                .options
                .create_physical_plan_with_region(
                    file_scan_config.clone(),
                    regions.first().unwrap().clone(),
                )
                .await?;

            Ok(plan)
        } else {
            let mut plans = Vec::new();

            for region in regions {
                let plan = self
                    .options
                    .create_physical_plan_with_region(file_scan_config.clone(), region)
                    .await?;

                plans.push(plan);
            }

            let plan = datafusion::physical_plan::union::UnionExec::new(plans);

            Ok(Arc::new(plan))
        }
    }
}

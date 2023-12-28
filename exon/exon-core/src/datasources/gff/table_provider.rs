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

use std::{any::Any, str::FromStr, sync::Arc};

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
    logical_expr::{expr::ScalarFunction, TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_gff::new_gff_schema_builder;
use futures::{StreamExt, TryStreamExt};
use noodles::core::Region;

use crate::{
    datasources::indexed_file_utils::augment_partitioned_file_with_byte_range,
    datasources::{
        hive_partition::filter_matches_partition_cols, indexed_file_utils::IndexedFile,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::{indexed_scanner::IndexedGffScanner, GFFScan};

fn infer_region_from_scalar_udf(scalar_udf: &ScalarFunction) -> Option<Region> {
    if scalar_udf.name() == "gff_region_filter" {
        match &scalar_udf.args[0] {
            Expr::Literal(l) => {
                let region_str = l.to_string();
                let region = Region::from_str(region_str.as_str()).ok()?;
                Some(region)
            }
            _ => None,
        }
    } else {
        None
    }
}

#[derive(Debug, Clone)]
/// Configuration for a GFF listing table
pub struct ListingGFFTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingGFFTableOptions>,
}

impl ListingGFFTableConfig {
    /// Create a new GFF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingGFFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

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
    region: Option<Region>,
}

impl ListingGFFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType, indexed: bool) -> Self {
        let file_extension = ExonFileType::GFF.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
            indexed,
            region: None,
        }
    }

    /// Set the region
    pub fn with_region(self, region: Region) -> Self {
        Self {
            region: Some(region),
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

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
        region: Arc<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedGffScanner::new(conf.clone(), region)?;

        Ok(Arc::new(scan))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = GFFScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A GFF listing table
pub struct ListingGFFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingGFFTableOptions,
}

impl ListingGFFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingGFFTableConfig, table_schema: TableSchema) -> Result<Self> {
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
impl TableProvider for ListingGFFTable {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        tracing::info!("Scanning GFF table with filters: {:?}", filters);
        let object_store_url = if let Some(url) = self.table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let regions = filters
            .iter()
            .filter_map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    infer_region_from_scalar_udf(s)
                } else {
                    None
                }
            })
            .collect::<Vec<Region>>();

        let regions = if self.options.indexed {
            if regions.len() == 1 {
                regions
            } else {
                match self.options.region.clone() {
                    Some(region) => vec![region],
                    None => regions,
                }
            }
        } else {
            regions
        };

        if regions.is_empty() && self.options.indexed {
            return Err(DataFusionError::Plan(
                "INDEXED_GFF table type requires a region filter. See the 'gff_region_filter' function.".to_string(),
            ));
        }

        if self.options.indexed && !regions.is_empty() {
            let mut file_list = pruned_partition_list(
                state,
                &object_store,
                &self.table_paths[0],
                filters,
                self.options.file_extension.as_str(),
                &self.options.table_partition_cols,
            )
            .await?;

            let mut file_partitions = Vec::new();

            let region = regions.first().unwrap();

            while let Some(f) = file_list.next().await {
                let f = f?;

                let file_byte_range = augment_partitioned_file_with_byte_range(
                    object_store.clone(),
                    &f,
                    region,
                    &IndexedFile::Gff,
                )
                .await?;

                file_partitions.extend(file_byte_range);
            }

            let file_scan_config = FileScanConfigBuilder::new(
                object_store_url.clone(),
                self.table_schema.file_schema()?,
                vec![file_partitions],
            )
            .projection_option(projection.cloned())
            .table_partition_cols(self.options.table_partition_cols.clone())
            .limit_option(limit)
            .build();

            let region = Arc::new(region.clone());
            let plan = self
                .options
                .create_physical_plan_with_region(file_scan_config, region)
                .await?;
            return Ok(plan);
        }

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

        let plan = self.options.create_physical_plan(file_scan_config).await?;
        Ok(plan)
    }
}

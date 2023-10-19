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

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{expr::ScalarUDF, TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_bam::TagSchemaBuilder;
use futures::{StreamExt, TryStreamExt};
use noodles::{bam::lazy::Record, core::Region};
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

fn infer_region_from_scalar_udf(scalar_udf: &ScalarUDF) -> Option<Region> {
    if scalar_udf.fun.name.as_str() == "bam_region_filter" {
        if scalar_udf.args.len() == 2 || scalar_udf.args.len() == 4 {
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
    } else {
        None
    }
}

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBAMTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingBAMTableOptions>,
}

impl ListingBAMTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingBAMTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

use crate::{
    datasources::{indexed_file_utils::IndexedFile, sam::schema},
    physical_plan::file_scan_config_builder::FileScanConfigBuilder,
};

use super::{indexed_scanner::IndexedBAMScan, BAMScan};

#[derive(Debug, Clone)]
/// Listing options for a BAM table
pub struct ListingBAMTableOptions {
    file_extension: String,

    indexed: bool,

    tag_as_struct: bool,
}

impl Default for ListingBAMTableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from("bam"),
            indexed: false,
            tag_as_struct: false,
        }
    }
}

impl ListingBAMTableOptions {
    /// Infer the schema for the table
    pub async fn infer_schema(
        &self,
        state: &SessionState,
        table_path: &ListingTableUrl,
    ) -> datafusion::error::Result<SchemaRef> {
        if !self.tag_as_struct {
            let schema = schema();
            return Ok(Arc::new(schema));
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

        let mut tag_schema_builder = TagSchemaBuilder::new();

        while let Some(f) = files.next().await {
            let f = f?;

            let get_result = store.get(&f.location).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);
            let mut reader = noodles::bam::AsyncReader::new(stream_reader);

            reader.read_header().await?;
            reader.read_reference_sequences().await?;

            let mut record = Record::default();

            reader.read_lazy_record(&mut record).await?;

            let data = record.data();
            let data: noodles::sam::record::Data = data.try_into()?;

            tag_schema_builder.add_tag_data(&data).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Error adding tag data to schema builder: {}",
                    e
                ))
            })?;
        }

        let schema = tag_schema_builder.build();

        Ok(Arc::new(schema))
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

    async fn create_physical_plan_with_region(
        &self,
        conf: FileScanConfig,
        region: Arc<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = IndexedBAMScan::new(conf, region);
        Ok(Arc::new(scan))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BAMScan::new(conf);
        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BAM listing table
pub struct ListingBAMTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingBAMTableOptions,
}

impl ListingBAMTable {
    /// Create a new BAM listing table
    pub fn try_new(config: ListingBAMTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingBAMTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        tracing::debug!("Filters for BAM table provider: {:?}", filters);
        Ok(filters
            .iter()
            .map(|f| match f {
                Expr::ScalarUDF(s) if s.fun.name.as_str() == "bam_region_filter" => {
                    if s.args.len() == 2 || s.args.len() == 4 {
                        tracing::debug!("Pushing down region filter");
                        TableProviderFilterPushDown::Exact
                    } else {
                        tracing::debug!("Unsupported number of arguments for region filter");
                        TableProviderFilterPushDown::Unsupported
                    }
                }
                _ => TableProviderFilterPushDown::Unsupported,
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
        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let regions = filters
            .iter()
            .filter_map(|f| {
                if let Expr::ScalarUDF(s) = f {
                    infer_region_from_scalar_udf(s)
                } else {
                    None
                }
            })
            .collect::<Vec<Region>>();

        if regions.len() > 1 {
            return Err(DataFusionError::Execution(
                "Multiple regions are not supported yet".to_string(),
            ));
        }

        if regions.is_empty() && self.options.indexed {
            return Err(DataFusionError::Plan(
                "INDEXED_BAM table type requires a region filter. See the 'bam_region_filter' function.".to_string(),
            ));
        }

        let region = regions.get(0).cloned();

        match region {
            None => {
                let partitioned_file_lists = vec![
                    crate::physical_plan::object_store::list_files_for_scan(
                        object_store,
                        self.table_paths.clone(),
                        &self.options.file_extension,
                    )
                    .await?,
                ];

                let file_scan_config = FileScanConfigBuilder::new(
                    object_store_url.clone(),
                    Arc::clone(&self.table_schema),
                    partitioned_file_lists,
                )
                .projection_option(projection.cloned())
                .limit_option(limit)
                .build();

                let plan = self.options.create_physical_plan(file_scan_config).await?;

                Ok(plan)
            }
            Some(region) => {
                let regions = vec![region.clone()];
                let partitioned_file_lists = IndexedFile::Bam
                    .list_files_for_scan(self.table_paths.clone(), object_store, &regions)
                    .await?;

                if partitioned_file_lists.is_empty() || partitioned_file_lists[0].is_empty() {
                    return Ok(Arc::new(EmptyExec::new(
                        false,
                        Arc::clone(&self.table_schema),
                    )));
                }

                let file_scan_config = FileScanConfigBuilder::new(
                    object_store_url.clone(),
                    Arc::clone(&self.table_schema),
                    partitioned_file_lists,
                )
                .projection_option(projection.cloned())
                .limit_option(limit)
                .build();

                let plan = self
                    .options
                    .create_physical_plan_with_region(file_scan_config, Arc::new(region))
                    .await?;

                Ok(plan)
            }
        }
    }
}

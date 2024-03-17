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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use exon_common::TableSchema;
use exon_sam::SAMSchemaBuilder;
use futures::{StreamExt, TryStreamExt};
use noodles::sam::Header;
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    config::ExonConfigExtension,
    datasources::hive_partition::filter_matches_partition_cols,
    error::{ExonError, Result},
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::scanner::CRAMScan;

const CRAM_EXTENSION: &str = "cram";

#[derive(Debug, Clone)]
/// Configuration for a CRAM listing table.
pub struct ListingCRAMTableConfig {
    /// The options for the generic listing table.
    inner: ListingTableConfig,

    /// The options for the CRAM listing table.
    options: ListingCRAMTableOptions,
}

impl ListingCRAMTableConfig {
    /// Create a new CRAM listing table configuration.
    pub fn new(table_path: ListingTableUrl, options: ListingCRAMTableOptions) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListingCRAMTableOptions {
    /// The partition columns for the table.
    table_partition_cols: Vec<Field>,

    /// FASTA Reference
    fasta_reference: String,
}

impl TryFrom<&HashMap<String, String>> for ListingCRAMTableOptions {
    type Error = ExonError;

    fn try_from(options: &HashMap<String, String>) -> Result<Self> {
        let fasta_reference = options
            .get("fasta_reference")
            .ok_or(ExonError::ExecutionError(
                "No fasta reference provided for CRAM table".to_string(),
            ))?
            .to_string();

        Ok(Self::new(fasta_reference))
    }
}

impl ListingCRAMTableOptions {
    /// Create a new CRAM listing table options.
    pub fn new(fasta_reference: String) -> Self {
        Self {
            table_partition_cols: Vec::new(),
            fasta_reference,
        }
    }

    /// Set the partition columns for the table.
    pub fn with_table_partition_cols(mut self, table_partition_cols: Vec<Field>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Infer the schema from the file.
    async fn infer_schema_from_object_meta(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<TableSchema> {
        if objects.is_empty() {
            return Err(ExonError::ExecutionError("No objects found".to_string()));
        }

        let _exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(ExonError::ExecutionError(
                "Exon settings must be configured.".to_string(),
            ))?;

        tracing::info!("Getting object from store: {:?}", objects[0].location);
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(ExonError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let mut cram_reader = noodles::cram::AsyncReader::new(stream_reader);

        cram_reader.read_file_definition().await?;
        let header = cram_reader.read_file_header().await?;
        let header: Header = header
            .to_owned()
            .parse()
            .map_err(|_| DataFusionError::Execution("Unable to parse header".to_string()))?;

        let mut schema_builder = SAMSchemaBuilder::default();

        let reference_sequence_repository = noodles::fasta::Repository::default();

        if let Some(Ok(record)) = cram_reader
            .records(&reference_sequence_repository, &header)
            .next()
            .await
        {
            schema_builder = schema_builder.with_tags_data_type_from_data(record.data())?;
        } else {
            return Err(ExonError::ExecutionError(
                "No records found in CRAM file".to_string(),
            ));
        }

        Ok(schema_builder.build())
    }

    pub async fn infer_schema<'a>(
        &self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let files = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            CRAM_EXTENSION,
            None,
        )
        .await;

        // collect the files as a slice
        let files = files
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Unable to get path info: {}", e)))?;

        self.infer_schema_from_object_meta(state, &store, &files)
            .await
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = CRAMScan::new(conf, self.fasta_reference.clone());

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
pub struct ListingCRAMTable {
    /// The paths to the listing table.
    table_paths: Vec<ListingTableUrl>,

    /// The table schema.
    table_schema: TableSchema,

    /// The options for the listing table.
    options: ListingCRAMTableOptions,
}

impl ListingCRAMTable {
    /// Create a new CRAM listing table.
    pub fn try_new(config: ListingCRAMTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config.options,
        })
    }
}

#[async_trait]
impl TableProvider for ListingCRAMTable {
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
        tracing::info!(
            "cram table provider supports_filters_pushdown: {:?}",
            filters
        );

        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::ScalarFunction(s) = f {
                    if s.name() == "cram_region_filter" && (s.args.len() == 2 || s.args.len() == 3)
                    {
                        return TableProviderFilterPushDown::Exact;
                    }
                }

                filter_matches_partition_cols(f, &self.options.table_partition_cols)
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
        let object_store_url = self.table_paths[0].object_store();
        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.table_paths[0],
            filters,
            CRAM_EXTENSION,
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

        let table = self.options.create_physical_plan(file_scan_config).await?;

        return Ok(table);
    }
}

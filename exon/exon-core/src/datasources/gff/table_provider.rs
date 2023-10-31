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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::{FileCompressionType, ToDFSchema},
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use exon_gff::GFFSchemaBuilder;
use futures::TryStreamExt;

use crate::{
    datasources::{hive_partition::filter_matches_partition_cols, ExonFileType},
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::GFFScan;

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
    file_extension: String,

    file_compression_type: FileCompressionType,

    table_partition_cols: Vec<(String, DataType)>,
}

impl ListingGFFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::GFF.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
        }
    }

    /// Set the table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<(String, DataType)>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = GFFSchemaBuilder::default().build();

        Ok(schema)
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = GFFScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A GFF listing table
pub struct ListingGFFTable {
    table_paths: Vec<ListingTableUrl>,

    file_schema: SchemaRef,

    table_schema: SchemaRef,

    options: ListingGFFTableOptions,
}

impl ListingGFFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingGFFTableConfig, file_schema: Arc<Schema>) -> Result<Self> {
        let fields = config
            .options
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?
            .table_partition_cols
            .iter()
            .map(|f| Field::new(&f.0, f.1.clone(), false))
            .collect::<Vec<_>>();

        let schema_builder = GFFSchemaBuilder::default().extend(fields);

        Ok(Self {
            table_paths: config.inner.table_paths,
            file_schema,
            table_schema: schema_builder.build(),
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
        Arc::clone(&self.table_schema)
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
            .map(|f| filter_matches_partition_cols(f, &self.options.table_partition_cols))
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

        let inner_size = 1;
        let file_groups: Vec<Vec<PartitionedFile>> = file_list
            .chunks(inner_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&self.file_schema),
            file_groups,
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.options.table_partition_cols.clone())
        .limit_option(limit)
        .build();

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                &self.table_schema,
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };

        let plan = self
            .options
            .create_physical_plan(file_scan_config, filters.as_ref())
            .await?;

        Ok(plan)
    }
}

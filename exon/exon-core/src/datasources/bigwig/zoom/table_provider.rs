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
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};
use arrow::datatypes::{Field, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use exon_bigwig::BigWigSchemaBuilder;
use exon_common::TableSchema;
use futures::TryStreamExt;

use super::scanner::Scanner;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBigWigTableConfig {
    inner: ListingTableConfig,

    options: ListingBigWigTableOptions,
}

impl ListingBigWigTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl, options: ListingBigWigTableOptions) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a BigWig table
pub struct ListingBigWigTableOptions {
    /// The file extension, including the compression type
    file_extension: String,

    /// A list of table partition columns
    table_partition_cols: Vec<Field>,

    /// The reduction level for the BigWig scan
    reduction_level: u32,
}

impl ListingBigWigTableOptions {
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
        let mut schema_builder = BigWigSchemaBuilder::default();
        schema_builder.add_partition_fields(self.table_partition_cols.clone());

        Ok(schema_builder.build())
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = Scanner::new(conf.clone(), self.reduction_level);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BigWig listing table
pub struct ListingBigWigTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingBigWigTableOptions,
}

impl ListingBigWigTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingBigWigTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config.options,
        })
    }
}

#[async_trait]
impl TableProvider for ListingBigWigTable {
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
        let object_store_url = if let Some(url) = self.table_paths.first() {
            url.object_store()
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "No table paths found".to_string(),
            ));
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

        let plan = self.options.create_physical_plan(file_scan_config).await?;

        Ok(plan)
    }
}

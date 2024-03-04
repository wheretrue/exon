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

use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::GetExt,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_fastq::new_fastq_schema_builder;
use futures::TryStreamExt;

use crate::{
    datasources::{hive_partition::filter_matches_partition_cols, ExonFileType},
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::FASTQScan;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingFASTQTableConfig {
    inner: ListingTableConfig,

    options: ListingFASTQTableOptions,
}

impl ListingFASTQTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl, options: ListingFASTQTableOptions) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a FASTQ table
pub struct ListingFASTQTableOptions {
    /// The file extension for the table
    file_extension: String,

    /// The file compression type
    file_compression_type: FileCompressionType,

    /// The table partition columns
    table_partition_cols: Vec<Field>,
}

impl Default for ListingFASTQTableOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from("fastq"),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
        }
    }
}

impl ListingFASTQTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::FASTQ.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
        }
    }

    /// Get the extension when accounting for the compression type
    pub fn file_extension(&self) -> String {
        if self
            .file_extension
            .ends_with(&self.file_compression_type.get_ext())
        {
            self.file_extension.clone()
        } else {
            format!(
                "{}{}",
                self.file_extension,
                self.file_compression_type.get_ext()
            )
        }
    }

    /// Set the file extension for the table
    pub fn with_file_extension(self, file_extension: String) -> Self {
        Self {
            file_extension,
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

    /// Infer the schema for the underlying files
    pub fn infer_schema(&self) -> TableSchema {
        let builder =
            new_fastq_schema_builder().add_partition_fields(self.table_partition_cols.clone());
        builder.build()
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = FASTQScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A FASTQ listing table
pub struct ListingFASTQTable {
    /// The schema for the table
    table_schema: TableSchema,

    /// The config for the table
    config: ListingFASTQTableConfig,
}

impl ListingFASTQTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingFASTQTableConfig, table_schema: TableSchema) -> Result<Self> {
        Ok(Self {
            table_schema,
            config,
        })
    }
}

#[async_trait]
impl TableProvider for ListingFASTQTable {
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
            .map(|f| filter_matches_partition_cols(f, &self.config.options.table_partition_cols))
            .collect())
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

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.config.inner.table_paths[0],
            filters,
            &self.config.options.file_extension(),
            &self.config.options.table_partition_cols,
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        eprintln!("file_list: {:?}", file_list);

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            self.table_schema.file_schema()?,
            vec![file_list],
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.config.options.table_partition_cols.clone())
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

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        prelude::SessionContext,
    };
    use exon_test::test_listing_table_url;

    use crate::{
        datasources::{ExonFileType, ExonListingTableFactory},
        ExonSessionExt,
    };

    #[tokio::test]
    async fn test_table_scan() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("fastq");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::FASTQ,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
            )
            .await?;

        let df = ctx.read_table(table)?;

        let mut row_cnt = 0;
        let bs = df.collect().await?;
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 2);

        Ok(())
    }
}

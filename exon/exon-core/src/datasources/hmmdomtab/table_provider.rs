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
    datasource::{
        file_format::file_compression_type::FileCompressionType, physical_plan::FileScanConfig,
        TableProvider,
    },
    error::Result,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use exon_common::TableSchema;
use futures::TryStreamExt;

use crate::{
    datasources::{
        exon_listing_table_options::{ExonListingConfig, ExonListingOptions},
        hive_partition::filter_matches_partition_cols,
        ExonFileType,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::{hmm_dom_tab_config::HMMDomTabSchemaBuilder, HMMDomTabScan};

#[derive(Debug, Clone)]
/// Listing options for a HMM Dom Tab table
pub struct ListingHMMDomTabTableOptions {
    /// File extension for the table
    file_extension: String,

    /// File compression type
    file_compression_type: FileCompressionType,

    /// Partition columns for the table
    table_partition_cols: Vec<Field>,
}

#[async_trait]
impl ExonListingOptions for ListingHMMDomTabTableOptions {
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
        let scan = HMMDomTabScan::new(conf.clone(), self.file_compression_type);
        Ok(Arc::new(scan))
    }
}

impl Default for ListingHMMDomTabTableOptions {
    fn default() -> Self {
        Self::new(FileCompressionType::UNCOMPRESSED)
    }
}

impl ListingHMMDomTabTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::HMMDOMTAB.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: Vec::new(),
        }
    }

    /// Set the partition columns for the table
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<TableSchema> {
        let mut schema_builder = HMMDomTabSchemaBuilder::default();
        schema_builder.add_partition_fields(self.table_partition_cols.clone());

        let table_schema = schema_builder.build();
        Ok(table_schema)
    }
}

#[derive(Debug, Clone)]
/// A HMM Dom listing table
pub struct ListingHMMDomTabTable<T: ExonListingOptions> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T: ExonListingOptions> ListingHMMDomTabTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonListingOptions + 'static> TableProvider for ListingHMMDomTabTable<T> {
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
            .map(|f| filter_matches_partition_cols(f, self.config.options.table_partition_cols()))
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let url = if let Some(url) = self.config.inner.table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(url.clone())?;

        let file_list = pruned_partition_list(
            state,
            &object_store,
            &self.config.inner.table_paths[0],
            filters,
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config = FileScanConfigBuilder::new(url, file_schema, vec![file_list])
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        datasources::{ExonFileType, ExonListingTableFactory},
        ExonSessionExt,
    };

    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        prelude::SessionContext,
    };
    use exon_test::test_listing_table_url;

    #[tokio::test]
    async fn test_listing() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("hmmdomtab");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::HMMDOMTAB,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
                &HashMap::new(),
            )
            .await?;

        let df = ctx.read_table(table).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 100);

        Ok(())
    }
}

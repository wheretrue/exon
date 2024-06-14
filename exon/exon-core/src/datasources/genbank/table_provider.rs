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
use exon_genbank::schema;

use crate::{
    datasources::{
        exon_listing_table_options::{ExonListingConfig, ExonListingOptions},
        ExonFileType,
    },
    physical_plan::file_scan_config_builder::FileScanConfigBuilder,
};

use super::GenbankScan;

impl Default for ListingGenbankTableOptions {
    fn default() -> Self {
        Self::new(FileCompressionType::UNCOMPRESSED)
    }
}

#[derive(Debug, Clone)]
/// Listing options for a Genbank table
pub struct ListingGenbankTableOptions {
    file_extension: String,

    file_compression_type: FileCompressionType,
}

#[async_trait]
impl ExonListingOptions for ListingGenbankTableOptions {
    fn table_partition_cols(&self) -> &[Field] {
        &[]
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
        let scan = GenbankScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

impl ListingGenbankTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::GENBANK.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
/// A table provider for a Genbank listing table
pub struct ListingGenbankTable<T> {
    table_schema: SchemaRef,

    config: ExonListingConfig<T>,
}

impl<T> ListingGenbankTable<T> {
    /// Create a new Genbank listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: Arc<Schema>) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonListingOptions + 'static> TableProvider for ListingGenbankTable<T> {
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
        Ok(filters
            .iter()
            .map(|_f| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let url = if let Some(url) = self.config.inner.table_paths.first() {
            url
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(url.object_store())?;

        let partitioned_file_lists = vec![
            crate::physical_plan::object_store::list_files_for_scan(
                object_store,
                self.config.inner.table_paths.to_vec(),
                self.config.options.file_extension(),
                &[],
            )
            .await?,
        ];

        let file_scan_config = FileScanConfigBuilder::new(
            url.object_store(),
            Arc::clone(&self.table_schema),
            partitioned_file_lists,
        )
        .projection_option(projection.cloned())
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
        ExonSession,
    };

    use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
    use exon_test::test_listing_table_url;

    #[tokio::test]
    async fn test_listing() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon();
        let session_state = ctx.session.state();

        let table_path = test_listing_table_url("genbank/test.gb");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::GENBANK,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
                &HashMap::new(),
            )
            .await?;

        let df = ctx.session.read_table(table)?;

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 1);

        Ok(())
    }
}

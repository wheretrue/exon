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

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::FileCompressionType,
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};

use crate::{
    datasources::ExonFileType, physical_plan::file_scan_config_builder::FileScanConfigBuilder,
};

use super::{hmm_dom_tab_config::schema, HMMDomTabScan};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingHMMDomTabTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingHMMDomTabTableOptions>,
}

impl ListingHMMDomTabTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingHMMDomTabTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a HMM Dom Tab table
pub struct ListingHMMDomTabTableOptions {
    file_extension: String,

    file_compression_type: FileCompressionType,
}

impl ListingHMMDomTabTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::HMMDOMTAB.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();
        Ok(Arc::new(schema))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = HMMDomTabScan::new(conf.clone(), self.file_compression_type);
        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A HMM Dom listing table
pub struct ListingHMMDomTabTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingHMMDomTabTableOptions,
}

impl ListingHMMDomTabTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingHMMDomTabTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingHMMDomTabTable {
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
        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

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
}

#[cfg(test)]
mod tests {
    use crate::datasources::{ExonFileType, ExonListingTableFactory};

    use datafusion::{common::FileCompressionType, prelude::SessionContext};
    use exon_test::test_listing_table_url;

    #[tokio::test]
    async fn test_listing() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("hmmdomtab");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::HMMDOMTAB,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
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

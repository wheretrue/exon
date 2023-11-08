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
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics},
    prelude::Expr,
};
use futures::TryStreamExt;

use crate::{
    datasources::hive_partition::filter_matches_partition_cols,
    physical_plan::object_store::pruned_partition_list,
};

use super::{scanner::SAMScan, SAMSchemaBuilder};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingSAMTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingSAMTableOptions>,
}

impl ListingSAMTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingSAMTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone, Default)]
/// Listing options for a SAM table
pub struct ListingSAMTableOptions {
    /// The file extension for the SAM file
    file_extension: String,

    /// The table partition columns
    table_partition_cols: Vec<(String, DataType)>,
}

impl ListingSAMTableOptions {
    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<(SchemaRef, Vec<usize>)> {
        let partition_fields = self
            .table_partition_cols
            .iter()
            .map(|f| Field::new(&f.0, f.1.clone(), false))
            .collect::<Vec<_>>();

        let builder = SAMSchemaBuilder::default().with_partition_fields(partition_fields.clone());

        let (schema, file_projection) = builder.build();

        Ok((Arc::new(schema), file_projection))
    }

    /// Add table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<(String, DataType)>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = SAMScan::new(conf);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A SAM listing table
pub struct ListingSAMTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    file_projection: Vec<usize>,

    options: ListingSAMTableOptions,
}

impl ListingSAMTable {
    /// Create a new SAM listing table
    pub fn try_new(
        config: ListingSAMTableConfig,
        table_schema: Arc<Schema>,
        file_projection: Vec<usize>,
    ) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            file_projection,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }

    /// File Schema
    pub fn file_schema(&self) -> Result<SchemaRef> {
        let file_schema = &self.table_schema.project(&self.file_projection)?;
        Ok(Arc::new(file_schema.clone()))
    }
}

#[async_trait]
impl TableProvider for ListingSAMTable {
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

        let file_scan_config = FileScanConfig {
            object_store_url,
            file_schema: self.file_schema()?,
            file_groups: vec![file_list],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            output_ordering: Vec::new(),
            table_partition_cols: self.options.table_partition_cols.clone(),
            infinite_source: false,
        };

        let plan = self.options.create_physical_plan(file_scan_config).await?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use crate::datasources::{ExonFileType, ExonListingTableFactory};

    use datafusion::{
        datasource::file_format::file_compression_type::FileCompressionType,
        prelude::SessionContext,
    };
    use exon_test::test_listing_table_url;

    #[tokio::test]
    async fn test_table_provider() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("sam");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::SAM,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
            )
            .await?;

        let df = ctx.read_table(table.clone()).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 1);

        Ok(())
    }
}

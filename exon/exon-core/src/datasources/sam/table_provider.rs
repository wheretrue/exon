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
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        physical_plan::FileScanConfig, TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics},
    prelude::Expr,
};
use exon_common::TableSchema;
use exon_sam::SAMSchemaBuilder;
use futures::{StreamExt, TryStreamExt};
use noodles::sam::alignment::RecordBuf;
use tokio_util::io::StreamReader;

use crate::{
    datasources::{
        exon_listing_table_options::{ExonListingConfig, ExonListingOptions},
        hive_partition::filter_matches_partition_cols,
    },
    physical_plan::object_store::pruned_partition_list,
};

use super::SAMScan;

#[derive(Debug, Clone, Default)]
/// Listing options for a SAM table
pub struct ListingSAMTableOptions {
    /// The file extension for the SAM file
    file_extension: String,

    /// The table partition columns
    table_partition_cols: Vec<Field>,

    /// Whether to infer the schema from the tags
    tag_as_struct: bool,
}

#[async_trait]
impl ExonListingOptions for ListingSAMTableOptions {
    fn table_partition_cols(&self) -> Vec<Field> {
        self.table_partition_cols
    }

    fn file_extension(&self) -> String {
        self.file_extension
    }

    fn file_compression_type(&self) -> FileCompressionType {
        FileCompressionType::UNCOMPRESSED
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = SAMScan::new(conf);

        Ok(Arc::new(scan))
    }
}

impl ListingSAMTableOptions {
    /// Infer the schema for the table
    pub async fn infer_schema(
        &self,
        state: &SessionState,
        table_path: &ListingTableUrl,
    ) -> datafusion::error::Result<TableSchema> {
        if !self.tag_as_struct {
            let builder = SAMSchemaBuilder::default()
                .with_partition_fields(self.table_partition_cols.clone()); // TODO: get rid of clone
            let table_schema = builder.build();

            return Ok(table_schema);
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

        let mut schema_builder = SAMSchemaBuilder::default();

        while let Some(f) = files.next().await {
            let f = f?;

            let get_result = store.get(&f.location).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);
            let mut reader = noodles::sam::AsyncReader::new(stream_reader);

            let header = reader.read_header().await?;

            let mut record = RecordBuf::default();

            reader.read_record_buf(&header, &mut record).await?;

            let data = record.data();
            schema_builder = schema_builder.with_tags_data_type_from_data(data)?;
        }

        schema_builder = schema_builder.with_partition_fields(self.table_partition_cols.clone()); // TODO: get rid of clone

        let table_schema = schema_builder.build();

        Ok(table_schema)
    }

    /// Add table partition columns
    pub fn with_table_partition_cols(self, table_partition_cols: Vec<Field>) -> Self {
        Self {
            table_partition_cols,
            ..self
        }
    }

    /// Update the tag_as_struct option
    pub fn with_tag_as_struct(self, tag_as_struct: bool) -> Self {
        Self {
            tag_as_struct,
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// A SAM listing table
pub struct ListingSAMTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingSAMTable<T> {
    /// Create a new SAM listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonListingOptions> TableProvider for ListingSAMTable<T> {
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
            .map(|f| filter_matches_partition_cols(f, &self.config.options.table_partition_cols()))
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
            &self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_schema = self.table_schema.file_schema()?;
        let file_scan_config = FileScanConfig {
            object_store_url,
            file_schema: file_schema.clone(),
            file_groups: vec![file_list],
            statistics: Statistics::new_unknown(&file_schema),
            projection: projection.cloned(),
            limit,
            output_ordering: Vec::new(),
            table_partition_cols: self.config.options.table_partition_cols().to_vec(),
        };

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
    async fn test_table_provider() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("sam");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::SAM,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                Vec::new(),
                &HashMap::new(),
            )
            .await?;

        let df = ctx.read_table(table.clone())?;

        let mut row_cnt = 0;
        let bs = df.collect().await?;
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 1);

        Ok(())
    }
}

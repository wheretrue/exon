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
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use exon_gff::GFFSchemaBuilder;

use crate::{
    datasources::ExonFileType, physical_plan::file_scan_config_builder::FileScanConfigBuilder,
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
        // let partition_fields = self
        //     .table_partition_cols
        //     .iter()
        //     .map(|(name, data_type)| Field::new(name, data_type.clone(), false))
        //     .collect::<Vec<_>>();

        // let schema = GFFSchemaBuilder::default().extend(partition_fields).build();

        let schema = GFFSchemaBuilder::default().build();

        Ok(schema)
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
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
            .map(|f| {
                if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = f {
                    if *op == Operator::Eq {
                        if let Expr::Column(c) = &**left {
                            if let Expr::Literal(_) = &**right {
                                let name = &c.name;

                                if self
                                    .options
                                    .table_partition_cols
                                    .iter()
                                    .any(|(n, _)| n == name)
                                {
                                    return TableProviderFilterPushDown::Exact;
                                } else {
                                    return TableProviderFilterPushDown::Unsupported;
                                }
                            }
                        }
                    }
                }

                return TableProviderFilterPushDown::Unsupported;
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

        let col_strings = self
            .options
            .table_partition_cols
            .iter()
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();

        let partitioned_file_lists = vec![
            crate::physical_plan::object_store::list_files_for_scan(
                object_store,
                self.table_paths.clone(),
                &self.options.file_extension,
                &col_strings,
            )
            .await?,
        ];

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
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

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&self.file_schema),
            partitioned_file_lists,
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.options.table_partition_cols.clone())
        .limit_option(limit)
        .build();

        // https://github.com/apache/arrow-datafusion/blob/9b45967edc6dba312ea223464dad3e66604d2095/datafusion/core/src/datasource/listing/table.rs#L774
        let plan = self
            .options
            .create_physical_plan(file_scan_config, filters.as_ref())
            .await?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::{common::FileCompressionType, prelude::SessionContext};
    use exon_test::test_listing_table_url;

    use crate::datasources::{ExonFileType, ExonListingTableFactory};

    #[tokio::test]
    async fn test_listing() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("gff-partition");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::GFF,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
                vec![("sample".to_string(), DataType::Utf8)],
            )
            .await?;

        let df = ctx.read_table(table).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }

        assert_eq!(row_cnt, 5000);

        Ok(())
    }
}

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
        file_format::file_compression_type::FileCompressionType,
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
use exon_common::TableSchema;
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    datasources::{hive_partition::filter_matches_partition_cols, ExonFileType},
    physical_plan::object_store::pruned_partition_list,
};

use crate::physical_plan::file_scan_config_builder::FileScanConfigBuilder;

use super::{config::FCSSchemaBuilder, reader::FcsReader, scanner::FCSScan};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingFCSTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingFCSTableOptions>,
}

impl ListingFCSTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingFCSTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for an FCS table
pub struct ListingFCSTableOptions {
    /// The file extension for the table
    file_extension: String,

    /// The file compression type for the table
    file_compression_type: FileCompressionType,

    /// The table partition columns
    table_partition_cols: Vec<Field>,
}

impl ListingFCSTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::FCS.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            table_partition_cols: vec![],
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
    pub async fn infer_schema(
        &self,
        state: &SessionState,
        table_path: &ListingTableUrl,
    ) -> datafusion::error::Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let objects = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            self.file_extension.as_str(),
            None,
        )
        .await
        .try_collect::<Vec<_>>()
        .await
        .map_err(DataFusionError::from)?;

        let (schema_ref, projection) = self.infer_from_object_meta(&store, &objects).await?;

        let table_schema = TableSchema::new(schema_ref, projection);
        Ok(table_schema)
    }

    async fn infer_from_object_meta(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<(SchemaRef, Vec<usize>)> {
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let fcs_reader = FcsReader::new(stream_reader).await?;
        let parameter_names = fcs_reader.text_data.parameter_names();

        // Create the schema assuming all columns are of type `Float32`
        let mut fields = Vec::with_capacity(parameter_names.len());
        for parameter_name in parameter_names {
            fields.push(arrow::datatypes::Field::new(
                parameter_name,
                arrow::datatypes::DataType::Float32,
                false,
            ));
        }

        let mut file_schema = FCSSchemaBuilder::new();
        file_schema.add_file_fields(fields);
        file_schema.add_partition_fields(self.table_partition_cols.clone());

        let (schema, projection) = file_schema.build();

        Ok((Arc::new(schema), projection))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = FCSScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A FCS listing table
pub struct ListingFCSTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: TableSchema,

    options: ListingFCSTableOptions,
}

impl ListingFCSTable {
    /// Create a new FCS listing table
    pub fn try_new(config: ListingFCSTableConfig, table_schema: TableSchema) -> Result<Self> {
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
impl TableProvider for ListingFCSTable {
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
        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
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
                .table_partition_cols(self.options.table_partition_cols.clone())
                .limit_option(limit)
                .build();

        let plan = self.options.create_physical_plan(file_scan_config).await?;

        Ok(plan)
    }
}

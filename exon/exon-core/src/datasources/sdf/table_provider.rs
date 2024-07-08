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
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
        physical_plan::FileScanConfig, TableProvider, TableType,
    },
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
};
use exon_common::TableSchema;
use futures::TryStreamExt;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

use crate::{
    datasources::{
        exon_listing_table_options::{ExonListingConfig, ExonListingOptions},
        hive_partition::filter_matches_partition_cols,
    },
    physical_plan::{
        file_scan_config_builder::FileScanConfigBuilder, object_store::pruned_partition_list,
    },
};

use super::scanner::SDFScan;

#[derive(Debug)]
/// Options specific to the SDF File format.
pub struct ListingSDFTableOptions {
    /// The extension of the SDF file.
    file_extension: String,

    /// The file compression format.
    file_compression_type: FileCompressionType,

    /// A list of partitioned columns
    table_partition_cols: Vec<Field>,
}

impl Default for ListingSDFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: "sdf".to_string(),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            table_partition_cols: Vec::new(),
        }
    }
}

#[async_trait]
impl ExonListingOptions for ListingSDFTableOptions {
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
        _conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

impl ListingSDFTableOptions {
    /// Update the table partition columns
    pub fn with_table_partition_cols(mut self, table_partition_cols: Vec<Field>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Infer the schema of the files in the table
    pub async fn infer_schema<'a>(
        &'a self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> datafusion::common::Result<TableSchema> {
        let store = state.runtime_env().object_store(table_path)?;

        let files = exon_common::object_store_files_from_table_path(
            &store,
            table_path.as_ref(),
            table_path.prefix(),
            self.file_extension.as_str(),
            None,
        )
        .await;

        // collect the files as a slice
        let files = files
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Unable to get path info: {}", e)))?;

        self.infer_schema_from_object_meta(state, &store, &files)
            .await
    }

    async fn infer_schema_from_object_meta(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<TableSchema> {
        if objects.is_empty() {
            return Err(DataFusionError::Execution(
                "No objects found in the table path".to_string(),
            ));
        }

        let f = match store.get(&objects[0].location).await?.payload {
            GetResultPayload::Stream(_) => {
                return Err(DataFusionError::Execution(
                    "Cannot infer schema from stream".to_string(),
                ));
            }
            GetResultPayload::File(f, _) => f,
        };

        let reader = std::io::BufReader::new(f);
        let mut sdf_reader = exon_sdf::Reader::new(reader);

        let record = if let Some(r) = sdf_reader.read_record().unwrap() {
            r
        } else {
            return Err(DataFusionError::Execution(
                "No records found in the table path".to_string(),
            ));
        };

        let mut schema_builder = exon_sdf::SDFSchemaBuilder::default();
        schema_builder.update_data_field(record.data());

        Ok(schema_builder.build())
    }
}

#[derive(Debug, Clone)]
/// A SDF listing table
pub struct ListingSDFTable<T> {
    table_schema: TableSchema,

    config: ExonListingConfig<T>,
}

impl<T> ListingSDFTable<T> {
    /// Create a new VCF listing table
    pub fn new(config: ExonListingConfig<T>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            config,
        }
    }
}

#[async_trait]
impl<T: ExonListingOptions + 'static> TableProvider for ListingSDFTable<T> {
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
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
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
            self.config.options.file_extension(),
            self.config.options.table_partition_cols(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&self.table_schema.file_schema()?),
            vec![file_list],
        )
        .projection_option(projection.cloned())
        .table_partition_cols(self.config.options.table_partition_cols().to_vec())
        .limit_option(limit)
        .build();

        let scan = SDFScan::new(
            file_scan_config,
            self.config.options.file_compression_type(),
        );

        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::{datasource::listing::ListingTableUrl, prelude::SessionContext};

    use crate::{datasources::sdf::ListingSDFTableOptions, ExonError};

    #[tokio::test]
    async fn test_infer_schema() -> crate::Result<()> {
        let options = ListingSDFTableOptions::default();

        let ctx = SessionContext::new();

        let cargo_manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let file_path = format!(
            "{}/test-data/datasources/sdf/tox_benchmark_N6512.sdf",
            cargo_manifest
        );
        let table_path = ListingTableUrl::parse(&file_path)?;

        let schema = options.infer_schema(&ctx.state(), &table_path).await?;

        assert_eq!(schema.fields().len(), 4);

        let fields = schema.fields();

        assert_eq!(fields[0].name(), "header");
        assert_eq!(fields[1].name(), "atom_count");
        assert_eq!(fields[2].name(), "bond_count");
        assert_eq!(fields[3].name(), "data");

        let data_field = if let DataType::Struct(f) = fields[3].data_type() {
            f
        } else {
            return Err(ExonError::ArrowError(
                arrow::error::ArrowError::SchemaError("Expected struct".to_string()),
            ));
        };

        let data_field_names = data_field.iter().map(|f| f.name()).collect::<Vec<_>>();
        assert_eq!(data_field_names, vec!["canonical_smiles", "molblock"]);

        Ok(())
    }
}

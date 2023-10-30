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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{
    error::DataFusionError,
    physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan, Partitioning},
};

use futures::stream;

use crate::exome_catalog_manager::{
    CatalogName, Change, CreateTable, ExomeCatalogManager, LibraryName, SchemaName, TableName,
};

use super::CHANGE_SCHEMA;

#[derive(Debug, Clone)]
pub struct CreateTableExec {
    name: TableName,
    schema_name: SchemaName,
    catalog_name: CatalogName,
    library_name: LibraryName,

    location: String,
    file_format: String,
    is_listing: bool,
    compression_type: String,
}

impl CreateTableExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: TableName,
        schema_name: SchemaName,
        catalog_name: CatalogName,
        library_name: LibraryName,
        location: String,
        file_format: String,
        is_listing: bool,
        compression_type: String,
    ) -> Self {
        Self {
            name,
            catalog_name,
            schema_name,
            library_name,
            location,
            file_format,
            is_listing,
            compression_type,
        }
    }

    pub async fn create_table(
        self,
        manager: Arc<ExomeCatalogManager>,
    ) -> Result<RecordBatch, DataFusionError> {
        let changes = vec![Change::CreateTable(CreateTable::new(
            self.name,
            self.schema_name,
            self.catalog_name,
            self.library_name,
            self.location,
            self.file_format,
            self.is_listing,
            self.compression_type,
        ))];

        manager
            .apply_changes(changes)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Error applying changes: {}", e)))?;

        Ok(RecordBatch::new_empty(CHANGE_SCHEMA.clone()))
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CreateCatalogExec")
    }
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        CHANGE_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CreateTableExec {
            name: self.name.clone(),
            schema_name: self.schema_name.clone(),
            catalog_name: self.catalog_name.clone(),
            library_name: self.library_name.clone(),
            location: self.location.clone(),
            file_format: self.file_format.clone(),
            is_listing: self.is_listing,
            compression_type: self.compression_type.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "CreateTableExec only supports 1 partition".to_string(),
            ));
        }

        let exome_catalog_manager = match context
            .session_config()
            .get_extension::<ExomeCatalogManager>()
        {
            Some(exome_catalog_manager) => exome_catalog_manager,
            None => {
                return Err(DataFusionError::Execution(
                    "ExomeCatalogManager not found".to_string(),
                ))
            }
        };

        let this = self.clone();
        let stream = stream::once(this.create_table(exome_catalog_manager));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            CHANGE_SCHEMA.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }
}

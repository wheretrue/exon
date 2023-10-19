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

use datafusion::{
    error::DataFusionError,
    physical_plan::{DisplayAs, ExecutionPlan, Partitioning},
};

use crate::exome_catalog_manager::{Change, CreateCatalog, ExomeCatalogManager};

use super::CHANGE_SCHEMA;

#[derive(Debug, Clone)]
pub struct CreateCatalogExec {
    name: String,
    library_id: String,
}

impl CreateCatalogExec {
    pub fn new(name: String, library_id: String) -> Self {
        Self { name, library_id }
    }
}

impl DisplayAs for CreateCatalogExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CreateCatalogExec")
    }
}

impl ExecutionPlan for CreateCatalogExec {
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
        Ok(Arc::new(CreateCatalogExec {
            name: self.name.clone(),
            library_id: self.library_id.clone(),
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

        let exome_catalog_manager = context
            .session_config()
            .get_extension::<ExomeCatalogManager>()
            .unwrap();

        let name = self.name.clone();
        let library_id = self.library_id.clone();

        tokio::spawn(async move {
            let changes = vec![Change::CreateCatalog(CreateCatalog::new(
                name.clone(),
                library_id.clone(),
            ))];

            let result = exome_catalog_manager
                .apply_changes(changes)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Error applying changes: {}", e)));

            match result {
                Ok(_) => {}
                Err(e) => {
                    println!("Error applying changes: {}", e);
                }
            }
        });

        // Create an empty stream
        Ok(Box::pin(
            datafusion::physical_plan::EmptyRecordBatchStream::new(self.schema()),
        ))
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        todo!()
    }
}

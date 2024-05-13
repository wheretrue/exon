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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    common::DFSchema,
    datasource::file_format::{csv::CsvSink, json::JsonSink, parquet::ParquetSink},
    error::Result,
    execution::context::SessionState,
    logical_expr::{Expr, LogicalPlan},
    physical_plan::{insert::DataSinkExec, ExecutionPlan, PhysicalExpr},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};

use crate::ExonRuntimeEnvExt;

#[derive(Default)]
/// A custom PhysicalPlanner that adds Exon-specific functionality.
pub struct ExonPhysicalPlanner {
    planner: DefaultPhysicalPlanner,
}

#[async_trait]
impl PhysicalPlanner for ExonPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .planner
            .create_physical_plan(logical_plan, session_state)
            .await?;

        let runtime = session_state.runtime_env();

        // try to downcast plan as FileSinkExec
        if let Some(file_sink) = plan.as_any().downcast_ref::<DataSinkExec>() {
            let sink = file_sink.sink();

            // Try to downcast to a ParquetSink
            if let Some(parquet_sink) = sink.as_any().downcast_ref::<ParquetSink>() {
                let config = parquet_sink.config();
                let url = config.object_store_url.as_ref();
                runtime.exon_register_object_store_url(url).await?;
            }

            // Try to downcast a JsonSink
            if let Some(json_sink) = sink.as_any().downcast_ref::<JsonSink>() {
                let url = json_sink.config().object_store_url.as_ref();
                runtime.exon_register_object_store_url(url).await?;
            }

            // Try to downcast to CsvSink
            if let Some(csv_sink) = sink.as_any().downcast_ref::<CsvSink>() {
                let url = csv_sink.config().object_store_url.as_ref();
                runtime.exon_register_object_store_url(url).await?;
            }
        }

        Ok(plan)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.planner
            .create_physical_expr(expr, input_dfschema, session_state)
    }
}

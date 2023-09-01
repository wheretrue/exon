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

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::Expr;
use datafusion::{
    execution::context::SessionState, logical_expr::LogicalPlan, physical_plan::ExecutionPlan,
    physical_planner::PhysicalPlanner,
};

use datafusion::error::Result;

/// Exon physical planner.
pub struct ExonPhysicalPlanner {
    inner: datafusion::physical_planner::DefaultPhysicalPlanner,
}

impl Default for ExonPhysicalPlanner {
    fn default() -> Self {
        Self {
            inner: datafusion::physical_planner::DefaultPhysicalPlanner::default(),
        }
    }
}

#[async_trait]
impl PhysicalPlanner for ExonPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_physical_plan(logical_plan, session_state)
            .await
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner
            .create_physical_expr(expr, input_dfschema, input_schema, session_state)
    }
}

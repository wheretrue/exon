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
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::PhysicalPlanner,
};

use super::exon_physical_planner::ExonPhysicalPlanner;

/// A custom PhysicalPlanner that adds Exon-specific functionality.
#[derive(Debug, Default)]
pub struct ExonQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExonQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = ExonPhysicalPlanner::default();
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

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

use datafusion::common::tree_node::Transformed;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::expressions::BinaryExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use crate::physical_plan::interval_physical_expr::IntervalPhysicalExpr;

fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let plan = if plan.children().is_empty() {
        Transformed::No(plan)
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| optimize(child.clone()).map(Transformed::into))
            .collect::<Result<Vec<_>>>()?;

        with_new_children_if_necessary(plan, children)?
    };

    let (plan, _transformed) = plan.into_pair();

    let filter_exec = if let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() {
        filter_exec
    } else {
        return Ok(Transformed::No(plan));
    };

    let pred = match filter_exec
        .predicate()
        .as_any()
        .downcast_ref::<BinaryExpr>()
    {
        Some(expr) => expr,
        None => return Ok(Transformed::No(plan)),
    };

    let interval_expr = match IntervalPhysicalExpr::try_from(pred.clone()) {
        Ok(expr) => expr,
        Err(_) => return Ok(Transformed::No(plan)),
    };

    let exec = FilterExec::try_new(Arc::new(interval_expr), filter_exec.input().clone())?;

    Ok(Transformed::Yes(Arc::new(exec)))
}

#[derive(Default)]
pub struct ExonIntervalOptimizer {}

impl PhysicalOptimizerRule for ExonIntervalOptimizer {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        let plan = optimize(plan)?;
        let (plan, _transformed) = plan.into_pair();

        Ok(plan)
    }

    fn name(&self) -> &str {
        "exon_interval_optimizer_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use datafusion::{physical_plan::filter::FilterExec, prelude::SessionContext};
    use noodles::core::region::Interval;

    use crate::{physical_plan::interval_physical_expr::IntervalPhysicalExpr, ExonSessionExt};

    #[tokio::test]
    async fn test_interval_rule_eq() {
        let ctx = SessionContext::new_exon();

        let sql = "CREATE TABLE test AS (SELECT 1 as pos UNION ALL SELECT 2 as pos)";
        ctx.sql(sql).await.unwrap();

        let sql = "SELECT * FROM test WHERE pos = 1";
        let df = ctx.sql(sql).await.unwrap();

        let logical_plan = df.logical_plan();

        let optimized_plan = ctx
            .state()
            .create_physical_plan(logical_plan)
            .await
            .unwrap();

        // Downcast to FilterExec and check that the predicate is a IntervalPhysicalExpr
        let filter_exec = optimized_plan
            .as_any()
            .downcast_ref::<FilterExec>()
            .unwrap();

        let pred = filter_exec
            .predicate()
            .as_any()
            .downcast_ref::<IntervalPhysicalExpr>()
            .unwrap();

        let expected_interval = Interval::from_str("1-1").unwrap();

        assert_eq!(pred.interval().unwrap(), expected_interval);
    }
}

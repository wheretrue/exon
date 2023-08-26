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
use datafusion::physical_plan::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

fn transform_expression(binary_expression: &BinaryExpr) -> Option<BinaryExpr> {
    let left = match binary_expression
        .left()
        .as_any()
        .downcast_ref::<BinaryExpr>()
    {
        Some(expr) => expr,
        None => return None,
    };

    let right = match binary_expression
        .right()
        .as_any()
        .downcast_ref::<BinaryExpr>()
    {
        Some(expr) => expr,
        None => return None,
    };

    // second check that the left expression is a (chrom = '1' AND pos >= 2) and the right is a
    // (pos <= 3)
    let left_chrom = match left.left().as_any().downcast_ref::<BinaryExpr>() {
        Some(expr) => expr,
        None => return None,
    };

    // Check the left_chrom is a Column and the right is a Literal
    let left_chrom_col = match left_chrom.left().as_any().downcast_ref::<Column>() {
        Some(expr) => expr,
        None => return None,
    };

    if left_chrom_col.name() != "chrom"
        || left_chrom.op() != &datafusion::logical_expr::Operator::Eq
    {
        return None;
    }

    let left_pos = match left.right().as_any().downcast_ref::<BinaryExpr>() {
        Some(expr) => expr,
        None => return None,
    };

    // Check the left_pos is a Column and the right is a Literal
    let left_pos_col = match left_pos.left().as_any().downcast_ref::<Column>() {
        Some(expr) => expr,
        None => return None,
    };

    if left_pos_col.name() != "pos" || left_pos.op() != &datafusion::logical_expr::Operator::GtEq {
        return None;
    }

    let right_pos_col = match right.left().as_any().downcast_ref::<Column>() {
        Some(expr) => expr,
        None => return None,
    };

    if right_pos_col.name() != "pos" || right.op() != &datafusion::logical_expr::Operator::LtEq {
        return None;
    }

    let pos_expr = BinaryExpr::new(
        Arc::new(left_pos.clone()),
        datafusion::logical_expr::Operator::And,
        Arc::new(right.clone()),
    );

    let full_expr = BinaryExpr::new(
        Arc::new(left_chrom.clone()),
        datafusion::logical_expr::Operator::And,
        Arc::new(pos_expr),
    );

    Some(full_expr)
}

fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let new_plan = if plan.children().is_empty() {
        Transformed::No(plan)
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| optimize(child.clone()).map(Transformed::into))
            .collect::<Result<Vec<_>>>()?;

        with_new_children_if_necessary(plan, children)?
    };

    let (plan, _transformed) = new_plan.into_pair();

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

    if let Some(expr) = transform_expression(pred) {
        let exec = FilterExec::try_new(Arc::new(expr), filter_exec.input().clone())?;
        Ok(Transformed::Yes(Arc::new(exec)))
    } else {
        Ok(Transformed::No(plan))
    }
}

#[derive(Default)]
pub struct RegionBetweenRule {}

impl PhysicalOptimizerRule for RegionBetweenRule {
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
        "region_optimize_between"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::expressions::{col, lit, BinaryExpr, Column, Literal};

    use crate::optimizer::region_between_rewriter::transform_expression;

    #[tokio::test]
    async fn test_region_between_rule() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("chrom", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int32, false),
        ]));

        let chrom_expr = col("chrom", &schema).unwrap();
        let chrom_lit = lit("1");

        let full_chrom_expr = BinaryExpr::new(
            chrom_expr,
            datafusion::logical_expr::Operator::Eq,
            chrom_lit,
        );

        let left_pos_expr = col("pos", &schema).unwrap();
        let left_pos_lit = lit(2);

        let bin_expr = BinaryExpr::new(
            left_pos_expr,
            datafusion::logical_expr::Operator::GtEq,
            left_pos_lit,
        );

        let left_expr = BinaryExpr::new(
            Arc::new(full_chrom_expr),
            datafusion::logical_expr::Operator::And,
            Arc::new(bin_expr),
        );

        let right_pos_expr = col("pos", &schema).unwrap();
        let right_pos_lit = lit(3);

        let bin_expr = BinaryExpr::new(
            right_pos_expr,
            datafusion::logical_expr::Operator::LtEq,
            right_pos_lit,
        );

        let full_expr = BinaryExpr::new(
            Arc::new(left_expr),
            datafusion::logical_expr::Operator::And,
            Arc::new(bin_expr),
        );

        let actual_expr = transform_expression(&full_expr).unwrap();

        // Assert left is chrom = '1'
        let left_chrom = actual_expr
            .left()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();

        let left_chrom_col = left_chrom.left().as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(
            left_chrom_col.name(),
            "chrom",
            "left_chrom_col.name() != 'chrom'"
        );

        let left_chrom_lit = left_chrom
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(
            left_chrom_lit.value(),
            &datafusion::scalar::ScalarValue::Utf8(Some("1".to_owned())),
            "left_chrom_lit.value() != '1'"
        );
    }
}

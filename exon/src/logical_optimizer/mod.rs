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

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Between, BinaryExpr, Filter, LogicalPlan};
use datafusion::optimizer::{optimize_children, OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;

use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

use crate::udfs::vcf::{create_chrom_udf, create_interval_udf, create_region_udf};

fn between_to_interval_udf(expr: Expr) -> Result<Expr> {
    eprintln!("expr: {:?}", expr);

    expr.transform(&|expr| {
        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(column), Expr::Literal(literal)) => {
                        let interval_udf = create_chrom_udf().call(vec![
                            Expr::Column(column.clone()),
                            Expr::Literal(literal.clone()),
                        ]);

                        return Ok(Transformed::Yes(interval_udf));
                    }
                    (Expr::ScalarUDF(left_udf), Expr::ScalarUDF(right_udf)) => {
                        // TODO: stricter checks (e.g. func names equal)

                        let chrom_scalar = match left_udf.args[1] {
                            Expr::Literal(ScalarValue::Utf8(Some(ref chrom))) => chrom.clone(),
                            _ => {
                                return Ok(Transformed::Yes(Expr::BinaryExpr(BinaryExpr {
                                    left,
                                    op,
                                    right,
                                })))
                            }
                        };

                        let interval_scalar = match right_udf.args[1] {
                            Expr::Literal(ScalarValue::Utf8(Some(ref interval))) => {
                                interval.clone()
                            }
                            _ => {
                                return Ok(Transformed::Yes(Expr::BinaryExpr(BinaryExpr {
                                    left,
                                    op,
                                    right,
                                })))
                            }
                        };

                        let region_udf = create_region_udf().call(vec![
                            left_udf.args[0].clone(),
                            right_udf.args[0].clone(),
                            Expr::Literal(ScalarValue::Utf8(Some(format!(
                                "{}:{}",
                                chrom_scalar, interval_scalar
                            )))),
                        ]);

                        return Ok(Transformed::Yes(region_udf));
                    }
                    _ => {
                        return Ok(Transformed::Yes(Expr::BinaryExpr(BinaryExpr {
                            left,
                            op,
                            right,
                        })))
                    }
                }
            }

            // Expr::BinaryExpr(expr) if expr.op == Operator::Eq => {
            //     // We have a binary expression, the left should be a col and the right should be a literal
            //     let (column, literal) = match (expr.left.as_ref(), expr.right.as_ref()) {
            //         (Expr::Column(column), Expr::Literal(literal)) => (column, literal),
            //         _ => return Ok(Transformed::Yes(Expr::BinaryExpr(expr))),
            //     };

            //     let interval_udf = create_interval_udf().call(vec![
            //         Expr::Column(column.clone()),
            //         Expr::Literal(literal.clone()),
            //     ]);

            //     Transformed::Yes(interval_udf)
            // }
            Expr::Between(between) if !between.negated => {
                // We have a standard BETWEEN expression, first get the column, and make sure it's name is pos
                let column = match between.expr.as_ref() {
                    Expr::Column(column) => column,
                    _ => {
                        return Ok(Transformed::Yes(Expr::Between(Between::new(
                            between.expr.clone(),
                            between.negated,
                            between.low.clone(),
                            between.high.clone(),
                        ))))
                    }
                };

                // Now get the high and low, these should be literals
                let (low, high) = match (between.low.as_ref(), between.high.as_ref()) {
                    (Expr::Literal(low), Expr::Literal(high)) => (low, high),
                    _ => {
                        return Ok(Transformed::Yes(Expr::Between(Between::new(
                            between.expr.clone(),
                            between.negated,
                            between.low.clone(),
                            between.high.clone(),
                        ))))
                    }
                };

                let region_string = format!("{}-{}", low, high);

                let interval_udf = create_interval_udf().call(vec![
                    Expr::Column(column.clone()),
                    Expr::Literal(ScalarValue::Utf8(Some(region_string))),
                ]);

                Transformed::Yes(interval_udf)
            }
            _ => Transformed::No(expr),
        })
    })
}

struct PositionBetweenRewriter {}

impl OptimizerRule for PositionBetweenRewriter {
    fn name(&self) -> &str {
        "position_between_rewriter"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        eprintln!("plan: {:?}", plan);

        let optimized_plan = optimize_children(self, plan, config)?;
        eprintln!("optimized_plan: {:?}", optimized_plan);

        match plan {
            LogicalPlan::Filter(filters) => {
                let predicate = &filters
                    .predicate
                    .clone()
                    .map_children(between_to_interval_udf)?;

                let predicate = between_to_interval_udf(predicate.clone())?;

                let new_plan = Filter::try_new(predicate.clone(), filters.input.clone())?;

                Ok(Some(LogicalPlan::Filter(new_plan)))
            }
            _ => Ok(Some(plan.clone())),
        }
    }
}

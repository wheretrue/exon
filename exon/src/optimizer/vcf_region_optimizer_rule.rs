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
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use noodles::core::region::Interval;
use noodles::core::{Position, Region};

use crate::datasources::vcf::VCFScan;

fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // if we get a FilterExec with the correct expression and its input is a VCFScan, we can
    // replace the FilterExec with a VCFScan with the same expression and push down the filter

    if let Some(filter_exec) = plan.as_any().downcast_ref::<FilterExec>() {
        let vcf_scan = filter_exec.input().as_any().downcast_ref::<VCFScan>();

        filter_exec.predicate();

        let pred = filter_exec
            .predicate()
            .as_any()
            .downcast_ref::<datafusion::physical_expr::expressions::BinaryExpr>();

        match (vcf_scan, pred) {
            (Some(scan), Some(pred)) => {
                let left_pred = pred
                    .left()
                    .as_any()
                    .downcast_ref::<datafusion::physical_expr::expressions::BinaryExpr>()
                    .unwrap();

                // let left_column = left_pred
                //     .left()
                //     .as_any()
                //     .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                //     .unwrap();
                let right_literal = left_pred
                    .right()
                    .as_any()
                    .downcast_ref::<datafusion::physical_expr::expressions::Literal>()
                    .unwrap();

                let chrom = Some("chr1".to_string());

                // let chrom = match right_literal.value() {
                //     Expr::BinaryExpr(left) => chrom_operator(left),
                //     _ => None,
                // };

                // let pos = match pred.right.as_ref() {
                //     Expr::BinaryExpr(right) => {
                //         position_operator(right).map(|p| Position::new(p).unwrap())
                //     }
                //     _ => None,
                // };

                let pos = Some(Position::new(1).unwrap());

                match (chrom, pos) {
                    (Some(chrom), Some(pos)) => {
                        let start = pos;
                        let end = pos;
                        let interval = Interval::from(start..=end);

                        let region = Region::new(chrom, interval);

                        let new_scan = scan.clone().with_filter(region);

                        return Ok(Transformed::Yes(Arc::new(new_scan)));
                    }
                    (_, _) => return Ok(Transformed::No(plan)),
                }
            }
            (_, _) => return Ok(Transformed::No(plan)),
        }
    }

    Ok(Transformed::No(plan))
}

#[derive(Default)]
pub struct ExonVCFRegionOptimizer {}

impl PhysicalOptimizerRule for ExonVCFRegionOptimizer {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        eprintln!("ExonVCFRegionOptimizer::optimize");

        let plan = optimize(plan)?;

        let (plan, _transformed) = plan.into_pair();

        Ok(plan)
    }

    fn name(&self) -> &str {
        "exon_vcf_region_optimizer_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

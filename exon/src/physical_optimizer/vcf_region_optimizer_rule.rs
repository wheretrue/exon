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
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use crate::datasources::vcf::VCFScan;
use crate::physical_plan::region_physical_expr::RegionPhysicalExpr;

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

    let vcf_scan = match filter_exec.input().as_any().downcast_ref::<VCFScan>() {
        Some(scan) => scan,
        None => return Ok(Transformed::No(plan)),
    };

    let pred = match filter_exec
        .predicate()
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::BinaryExpr>()
    {
        Some(expr) => expr,
        None => return Ok(Transformed::No(plan)),
    };

    let region_expr: RegionPhysicalExpr = match RegionPhysicalExpr::try_from(pred.clone()) {
        Ok(expr) => expr,
        Err(_) => return Ok(Transformed::No(plan)),
    };

    let new_scan = vcf_scan.clone().with_filter(region_expr.region()?.clone());
    let new_filter = FilterExec::try_new(Arc::new(region_expr), Arc::new(new_scan))?;

    Ok(Transformed::Yes(Arc::new(new_filter)))
}

#[derive(Default)]
pub struct ExonVCFRegionOptimizer {}

impl PhysicalOptimizerRule for ExonVCFRegionOptimizer {
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
        "exon_vcf_region_optimizer_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_region_physical_expr() {
        // TODO: should this and the ability to register_exon_table be removed... or how should it handle things
        // so the registration can work with the optimizer
        // let ctx = SessionContext::new_exon();

        // let file_file = ExonFileType::from_str("vcf").unwrap();
        // let options = ExonReadOptions::new(file_file);

        // let path = test_path("vcf", "index.vcf");
        // let path = path.to_str().unwrap();
        // let query = "1";

        // ctx.register_exon_table("test_vcf", path, options)
        //     .await
        //     .unwrap();

        // // Check between
        // let sql = format!(
        //     "SELECT chrom, pos FROM test_vcf WHERE chrom = '{}' and pos BETWEEN 2 and 3",
        //     query
        // );

        // let df = ctx.sql(&sql).await.unwrap();
        // let logical_plan = df.logical_plan();

        // let optimized_plan = ctx
        //     .state()
        //     .create_physical_plan(logical_plan)
        //     .await
        //     .unwrap();

        // assert!(optimized_plan
        //     .as_any()
        //     .downcast_ref::<FilterExec>()
        //     .unwrap()
        //     .input()
        //     .as_any()
        //     .downcast_ref::<crate::datasources::vcf::VCFScan>()
        //     .is_some());

        // // Check eq
        // let sql = format!(
        //     "SELECT chrom, pos FROM test_vcf WHERE chrom = '{}' and pos = 2",
        //     query
        // );

        // let df = ctx.sql(&sql).await.unwrap();
        // let logical_plan = df.logical_plan();

        // let optimized_plan = ctx
        //     .state()
        //     .create_physical_plan(logical_plan)
        //     .await
        //     .unwrap();

        // assert!(optimized_plan
        //     .as_any()
        //     .downcast_ref::<FilterExec>()
        //     .unwrap()
        //     .input()
        //     .as_any()
        //     .downcast_ref::<crate::datasources::vcf::VCFScan>()
        //     .is_some());
    }
}

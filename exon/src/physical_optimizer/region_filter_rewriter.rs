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

use datafusion::error::Result;
use datafusion::{common::tree_node::Transformed, physical_plan::PhysicalExpr};

use crate::physical_plan::chrom_physical_expr::ChromPhysicalExpr;
use crate::physical_plan::interval_physical_expr::IntervalPhysicalExpr;
use crate::physical_plan::region_physical_expr::RegionPhysicalExpr;

use super::merging::{try_merge_chrom_exprs, try_merge_region_with_interval};

pub fn transform_region_expressions(
    e: Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    match e
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::BinaryExpr>()
    {
        Some(be) => {
            if let Ok(chrom_expr) = ChromPhysicalExpr::try_from(be.clone()) {
                let region_expr = RegionPhysicalExpr::new(Arc::new(chrom_expr), None);

                return Ok(Transformed::Yes(Arc::new(region_expr)));
            }

            if let Ok(interval_expr) = IntervalPhysicalExpr::try_from(be.clone()) {
                return Ok(Transformed::Yes(Arc::new(interval_expr)));
            }

            // Now we need to check if the left and right side can be merged in a single expression.

            // Case 1: left and right are both chrom expressions, and need to be downcast to chrom expressions
            if let Some(left_chrom) = be.left().as_any().downcast_ref::<ChromPhysicalExpr>() {
                if let Some(right_chrom) = be.right().as_any().downcast_ref::<ChromPhysicalExpr>() {
                    match try_merge_chrom_exprs(left_chrom, right_chrom) {
                        Ok(Some(new_expr)) => return Ok(Transformed::Yes(Arc::new(new_expr))),
                        Ok(None) => return Ok(Transformed::No(e)),
                        Err(e) => return Err(e),
                    }
                }
            }

            // Case 2: left is a chrom expression and right is an interval expression
            if let Some(_left_chrom) = be.left().as_any().downcast_ref::<ChromPhysicalExpr>() {
                if let Some(_right_interval) =
                    be.right().as_any().downcast_ref::<IntervalPhysicalExpr>()
                {
                    let new_expr =
                        RegionPhysicalExpr::new(be.left().clone(), Some(be.right().clone()));

                    return Ok(Transformed::Yes(Arc::new(new_expr)));
                }
            }

            // Case 3: left is a region expression and the right is an interval expression
            if let Some(left_region) = be.left().as_any().downcast_ref::<RegionPhysicalExpr>() {
                if let Some(right_interval) =
                    be.right().as_any().downcast_ref::<IntervalPhysicalExpr>()
                {
                    let new_region = try_merge_region_with_interval(left_region, right_interval)?;

                    if let Some(new_region) = new_region {
                        return Ok(Transformed::Yes(Arc::new(new_region)));
                    }
                }
            }

            Ok(Transformed::No(e))
        }
        None => Ok(Transformed::No(e)),
    }
}

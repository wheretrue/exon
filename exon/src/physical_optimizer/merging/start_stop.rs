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

use crate::physical_plan::start_end_interval_physical_expr::StartEndIntervalPhysicalExpr;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    error::Result,
    logical_expr::Operator,
    physical_plan::expressions::{col, lit, BinaryExpr, Column, Literal},
};

use super::intersect_ranges;

fn create_binary_expression(start: usize, end: usize) -> BinaryExpr {
    //  SELECT * FROM intervals
    // WHERE
    // start < 20 AND
    // end > 10;

    let start_schema = Schema::new(vec![Field::new("start", DataType::Int64, false)]);
    let end_schema = Schema::new(vec![Field::new("end", DataType::Int64, false)]);

    BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            col("start", &start_schema).unwrap(),
            Operator::Lt,
            lit(end as i64),
        )),
        Operator::And,
        Arc::new(BinaryExpr::new(
            col("end", &end_schema).unwrap(),
            Operator::Gt,
            lit(start as i64),
        )),
    )
}

// Helper function that combines two start/end intervals into a single interval.
pub fn try_merge_start_end_exprs(
    left: &StartEndIntervalPhysicalExpr,
    right: &StartEndIntervalPhysicalExpr,
) -> Result<Option<StartEndIntervalPhysicalExpr>> {
    let left_interval = left.interval_tuple();
    let right_interval = right.interval_tuple();

    let intersected_range = intersect_ranges(left_interval, right_interval);

    match intersected_range {
        Some((start, Some(end))) => {
            let binary_expr = create_binary_expression(start, end);

            Ok(Some(StartEndIntervalPhysicalExpr::new(
                start,
                Some(end),
                Arc::new(binary_expr),
            )))
        }
        _ => Ok(None),
    }
}

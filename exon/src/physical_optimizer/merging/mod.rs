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

use datafusion::error::{DataFusionError, Result};

mod start_stop;

use crate::physical_plan::{
    pos_interval_physical_expr::{pos_schema, PosIntervalPhysicalExpr},
    region_name_physical_expr::RegionNamePhysicalExpr,
    region_physical_expr::RegionPhysicalExpr,
};

/// Helper function to intersect two ranges
fn intersect_ranges(
    a: (usize, Option<usize>),
    b: (usize, Option<usize>),
) -> Option<(usize, Option<usize>)> {
    let (a_lower, a_upper) = a;
    let (b_lower, b_upper) = b;

    let lower_bound = std::cmp::max(a_lower, b_lower);
    let upper_bound = match (a_upper, b_upper) {
        (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };

    if let Some(upper) = upper_bound {
        if lower_bound > upper {
            return None;
        }
    }

    Some((lower_bound, upper_bound))
}

/// Merge two `RegionNamePhysicalExpr`s.
pub(crate) fn try_merge_region_name_exprs(
    left: &crate::physical_plan::region_name_physical_expr::RegionNamePhysicalExpr,
    right: &crate::physical_plan::region_name_physical_expr::RegionNamePhysicalExpr,
) -> Result<Option<crate::physical_plan::region_name_physical_expr::RegionNamePhysicalExpr>> {
    if left.field_value() == right.field_value() && left.field_name() == right.field_name() {
        Ok(Some(
            crate::physical_plan::region_name_physical_expr::RegionNamePhysicalExpr::new(
                left.field_name().to_string(),
                left.field_value().to_string(),
                left.inner().clone(),
            ),
        ))
    } else {
        Ok(None)
    }
}

/// Merge two `IntervalPhysicalExpr`s.
pub fn try_merge_interval_exprs(
    left: &PosIntervalPhysicalExpr,
    right: &PosIntervalPhysicalExpr,
) -> Result<Option<PosIntervalPhysicalExpr>> {
    match intersect_ranges(left.interval_tuple(), right.interval_tuple()) {
        Some((start, Some(end))) => {
            return Ok(Some(PosIntervalPhysicalExpr::new(
                start,
                Some(end),
                left.inner().clone(),
            )))
        }
        Some((start, None)) => {
            let schema = pos_schema();
            let interval_expr = PosIntervalPhysicalExpr::from_interval(start, None, &schema)?;

            Ok(Some(interval_expr))
        }
        _ => Ok(None),
    }
}

pub fn try_merge_region_with_interval(
    left: &RegionPhysicalExpr,
    right: &PosIntervalPhysicalExpr,
) -> Result<Option<RegionPhysicalExpr>> {
    let interval = match left.interval_expr() {
        Some(interval_expr) => interval_expr,
        None => {
            let new_interval = right;
            let new_interval = PosIntervalPhysicalExpr::new(
                new_interval.start(),
                new_interval.end(),
                new_interval.inner().clone(),
            );

            let field_name = left
                .region_name_expr()
                .ok_or(DataFusionError::Execution(
                    "Could not downcast left chrom expression to RegionNamePhysicalExpr"
                        .to_string(),
                ))?
                .field_name()
                .to_string();

            let field_value = left
                .region_name_expr()
                .ok_or(DataFusionError::Execution(
                    "Could not downcast left chrom expression to RegionNamePhysicalExpr"
                        .to_string(),
                ))?
                .field_value()
                .to_string();

            let new_expr = Arc::new(RegionNamePhysicalExpr::from_name_and_value(
                field_name,
                field_value,
            )?);

            let new_region = RegionPhysicalExpr::new(new_expr, Some(Arc::new(new_interval)));

            return Ok(Some(new_region));
        }
    };

    let interval = match try_merge_interval_exprs(interval, right)? {
        Some(interval) => interval,
        None => return Ok(None),
    };

    let left_expr = left.region_name_expr().ok_or(DataFusionError::Execution(
        "Could not downcast left chrom expression to RegionNamePhysicalExpr".to_string(),
    ))?;

    let region_name_expr = RegionNamePhysicalExpr::from_name_and_value(
        left_expr.field_name().to_string(),
        left_expr.field_value().to_string(),
    )?;

    let region_expr = RegionPhysicalExpr::new(Arc::new(region_name_expr), Some(Arc::new(interval)));

    Ok(Some(region_expr))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use datafusion::{
        physical_plan::expressions::{col, lit},
        scalar::ScalarValue,
    };
    use noodles::core::Position;

    use crate::{
        physical_optimizer::merging::{try_merge_interval_exprs, try_merge_region_name_exprs},
        physical_plan::{
            pos_interval_physical_expr::{pos_schema, PosIntervalPhysicalExpr},
            region_name_physical_expr::RegionNamePhysicalExpr,
        },
        tests::{and, gteq, lteq},
    };

    #[test]
    fn test_try_merge_with_right_chrom_expr() {
        // Try merging two chrom expressions with the same chromosome

        let schema = Schema::new(vec![arrow::datatypes::Field::new(
            "chrom",
            arrow::datatypes::DataType::Utf8,
            false,
        )]);

        let chrom_expr_1 = RegionNamePhysicalExpr::from_chrom("1", &schema).unwrap();
        let chrom_expr_2 = RegionNamePhysicalExpr::from_chrom("1", &schema).unwrap();

        let merged = try_merge_region_name_exprs(&chrom_expr_1, &chrom_expr_2)
            .unwrap()
            .unwrap();

        assert_eq!(merged.field_value(), "1");

        // Try merging two chrom expressions with different chromosomes
        let chrom_expr_3 = RegionNamePhysicalExpr::from_chrom("2", &schema).unwrap();

        let merged = try_merge_region_name_exprs(&chrom_expr_1, &chrom_expr_3).unwrap();
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_expr_without_overlap() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let left_expr = gteq(col("pos", &schema).unwrap(), lit(3));
        let left_interval = PosIntervalPhysicalExpr::try_from(left_expr).unwrap();

        let right_expr = lteq(col("pos", &schema).unwrap(), lit(4));
        let right_interval = PosIntervalPhysicalExpr::try_from(right_expr).unwrap();

        try_merge_interval_exprs(&left_interval, &right_interval)
            .unwrap()
            .unwrap();

        let right_expr = lteq(col("pos", &schema).unwrap(), lit(2));
        let right_interval = PosIntervalPhysicalExpr::try_from(right_expr).unwrap();

        let merged = try_merge_interval_exprs(&left_interval, &right_interval).unwrap();
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_with_interval_equal_intervals() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let inner_expression = and(
            Arc::new(gteq(col("pos", &schema).unwrap(), lit(1))),
            Arc::new(lteq(col("pos", &schema).unwrap(), lit(10))),
        );

        let inner = Arc::new(inner_expression);

        let right_interval = PosIntervalPhysicalExpr::new(1, Some(10), inner.clone());
        let left_interval = PosIntervalPhysicalExpr::new(1, Some(10), inner.clone());

        let merged = try_merge_interval_exprs(&left_interval, &right_interval)
            .unwrap()
            .unwrap();

        assert_eq!(merged.start(), 1);
        assert_eq!(merged.end(), Some(10));
    }

    #[test]
    fn test_interval_leq_and_geq() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let left_inner = gteq(col("pos", &schema).unwrap(), lit(ScalarValue::from(10)));
        let left = PosIntervalPhysicalExpr::try_from(left_inner).unwrap();

        let right_inner = lteq(col("pos", &schema).unwrap(), lit(ScalarValue::from(20)));
        let right = PosIntervalPhysicalExpr::try_from(right_inner).unwrap();

        let merged = try_merge_interval_exprs(&left, &right)
            .unwrap()
            .unwrap()
            .interval()
            .unwrap();

        assert_eq!(
            merged,
            noodles::core::region::Interval::from(
                Position::new(10).unwrap()..=Position::new(20).unwrap()
            )
        );
    }

    #[test]
    fn test_from_between_expr() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let gteq_expr = gteq(col("pos", &schema).unwrap(), lit(4));
        let gt_interval = super::PosIntervalPhysicalExpr::try_from(gteq_expr).unwrap();

        let lteq_expr = lteq(col("pos", &schema).unwrap(), lit(10));
        let lt_interval = super::PosIntervalPhysicalExpr::try_from(lteq_expr).unwrap();

        let interval = try_merge_interval_exprs(&gt_interval, &lt_interval)
            .unwrap()
            .unwrap();

        assert_eq!(
            interval.interval().unwrap(),
            noodles::core::region::Interval::from(
                Position::new(4).unwrap()..=Position::new(10).unwrap()
            )
        );
    }

    #[test]
    fn test_merge_intervals_no_upper_bound() {
        let schema = pos_schema();

        let left_expr = gteq(col("pos", &schema).unwrap(), lit(3));
        let left_interval = PosIntervalPhysicalExpr::try_from(left_expr).unwrap();

        let right_expr = gteq(col("pos", &schema).unwrap(), lit(4));
        let right_interval = PosIntervalPhysicalExpr::try_from(right_expr).unwrap();

        let merged = try_merge_interval_exprs(&left_interval, &right_interval)
            .unwrap()
            .unwrap();

        assert_eq!(merged.start(), 4);
    }

    #[test]
    fn test_intersect_ranges() {
        let test_table = vec![
            ((1, Some(2)), (3, Some(4)), None), // Non-overlapping
            ((1, Some(2)), (2, Some(4)), Some((2, Some(2)))), // Edge overlap
            ((1, None), (2, None), Some((2, None))), // Both upper bounds are None
            ((1, Some(5)), (2, None), Some((2, Some(5)))), // One upper bound is None
            ((2, Some(5)), (2, Some(5)), Some((2, Some(5)))), // Identical ranges
            ((1, Some(5)), (5, Some(9)), Some((5, Some(5)))), // Single-point overlap
            ((1, Some(5)), (10, None), None),   // Non-overlapping, one upper bound is None
        ];

        for test in test_table {
            let (a, b, expected) = test;

            let result = super::intersect_ranges(a, b);

            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_merge_region_interval() {}
}
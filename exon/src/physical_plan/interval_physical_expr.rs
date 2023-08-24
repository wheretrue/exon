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

use std::{any::Any, fmt::Display, hash::Hash, sync::Arc};

use datafusion::{
    error::DataFusionError,
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
};
use noodles::core::{
    region::{self, Interval},
    Position, Region,
};

use super::InvalidRegionError;

#[derive(Debug)]
pub struct IntervalPhysicalExpr {
    interval: Interval,
}

impl IntervalPhysicalExpr {
    pub fn new(interval: Interval) -> Self {
        Self { interval }
    }

    pub fn interval(&self) -> &Interval {
        &self.interval
    }
}

impl Display for IntervalPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IntervalPhysicalExpr {{ interval: {} }}", self.interval)
    }
}

impl From<Interval> for IntervalPhysicalExpr {
    fn from(interval: Interval) -> Self {
        Self::new(interval)
    }
}

impl TryFrom<BinaryExpr> for IntervalPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: BinaryExpr) -> Result<Self, Self::Error> {
        // This attempt does the following to check the incoming expression:
        // 1. Check if the left expression is a column and the right is a literal
        //    if this is the case, then check the operator. If the operator is
        //    an equality operator, then we can extract the pos and set the interval
        //    to be a single position interval.
        // 2. Check if the right expression is a column and the left is a literal
        //    this follows the same logic as above, but the column is on the right
        //    and the literal is on the left.

        let op = expr.op();

        // Check the case the left expression is a column and the right is a literal
        let left = expr.left().as_any().downcast_ref::<Column>();
        let right = expr.right().as_any().downcast_ref::<Literal>();

        if let (Some(col), Some(lit), datafusion::logical_expr::Operator::Eq) = (left, right, op) {
            if col.name() != "pos" {
                return Err(DataFusionError::External(InvalidRegionError.into()));
            } else {
                let pos = lit.value().to_string().parse::<usize>().unwrap();
                let start = Position::new(pos).unwrap();
                let end = Position::new(pos).unwrap();
                let interval = region::Interval::from(start..=end);

                return Ok(Self::from(interval));
            }
        };

        // Check the right expression is a column and the left is a literal
        let left = expr.left().as_any().downcast_ref::<Literal>();
        let right = expr.right().as_any().downcast_ref::<Column>();

        if let (Some(lit), Some(col), datafusion::logical_expr::Operator::Eq) = (left, right, op) {
            if col.name() != "pos" {
                return Err(DataFusionError::External(InvalidRegionError.into()));
            } else {
                let pos = lit.value().to_string().parse::<usize>().unwrap();
                let start = Position::new(pos).unwrap();
                let end = Position::new(pos).unwrap();
                let interval = region::Interval::from(start..=end);

                return Ok(Self::from(interval));
            }
        };

        // TODO: Check between and/or gt/lt operators
        Err(DataFusionError::External(InvalidRegionError.into()))
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for IntervalPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            Self::try_from(binary_expr.clone())
        } else {
            Err(DataFusionError::External(InvalidRegionError.into()))
        }
    }
}

impl PartialEq<dyn Any> for IntervalPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<IntervalPhysicalExpr>() {
            self.interval == other.interval
        } else {
            false
        }
    }
}

impl PhysicalExpr for IntervalPhysicalExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &arrow::datatypes::Schema,
    ) -> datafusion::error::Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }

    fn nullable(
        &self,
        _input_schema: &arrow::datatypes::Schema,
    ) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        let pos = batch
            .column_by_name("pos")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        let mut values = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let pos_value = pos.value(i);

            let is_in_interval = self
                .interval()
                .contains(Position::new(pos_value as usize).unwrap());

            values.push(is_in_interval);
        }

        Ok(datafusion::physical_plan::ColumnarValue::Array(Arc::new(
            arrow::array::BooleanArray::from(values),
        )))
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IntervalPhysicalExpr::from(
            self.interval().clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.interval().start().hash(&mut s);
        self.interval().end().hash(&mut s);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::BooleanArray, record_batch::RecordBatch};
    use datafusion::{
        logical_expr::Operator,
        physical_plan::{
            expressions::{col, lit, BinaryExpr},
            PhysicalExpr,
        },
        scalar::ScalarValue,
    };
    use noodles::core::{region::Interval, Position, Region};

    use crate::physical_plan::interval_physical_expr;

    #[test]
    fn test_from_binary_exprs() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let pos_expr = BinaryExpr::new(
            col("pos", &schema).unwrap(),
            Operator::Eq,
            lit(ScalarValue::from(4)),
        );

        let interval = super::IntervalPhysicalExpr::try_from(pos_expr).unwrap();

        assert_eq!(
            interval.interval(),
            &noodles::core::region::Interval::from(
                Position::new(4).unwrap()..=Position::new(4).unwrap()
            )
        );
    }

    #[tokio::test]
    async fn test_evaluate() {
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let interval = "1-1".parse::<Interval>().unwrap();

        let expr = interval_physical_expr::IntervalPhysicalExpr::from(interval);

        let result = match expr.evaluate(&batch).unwrap() {
            datafusion::physical_plan::ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };

        // Convert the result to a boolean array
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        let expected = BooleanArray::from(vec![Some(true), Some(false), Some(false)]);

        result
            .iter()
            .zip(expected.iter())
            .for_each(|(result, expected)| {
                assert_eq!(result, expected);
            });
    }
}

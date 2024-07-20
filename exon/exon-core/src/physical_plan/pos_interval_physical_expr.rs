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

use arrow::datatypes::SchemaRef;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::Operator,
    physical_plan::{
        expressions::{col, lit, BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
};
use noodles::core::region::Interval;

use crate::error::invalid_interval::InvalidIntervalError;

/// A physical expression that represents a genomic interval.
#[derive(Debug)]
pub struct PosIntervalPhysicalExpr {
    start: usize,
    end: Option<usize>,
    inner: Arc<dyn PhysicalExpr>,
}

impl PosIntervalPhysicalExpr {
    /// Create a new interval physical expression from an interval and an inner expression.
    pub fn new(start: usize, end: Option<usize>, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { start, end, inner }
    }

    /// Construct a noodles `Interval` from the start and end of this expression.
    pub fn interval(&self) -> Result<Interval> {
        match self.end {
            Some(end) => {
                let interval = format!("{}-{}", self.start, end)
                    .parse::<Interval>()
                    .map_err(|_| DataFusionError::External(InvalidIntervalError.into()))?;

                Ok(interval)
            }
            None => Err(DataFusionError::External(InvalidIntervalError.into())),
        }
    }

    /// Get the start of the interval.
    pub fn start(&self) -> usize {
        self.start
    }

    /// Get the end of the interval.
    pub fn end(&self) -> Option<usize> {
        self.end
    }

    /// Get the interval as a tuple of (start, end).
    pub fn interval_tuple(&self) -> (usize, Option<usize>) {
        (self.start, self.end)
    }

    /// Get a reference to the inner expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    /// Create a new `IntervalPhysicalExpr` from an interval and a schema.
    pub fn from_interval(start: usize, end: Option<usize>, schema: &SchemaRef) -> Result<Self> {
        let start_expr = BinaryExpr::new(col("pos", schema)?, Operator::GtEq, lit(start as i64));

        match end {
            Some(end) => {
                // If we have an end, do a leq against the end, and AND the start expr
                let end_expr =
                    BinaryExpr::new(col("pos", schema)?, Operator::LtEq, lit(end as i64));

                let interval_expr =
                    BinaryExpr::new(Arc::new(start_expr), Operator::And, Arc::new(end_expr));

                Ok(Self::new(start, Some(end), Arc::new(interval_expr)))
            }
            None => {
                // If there's no end, just return the start expr
                Ok(Self::new(start, None, Arc::new(start_expr)))
            }
        }
    }
}

impl Display for PosIntervalPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IntervalPhysicalExpr {{ start: {}, end: {:?} }}",
            self.start, self.end
        )
    }
}

impl TryFrom<BinaryExpr> for PosIntervalPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: BinaryExpr) -> Result<Self, Self::Error> {
        let op = expr.op();

        // Check the case the left expression is a column and the right is a literal
        let left = expr.left().as_any().downcast_ref::<Column>();
        let right = expr.right().as_any().downcast_ref::<Literal>();

        if let (Some(col), Some(lit), _) = (left, right, op) {
            if col.name() != "pos" {
                return Err(DataFusionError::External("Invalid column for pos".into()));
            } else {
                match op {
                    Operator::Eq => {
                        let pos = lit.value().to_string().parse::<usize>().unwrap();

                        return Ok(Self::new(pos, Some(pos), Arc::new(expr)));
                    }
                    Operator::GtEq => {
                        let pos = lit.value().to_string().parse::<usize>().unwrap();

                        return Ok(Self::new(pos, None, Arc::new(expr)));
                    }
                    Operator::LtEq => {
                        let pos = lit.value().to_string().parse::<usize>().unwrap();

                        return Ok(Self::new(1, Some(pos), Arc::new(expr)));
                    }
                    _ => return Err(DataFusionError::External("Invalid operator for pos".into())),
                }
            }
        };

        Err(DataFusionError::External(
            format!("invalid expression for pos: {}", expr).into(),
        ))
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for PosIntervalPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            Self::try_from(binary_expr.clone())
        } else {
            Err(DataFusionError::External(InvalidIntervalError.into()))
        }
    }
}

impl PartialEq<dyn Any> for PosIntervalPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.start == other.start && self.end == other.end
        } else {
            false
        }
    }
}

impl PartialEq for PosIntervalPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}

impl PhysicalExpr for PosIntervalPhysicalExpr {
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
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(PosIntervalPhysicalExpr::new(
            self.start,
            self.end,
            Arc::clone(&self.inner),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.start.hash(&mut s);
        self.end.hash(&mut s);
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
    };
    use noodles::core::Position;

    use crate::{
        physical_plan::pos_interval_physical_expr,
        tests::{eq, gteq},
    };

    use super::PosIntervalPhysicalExpr;

    #[test]
    fn test_call_interval_with_no_upper_bound() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let expr = gteq(col("pos", &schema).unwrap(), lit(4));
        let interval_expr =
            pos_interval_physical_expr::PosIntervalPhysicalExpr::try_from(expr).unwrap();

        assert_eq!(interval_expr.start, 4);
        assert_eq!(interval_expr.end, None);

        assert!(interval_expr.interval().is_err());
    }

    #[test]
    fn test_from_binary_exprs() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let pos_expr = eq(col("pos", &schema).unwrap(), lit(4));

        let interval = super::PosIntervalPhysicalExpr::try_from(pos_expr).unwrap();

        assert_eq!(
            interval.interval().unwrap(),
            noodles::core::region::Interval::from(
                Position::new(4).unwrap()..=Position::new(4).unwrap()
            )
        );
    }

    #[tokio::test]
    async fn test_evaluate() -> Result<(), Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let binary_expr = eq(col("pos", &batch.schema()).unwrap(), lit(1i64));

        let expr = pos_interval_physical_expr::PosIntervalPhysicalExpr::new(
            1,
            Some(1),
            Arc::new(binary_expr),
        );

        let result = match expr.evaluate(&batch)? {
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

        Ok(())
    }

    #[test]
    fn test_from_interval() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("pos", arrow::datatypes::DataType::Int64, false),
        ]));

        let interval_expr = PosIntervalPhysicalExpr::from_interval(1, Some(10), &schema).unwrap();

        // The interval_expr should be a BinaryExpr with an AND operator
        let inner_expr = interval_expr
            .inner
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();

        match inner_expr.op() {
            Operator::And => {}
            _ => panic!("Expected AND operator"),
        }

        // Now test that without an end, we get a GtEq
        let interval_expr = PosIntervalPhysicalExpr::from_interval(1, None, &schema).unwrap();

        let inner_expr = interval_expr
            .inner
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();

        match inner_expr.op() {
            Operator::GtEq => {}
            _ => panic!("Expected GtEq operator"),
        }
    }
}

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

use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::PhysicalExpr;
use noodles::core::region::Interval;

/// A physical expression that represents start/end interval.
///
/// This is where the start is before the interval end and the end is after the interval start.
/// Query:   |---------|     (10 to 20)
/// Read:         |------|   (15 to 25)
#[derive(Debug)]
pub struct StartEndIntervalPhysicalExpr {
    start: usize,
    end: Option<usize>,
    inner: Arc<dyn PhysicalExpr>,
}

impl StartEndIntervalPhysicalExpr {
    /// Create a new StartEndIntervalPhysicalExpr
    pub fn new(start: usize, end: Option<usize>, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { start, end, inner }
    }

    /// Construct a noodles `Interval` from the start and end.
    pub fn interval(&self) -> Result<Interval> {
        match self.end {
            Some(end) => {
                let interval = format!("{}-{}", self.start, end)
                    .parse::<Interval>()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to parse interval: {}",
                            e
                        ))
                    })?;

                Ok(interval)
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Failed to parse interval: end is None".to_string(),
            )),
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

    /// Get the inner expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    /// Return a tuple of (start, end) for the interval.
    pub fn interval_tuple(&self) -> (usize, Option<usize>) {
        (self.start, self.end)
    }
}

impl Display for StartEndIntervalPhysicalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StartEndIntervalPhysicalExpr")
    }
}

impl TryFrom<BinaryExpr> for StartEndIntervalPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(value: BinaryExpr) -> std::result::Result<Self, Self::Error> {
        let op = value.op();

        let left = value.left().as_any().downcast_ref::<Column>().unwrap();
        let right = value.right().as_any().downcast_ref::<Literal>().unwrap();

        match op {
            Operator::Gt => {
                // If the operator is >, then the left's name needs to be "start" and the right's value needs to be a usize.

                if left.name() != "start" {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Failed to parse interval: left name is not start".to_string(),
                    ));
                }

                let start_value = right.value().to_string().parse::<usize>().unwrap();

                Ok(StartEndIntervalPhysicalExpr::new(
                    start_value,
                    None,
                    Arc::new(value),
                ))
            }
            Operator::Lt => {
                // If the operator is <, then the left's name needs to be "end" and the right's value needs to be a usize.

                if left.name() != "end" {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Failed to parse interval: left name is not end".to_string(),
                    ));
                }

                let end_value = right.value().to_string().parse::<usize>().unwrap();

                Ok(StartEndIntervalPhysicalExpr::new(
                    0,
                    Some(end_value),
                    Arc::new(value),
                ))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Failed to parse interval: operator is not > or <".to_string(),
            )),
        }
    }
}

impl PartialEq<dyn Any> for StartEndIntervalPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<Self>() {
            self.start == other.start && self.end == other.end
        } else {
            false
        }
    }
}

impl PartialEq for StartEndIntervalPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}

impl PhysicalExpr for StartEndIntervalPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
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

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StartEndIntervalPhysicalExpr::new(
            self.start,
            self.end,
            self.inner.clone(),
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

    use datafusion::physical_plan::expressions::{col, lit};

    use crate::tests::{gt, lt};

    #[test]
    fn test_from_start_expr() -> Result<(), Box<dyn std::error::Error>> {
        let start_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("start", arrow::datatypes::DataType::Int64, false),
        ]));

        let start_expr = gt(col("start", &start_schema)?, lit(4));

        let interval = super::StartEndIntervalPhysicalExpr::try_from(start_expr)?;

        assert_eq!(interval.start(), 4);
        assert_eq!(interval.end(), None);

        assert!(interval.interval().is_err());

        // Try with a lt operator, and see if it fails.
        let start_expr = lt(col("start", &start_schema)?, lit(4));
        let result = super::StartEndIntervalPhysicalExpr::try_from(start_expr);

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_from_end_expr() -> Result<(), Box<dyn std::error::Error>> {
        let end_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("end", arrow::datatypes::DataType::Int64, false),
        ]));

        let end_expr = lt(col("end", &end_schema)?, lit(4));

        let interval = super::StartEndIntervalPhysicalExpr::try_from(end_expr)?;

        assert_eq!(interval.start(), 0);
        assert_eq!(interval.end(), Some(4));

        assert!(interval.interval().is_err());

        // Try with a gt operator, and see if it fails.
        let end_expr = gt(col("end", &end_schema)?, lit(4));
        let result = super::StartEndIntervalPhysicalExpr::try_from(end_expr);
        assert!(result.is_err());

        Ok(())
    }
}

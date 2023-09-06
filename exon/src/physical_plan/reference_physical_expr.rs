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
    error::{DataFusionError, Result},
    logical_expr::Operator,
    physical_plan::{
        expressions::{col, lit, BinaryExpr, Column, Literal},
        PhysicalExpr,
    },
};

/// A physical expression that represents a reference.
///
/// Under the hood, this is a binary expression that compares the `reference` column to a literal. But may be used to optimize queries.
#[derive(Debug)]
pub struct ReferencePhysicalExpr {
    reference: String,
    inner: Arc<dyn PhysicalExpr>,
}

impl ReferencePhysicalExpr {
    /// Create a new `ReferencePhysicalExpr` from a reference name and an inner expression.
    pub fn new(reference: String, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { reference, inner }
    }

    /// Get the reference name.
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Return the noodles region with just the reference name.
    pub fn region(&self) -> noodles::core::Region {
        // TODO: how to do this w/o parsing?
        let region = self.reference().parse().unwrap();

        region
    }

    /// Create a new `ReferencePhysicalExpr` from a reference name and a schema.
    pub fn from_reference(reference: &str, schema: &arrow::datatypes::Schema) -> Result<Self> {
        let inner = BinaryExpr::new(col("reference", schema)?, Operator::Eq, lit(reference));

        Ok(Self::new(reference.to_string(), Arc::new(inner)))
    }
}

impl Display for ReferencePhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReferencePhysicalExpr {{ reference: {} }}",
            self.reference
        )
    }
}

impl TryFrom<BinaryExpr> for ReferencePhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: BinaryExpr) -> Result<Self, Self::Error> {
        let op = expr.op();

        if op != &Operator::Eq {
            return Err(DataFusionError::Internal(format!(
                "Invalid operator for reference from expression: {}",
                expr,
            )));
        }

        // Check the case the left expression is a column and the right is a literal
        let left = expr.left().as_any().downcast_ref::<Column>();
        let right = expr.right().as_any().downcast_ref::<Literal>();

        if let (Some(col), Some(lit)) = (left, right) {
            if col.name() != "reference" {
                return Err(DataFusionError::Internal(format!(
                    "Invalid column for reference: {}",
                    col.name()
                )));
            } else {
                return Ok(Self::new(lit.value().to_string(), Arc::new(expr)));
            }
        };

        // Check the right expression is a column and the left is a literal
        let left = expr.left().as_any().downcast_ref::<Literal>();
        let right = expr.right().as_any().downcast_ref::<Column>();

        if let (Some(lit), Some(col)) = (left, right) {
            if col.name() != "reference" {
                return Err(DataFusionError::Internal(format!(
                    "Invalid column for reference: {}",
                    col.name()
                )));
            } else {
                return Ok(Self::new(lit.value().to_string(), Arc::new(expr)));
            }
        };

        Err(DataFusionError::Internal(
            "Invalid expression for reference".to_string(),
        ))
    }
}

impl PartialEq<dyn Any> for ReferencePhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ReferencePhysicalExpr>() {
            self.reference == other.reference
        } else {
            false
        }
    }
}

impl PhysicalExpr for ReferencePhysicalExpr {
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

    fn children(&self) -> Vec<std::sync::Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ReferencePhysicalExpr::new(
            self.reference().to_string(),
            self.inner.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.reference().hash(&mut s);
    }
}

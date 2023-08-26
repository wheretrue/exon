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

#[derive(Debug)]
pub struct ChromPhysicalExpr {
    chrom: String,
    inner: Arc<dyn PhysicalExpr>,
}

impl ChromPhysicalExpr {
    pub fn new(chrom: String, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self { chrom, inner }
    }

    pub fn chrom(&self) -> &str {
        &self.chrom
    }

    pub fn from_chrom(chrom: &str, schema: &arrow::datatypes::Schema) -> Result<Self> {
        let inner = BinaryExpr::new(col("chrom", schema)?, Operator::Eq, lit(chrom));

        Ok(Self::new(chrom.to_string(), Arc::new(inner)))
    }
}

impl Display for ChromPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChromPhysicalExpr {{ chrom: {} }}", self.chrom)
    }
}

impl TryFrom<BinaryExpr> for ChromPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: BinaryExpr) -> Result<Self, Self::Error> {
        let op = expr.op();

        if op != &Operator::Eq {
            return Err(DataFusionError::Internal(format!(
                "Invalid operator for chrom from expression: {}",
                expr,
            )));
        }

        // Check the case the left expression is a column and the right is a literal
        let left = expr.left().as_any().downcast_ref::<Column>();
        let right = expr.right().as_any().downcast_ref::<Literal>();

        if let (Some(col), Some(lit)) = (left, right) {
            if col.name() != "chrom" {
                return Err(DataFusionError::Internal(format!(
                    "Invalid column for chrom: {}",
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
            if col.name() != "chrom" {
                return Err(DataFusionError::Internal(format!(
                    "Invalid column for chrom: {}",
                    col.name()
                )));
            } else {
                return Ok(Self::new(lit.value().to_string(), Arc::new(expr)));
            }
        };

        Err(DataFusionError::Internal(
            "Invalid expression for chrom".to_string(),
        ))
    }
}

impl PartialEq<dyn Any> for ChromPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ChromPhysicalExpr>() {
            self.chrom == other.chrom
        } else {
            false
        }
    }
}

impl PhysicalExpr for ChromPhysicalExpr {
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
        Ok(Arc::new(ChromPhysicalExpr::new(
            self.chrom().to_string(),
            self.inner.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.chrom().hash(&mut s);
    }
}

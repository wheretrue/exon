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
use noodles::core::{region::Interval, Position, Region};

// Extracts the chrom from a binary expression to use for region optimization
fn chrom_operator(expr: &BinaryExpr) -> Option<String> {
    let op = expr.op();

    // Check the case the left expression is a column and the right is a literal
    let left = expr.left().as_any().downcast_ref::<Column>();
    let right = expr.right().as_any().downcast_ref::<Literal>();

    match (left, right, op) {
        (Some(col), Some(lit), Operator::Eq) => {
            if col.name() != "chrom" {
                return None;
            } else {
                return Some(lit.value().to_string());
            }
        }
        (_, _, _) => (),
    }

    // Check the right expression is a column and the left is a literal
    let left = expr.left().as_any().downcast_ref::<Literal>();
    let right = expr.right().as_any().downcast_ref::<Column>();

    match (left, right, op) {
        (Some(literal), Some(col), Operator::Eq) => {
            if col.name() != "chrom" {
                return None;
            } else {
                return Some(literal.value().to_string());
            }
        }
        (_, _, _) => return None,
    }
}

// Extracts the pos from a binary expression to use for region optimization
fn position_operator(expr: &BinaryExpr) -> Option<usize> {
    let op = expr.op();

    // Check the case the left expression is a column and the right is a literal
    let left = expr.left().as_any().downcast_ref::<Column>();
    let right = expr.right().as_any().downcast_ref::<Literal>();

    match (left, right, op) {
        (Some(col), Some(lit), Operator::Eq) => {
            if col.name() != "pos" {
                return None;
            } else {
                return Some(lit.value().to_string().parse::<usize>().unwrap());
            }
        }
        (_, _, _) => (),
    }

    // Check the right expression is a column and the left is a literal
    let left = expr.left().as_any().downcast_ref::<Literal>();
    let right = expr.right().as_any().downcast_ref::<Column>();

    match (left, right, op) {
        (Some(lit), Some(col), Operator::Eq) => {
            if col.name() != "pos" {
                return None;
            } else {
                return Some(lit.value().to_string().parse::<usize>().unwrap());
            }
        }
        (_, _, _) => return None,
    }
}

#[derive(Debug)]
struct InvalidRegionError;

impl Display for InvalidRegionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid expression for region")
    }
}

impl std::error::Error for InvalidRegionError {}

#[derive(Debug)]
pub struct RegionPhysicalExpr {
    region: Region,
}

impl Display for RegionPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write struct fields
        write!(f, "RegionPhysicalExpr {{ region: {} }}", self.region)
    }
}

impl From<Region> for RegionPhysicalExpr {
    fn from(region: Region) -> Self {
        Self { region }
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for RegionPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let chrom_op = chrom_operator(binary_expr);
            let pos_op = position_operator(binary_expr);

            match (chrom_op, pos_op) {
                (Some(chrom), Some(pos)) => {
                    let start = Position::new(pos).unwrap();
                    let end = Position::new(pos).unwrap();
                    let interval = Interval::from(start..=end);

                    let region = Region::new(chrom, interval);

                    Ok(Self::from(region))
                }
                (_, _) => Err(DataFusionError::External(InvalidRegionError.into())),
            }
        } else {
            Err(DataFusionError::External(InvalidRegionError.into()))
        }
    }
}

impl PartialEq<dyn Any> for RegionPhysicalExpr {
    fn eq(&self, _other: &dyn Any) -> bool {
        todo!()
    }
}

impl PhysicalExpr for RegionPhysicalExpr {
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
        // Check if the batch contains the region
        // which is if the 'chrom' column is equal to the region's name
        // and the 'pos' column is within the region's interval

        let chrom = batch
            .column_by_name("chrom")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let pos = batch
            .column_by_name("pos")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        let mut values = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let chrom_value = chrom.value(i);
            let pos_value = pos.value(i);

            let is_in_region = chrom_value == self.region.name()
                && self
                    .region
                    .interval()
                    .contains(Position::new(pos_value as usize).unwrap());

            values.push(is_in_region);
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
        Ok(Arc::new(RegionPhysicalExpr::from(self.region.clone())))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.region.name().to_string().hash(&mut s);

        self.region.interval().start().hash(&mut s);
        self.region.interval().end().hash(&mut s);
    }
}

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

    if let (Some(col), Some(lit), Operator::Eq) = (left, right, op) {
        if col.name() != "chrom" {
            return None;
        } else {
            return Some(lit.value().to_string());
        }
    };

    // Check the right expression is a column and the left is a literal
    let left = expr.left().as_any().downcast_ref::<Literal>();
    let right = expr.right().as_any().downcast_ref::<Column>();

    if let (Some(lit), Some(col), Operator::Eq) = (left, right, op) {
        if col.name() != "chrom" {
            return None;
        } else {
            return Some(lit.value().to_string());
        }
    };

    None
}

// Extracts the pos from a binary expression to use for region optimization

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

/// A physical expression that represents a chromosome.
///
/// Under the hood, this is a binary expression that compares the `chrom` column to a literal. But may be used to optimize queries.
#[derive(Debug, Hash, Eq)]
pub struct RegionNamePhysicalExpr {
    field_name: String,
    field_value: String,
    inner: Arc<dyn PhysicalExpr>,
}

impl RegionNamePhysicalExpr {
    /// Create a new `RegionNamePhysicalExpr` from a chromosome name and an inner expression.
    pub fn new(field_name: String, field_value: String, inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            field_name,
            field_value,
            inner,
        }
    }

    /// Get the field value.
    pub fn field_value(&self) -> &str {
        &self.field_value
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the inner expression.
    pub fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        &self.inner
    }

    /// Return the noodles region with just the chromosome name.
    pub fn region(&self) -> noodles::core::Region {
        // TODO: how to do this w/o parsing?
        let region = self.field_value().parse().unwrap();

        region
    }

    /// Create a new `RegionNamePhysicalExpr` from a field name and field value.
    pub fn from_name_and_value(field_name: String, field_value: String) -> Result<Self> {
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            &field_name,
            arrow::datatypes::DataType::Utf8,
            false,
        )]);

        let inner = BinaryExpr::new(col(&field_name, &schema)?, Operator::Eq, lit(&field_value));

        Ok(Self::new(field_name, field_value, Arc::new(inner)))
    }

    /// Create a new `RegionNamePhysicalExpr` from a field name, field value, and schema.
    pub fn from_name_and_value_with_schema(
        field_name: String,
        field_value: String,
        schema: &arrow::datatypes::Schema,
    ) -> Result<Self> {
        let inner = BinaryExpr::new(col(&field_name, schema)?, Operator::Eq, lit(&field_value));

        Ok(Self::new(field_name, field_value, Arc::new(inner)))
    }

    /// Create a new `RegionNamePhysicalExpr` from a chromosome name and a schema.
    pub fn from_chrom(chrom: &str, schema: &arrow::datatypes::Schema) -> Result<Self> {
        let inner = BinaryExpr::new(col("chrom", schema)?, Operator::Eq, lit(chrom));

        Ok(Self::new(
            "chrom".to_string(),
            chrom.to_string(),
            Arc::new(inner),
        ))
    }

    /// Create a new `RegionNamePhysicalExpr` from a name field and a schema.
    pub fn from_name(name: &str, schema: &arrow::datatypes::Schema) -> Result<Self> {
        let inner = BinaryExpr::new(col("name", schema)?, Operator::Eq, lit(name));

        Ok(Self::new(
            "name".to_string(),
            name.to_string(),
            Arc::new(inner),
        ))
    }
}

impl Display for RegionNamePhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RegionNamePhysicalExpr {{ field_name: {}, field_value: {} }}",
            self.field_name, self.field_value
        )
    }
}

impl TryFrom<BinaryExpr> for RegionNamePhysicalExpr {
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
            match col.name() {
                "chrom" => {
                    return Ok(Self::new(
                        "chrom".to_string(),
                        lit.value().to_string(),
                        Arc::new(expr),
                    ));
                }
                "name" => {
                    return Ok(Self::new(
                        "name".to_string(),
                        lit.value().to_string(),
                        Arc::new(expr),
                    ));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid column for chrom or name: {}",
                        col.name()
                    )));
                }
            }
        };

        // Check the right expression is a column and the left is a literal
        let left = expr.left().as_any().downcast_ref::<Literal>();
        let right = expr.right().as_any().downcast_ref::<Column>();

        if let (Some(lit), Some(col)) = (left, right) {
            match col.name() {
                "chrom" => {
                    return Ok(Self::new(
                        "chrom".to_string(),
                        lit.value().to_string(),
                        Arc::new(expr),
                    ));
                }
                "name" => {
                    return Ok(Self::new(
                        "name".to_string(),
                        lit.value().to_string(),
                        Arc::new(expr),
                    ));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Invalid column for chrom or name: {}",
                        col.name()
                    )));
                }
            }
        };

        Err(DataFusionError::Internal(
            "Invalid expression for chrom".to_string(),
        ))
    }
}

impl PartialEq<dyn Any> for RegionNamePhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<RegionNamePhysicalExpr>() {
            self.field_name == other.field_name && self.field_value == other.field_value
        } else {
            false
        }
    }
}

impl PartialEq for RegionNamePhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.field_name == other.field_name && self.field_value == other.field_value
    }
}

impl PhysicalExpr for RegionNamePhysicalExpr {
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
        Ok(Arc::new(RegionNamePhysicalExpr::new(
            self.field_name.clone(),
            self.field_value.clone(),
            Arc::clone(&self.inner),
        )))
    }

    // fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
    //     let mut s = state;
    //     self.field_name().hash(&mut s);
    //     self.field_value().hash(&mut s);
    // }
}

#[cfg(test)]
mod tests {
    use super::RegionNamePhysicalExpr;
    use arrow::datatypes::Schema;
    use datafusion::{
        common::DFSchema,
        physical_expr::{create_physical_expr, execution_props::ExecutionProps},
        prelude::{col, lit},
    };

    #[test]
    fn test_try_from_chrom_binary_expr() {
        let exprs = vec![col("chrom").eq(lit(5)), lit(5).eq(col("chrom"))];
        let arrow_schema = Schema::new(vec![arrow::datatypes::Field::new(
            "chrom",
            arrow::datatypes::DataType::Utf8,
            false,
        )]);

        let df_schema = DFSchema::try_from(arrow_schema.clone()).unwrap();
        let execution_props = ExecutionProps::default();

        for expr in exprs {
            let phy_expr = create_physical_expr(&expr, &df_schema, &execution_props).unwrap();

            let binary_expr = phy_expr
                .as_any()
                .downcast_ref::<datafusion::physical_plan::expressions::BinaryExpr>()
                .unwrap();

            let chrom_phy_expr = RegionNamePhysicalExpr::try_from(binary_expr.clone()).unwrap();

            assert_eq!(chrom_phy_expr.field_value(), "5");
            assert_eq!(chrom_phy_expr.region(), "5".parse().unwrap());
        }
    }

    #[test]
    fn test_try_from_name_binary_expr() {
        let exprs = vec![col("name").eq(lit("5")), lit("5").eq(col("name"))];
        let arrow_schema = Schema::new(vec![arrow::datatypes::Field::new(
            "name",
            arrow::datatypes::DataType::Utf8,
            false,
        )]);

        let df_schema = DFSchema::try_from(arrow_schema.clone()).unwrap();
        let execution_props = ExecutionProps::default();

        for expr in exprs {
            let phy_expr = create_physical_expr(&expr, &df_schema, &execution_props).unwrap();

            let binary_expr = phy_expr
                .as_any()
                .downcast_ref::<datafusion::physical_plan::expressions::BinaryExpr>()
                .unwrap();

            let region_name_expr = RegionNamePhysicalExpr::try_from(binary_expr.clone()).unwrap();

            assert_eq!(region_name_expr.field_value(), "5");
        }
    }
}

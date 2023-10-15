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

use std::{any::Any, fmt::Display, str::FromStr, sync::Arc};

use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::{expressions::BinaryExpr, PhysicalExpr},
};
use noodles::core::{region::Interval, Region};

use crate::error::{invalid_chrom::InvalidRegionNameError, invalid_region::InvalidRegionError};

use super::{
    region_name_physical_expr::RegionNamePhysicalExpr,
    start_end_interval_physical_expr::StartEndIntervalPhysicalExpr,
};

/// A physical expression that represents a region, e.g. chr1:100-200.
#[derive(Debug)]
pub struct StartEndRegionPhysicalExpr {
    region_name_expr: Arc<dyn PhysicalExpr>,
    interval_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl StartEndRegionPhysicalExpr {
    /// Create a new `StartEndRegionPhysicalExpr` from a region and two inner expressions.
    pub fn new(
        region_name_expr: Arc<dyn PhysicalExpr>,
        interval_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            region_name_expr,
            interval_expr,
        }
    }

    /// Get the region.
    pub fn region(&self) -> Result<Region> {
        let internal_region_name_expr = self.region_name_expr().ok_or(InvalidRegionNameError)?;
        let field_value = internal_region_name_expr.field_value();

        match self.interval_expr() {
            Some(interval_expr) => {
                // let interval = interval_expr.interval()?;

                let start = interval_expr.start();
                let end = interval_expr.end().ok_or(InvalidRegionError)?;
                let interval_str = format!("{}-{}", start, end);

                let interval = Interval::from_str(&interval_str).map_err(|_| InvalidRegionError)?;

                let region = Region::new(field_value, interval);
                Ok(region)
            }
            None => {
                let region = field_value.parse().map_err(|_| InvalidRegionNameError)?;
                Ok(region)
            }
        }
    }

    /// Get the interval expression.
    pub fn interval_expr(&self) -> Option<&StartEndIntervalPhysicalExpr> {
        self.interval_expr
            .as_ref()
            .and_then(|expr| expr.as_any().downcast_ref::<StartEndIntervalPhysicalExpr>())
    }

    /// Get the chromosome expression.
    pub fn region_name_expr(&self) -> Option<&RegionNamePhysicalExpr> {
        self.region_name_expr
            .as_any()
            .downcast_ref::<RegionNamePhysicalExpr>()
    }
}

impl Display for StartEndRegionPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StartEndRegionPhysicalExpr {{ region_name: {}, interval: {:?} }}",
            self.region_name_expr, self.interval_expr,
        )
    }
}

impl From<RegionNamePhysicalExpr> for StartEndRegionPhysicalExpr {
    fn from(value: RegionNamePhysicalExpr) -> Self {
        let chrom_expr = Arc::new(value);

        Self::new(chrom_expr, None)
    }
}

impl TryFrom<BinaryExpr> for StartEndRegionPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: BinaryExpr) -> Result<Self, Self::Error> {
        if let Ok(chrom) = RegionNamePhysicalExpr::try_from(expr.clone()) {
            let new_region = Self::from(chrom);
            return Ok(new_region);
        }

        let chrom_op = expr
            .left()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .map(|e| RegionNamePhysicalExpr::try_from(e.clone()))
            .transpose()?;

        let pos_op = expr
            .right()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .map(|binary_expr| StartEndRegionPhysicalExpr::try_from(binary_expr.clone()))
            .transpose()?;

        match (chrom_op, pos_op) {
            (Some(chrom), Some(pos)) => Ok(Self::new(Arc::new(chrom), Some(Arc::new(pos)))),
            (_, _) => Err(DataFusionError::External(InvalidRegionError.into())),
        }
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for StartEndRegionPhysicalExpr {
    type Error = DataFusionError;

    fn try_from(expr: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            Self::try_from(binary_expr.clone())
        } else {
            Err(DataFusionError::External(InvalidRegionError.into()))
        }
    }
}

impl PartialEq<dyn Any> for StartEndRegionPhysicalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<StartEndRegionPhysicalExpr>() {
            self == other
        } else {
            false
        }
    }
}

impl PartialEq for StartEndRegionPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        let left_interval = match self.interval_expr() {
            Some(interval_expr) => interval_expr,
            None => return false,
        };

        let right_interval = match other.interval_expr() {
            Some(interval_expr) => interval_expr,
            None => return false,
        };

        if left_interval != right_interval {
            return false;
        }

        let left_chrom = match self.region_name_expr() {
            Some(chrom_expr) => chrom_expr,
            None => return false,
        };

        let right_chrom = match other.region_name_expr() {
            Some(chrom_expr) => chrom_expr,
            None => return false,
        };

        left_chrom == right_chrom
    }
}

impl PhysicalExpr for StartEndRegionPhysicalExpr {
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
        let eval = match self.interval_expr {
            Some(ref interval_expr) => {
                let binary_expr = BinaryExpr::new(
                    self.region_name_expr.clone(),
                    datafusion::logical_expr::Operator::And,
                    interval_expr.clone(),
                );

                binary_expr.evaluate(batch)
            }
            None => self.region_name_expr.evaluate(batch),
        };

        tracing::trace!("Got eval: {:?}", eval);

        eval
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StartEndRegionPhysicalExpr::new(
            self.region_name_expr.clone(),
            self.interval_expr.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;

        self.region_name_expr.dyn_hash(&mut s);

        if let Some(ref interval_expr) = self.interval_expr {
            interval_expr.dyn_hash(&mut s);
        }
    }
}

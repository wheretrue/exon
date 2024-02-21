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

use arrow::datatypes::{DataType, Field};

use datafusion::{
    common::cast::{as_float64_array, as_list_array},
    error::Result as DataFusionResult,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct ContainsPeak {
    signature: datafusion::logical_expr::Signature,
}

impl Default for ContainsPeak {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                DataType::Float64,
                DataType::Float64,
            ],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for ContainsPeak {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "contains_peak"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> DataFusionResult<datafusion::physical_plan::ColumnarValue> {
        if args.len() != 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "contains_peak takes three arguments".to_string(),
            ));
        }

        let mz_array = if let Some(ColumnarValue::Array(array)) = args.first() {
            as_list_array(array)
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get mz_array".to_string(),
            ));
        }?;

        let peak_mz = if let Some(ColumnarValue::Scalar(scalar)) = args.get(1) {
            if let ScalarValue::Float64(peak_mz) = scalar {
                peak_mz
            } else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "peak_mz should be a float".to_string(),
                ));
            }
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get peak_mz".to_string(),
            ));
        }
        .ok_or(datafusion::error::DataFusionError::Internal(
            "peak_mz should not be null".to_string(),
        ))?;

        let tolerance = if let Some(ColumnarValue::Scalar(scalar)) = args.get(2) {
            if let ScalarValue::Float64(tolerance) = scalar {
                tolerance
            } else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "tolerance should be a float".to_string(),
                ));
            }
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get tolerance".to_string(),
            ));
        }
        .ok_or(datafusion::error::DataFusionError::Internal(
            "tolerance should not be null".to_string(),
        ))?;

        let mut bool_builder = arrow::array::BooleanBuilder::new();

        for mz_array in mz_array.iter() {
            let mz_array = mz_array.ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "mz_array should not be null".to_string(),
                )
            })?;

            let mz_array = as_float64_array(&mz_array)?;

            let mut found = false;

            for mz in mz_array.iter() {
                let mz = mz.ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "mz should not be null".to_string(),
                    )
                })?;

                if (mz - peak_mz).abs() < tolerance {
                    found = true;
                    break;
                }
            }

            bool_builder.append_value(found);
        }

        Ok(ColumnarValue::Array(Arc::new(bool_builder.finish())))
    }
}

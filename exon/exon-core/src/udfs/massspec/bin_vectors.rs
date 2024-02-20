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

use arrow::{
    array::Float64Builder,
    datatypes::{DataType, Field},
};

use datafusion::{
    common::cast::{as_float64_array, as_list_array},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct BinVectors {
    signature: datafusion::logical_expr::Signature,
}

/// Bin the values in a vector by summing the values in each bin.
impl Default for BinVectors {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                DataType::Float64,
                DataType::Int64,
                DataType::Float64,
            ],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for BinVectors {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "bin_vectors"
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
        // The first two arguments are list types so extract them and conver them to the correct type.
        if args.len() < 4 {
            return Err(datafusion::error::DataFusionError::Execution(
                "bin_vectors takes four arguments".to_string(),
            ));
        }

        let mz_array = if let Some(ColumnarValue::Array(array)) = args.first() {
            as_list_array(array)
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get mz_array".to_string(),
            ));
        }?;

        let intensity_array = if let Some(ColumnarValue::Array(array)) = args.get(1) {
            as_list_array(array)
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get intensity_array".to_string(),
            ));
        }?;

        // The last two arguments are scalar types so extract them and convert them to the correct type.
        let min_mz = if let Some(ColumnarValue::Scalar(scalar)) = args.get(2) {
            if let ScalarValue::Float64(min_mz) = scalar {
                min_mz
            } else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "min_mz should be a float".to_string(),
                ));
            }
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get min_mz".to_string(),
            ));
        }
        .ok_or(datafusion::error::DataFusionError::Internal(
            "min_mz should not be null".to_string(),
        ))?;

        let numb_bins = if let Some(ColumnarValue::Scalar(scalar)) = args.get(3) {
            if let ScalarValue::Int64(numb_bins) = scalar {
                numb_bins
            } else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "numb_bins should be an int".to_string(),
                ));
            }
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get numb_bins".to_string(),
            ));
        }
        .ok_or(datafusion::error::DataFusionError::Internal(
            "numb_bins should not be null".to_string(),
        ))?;

        let bin_width = if let Some(ColumnarValue::Scalar(scalar)) = args.get(4) {
            if let ScalarValue::Float64(bin_width) = scalar {
                bin_width
            } else {
                return Err(datafusion::error::DataFusionError::Execution(
                    "bin_width should be a float".to_string(),
                ));
            }
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "Failed to get bin_width".to_string(),
            ));
        }
        .ok_or(datafusion::error::DataFusionError::Internal(
            "bin_width should not be null".to_string(),
        ))?;

        let value_iter = mz_array.iter().zip(intensity_array.iter());
        let mut bin_builder = arrow::array::ListBuilder::new(Float64Builder::new());

        for (mz_array, intensity_array) in value_iter {
            let mz_array =
                mz_array.ok_or(DataFusionError::Execution("mz_array is None".to_string()))?;

            let mz_array = as_float64_array(&mz_array)?;

            let intensity_array = intensity_array.ok_or(DataFusionError::Execution(
                "intensity_array is None".to_string(),
            ))?;

            let intensity_array = as_float64_array(&intensity_array)?;

            // Iterate through the mz values and bin them by placing the sum of all the mz values in the bin
            // that they belong to.
            let mut bins = vec![0.0; numb_bins as usize];

            let max_mz = min_mz + (numb_bins as f64 * bin_width);

            for (mz, intensity) in mz_array.iter().zip(intensity_array.iter()) {
                let Some(mz) = mz else {
                    continue;
                };

                let Some(intensity) = intensity else {
                    continue;
                };

                if mz < min_mz || mz > max_mz {
                    continue;
                }

                let bin = ((mz - min_mz) / bin_width) as usize;

                if bin < numb_bins as usize {
                    bins[bin] += intensity;
                }
            }

            bin_builder.values().append_slice(&bins);
            bin_builder.append(true);
        }

        let array = bin_builder.finish();

        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

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

pub(crate) mod vcf_region_filter;

use std::sync::Arc;

use arrow::{
    array::{as_string_array, ArrayRef, BooleanArray, BooleanBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::cast::as_int64_array,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionContext,
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};
use noodles::core::{region::Interval, Position, Region};

#[derive(Debug)]
struct VCFRegionMatch {
    signature: datafusion::logical_expr::Signature,
}

impl Default for VCFRegionMatch {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![DataType::Utf8, DataType::Int64, DataType::Utf8],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for VCFRegionMatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "region_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> DataFusionResult<datafusion::physical_plan::ColumnarValue> {
        let chrom_array = if let Some(ColumnarValue::Array(array)) = args.first() {
            as_string_array(array)
        } else {
            return Err(DataFusionError::Execution(
                "Failed to get chrom".to_string(),
            ));
        };

        let position_array = if let Some(ColumnarValue::Array(array)) = args.get(1) {
            as_int64_array(array)?
        } else {
            return Err(DataFusionError::Execution(
                "Failed to get position".to_string(),
            ));
        };

        let region = if let Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) = args.get(2) {
            let r: Region = s.parse().map_err(|e| {
                DataFusionError::Execution(format!("Failed to parse region: {}", e))
            })?;

            r
        } else {
            return Err(DataFusionError::Execution(
                "Failed to get region".to_string(),
            ));
        };

        let region_name = std::str::from_utf8(region.name()).map_err(|e| {
            DataFusionError::Execution(format!("Failed to convert region name: {}", e))
        })?;

        let mut new_bool_array = BooleanBuilder::new();

        let array = chrom_array
            .iter()
            .zip(position_array.iter())
            .map(|(chrom, pos)| {
                let chrom = chrom.ok_or(DataFusionError::Execution(
                    "Failed to get chrom".to_string(),
                ))?;
                let pos = pos.ok_or(DataFusionError::Execution("Failed to get pos".to_string()))?;

                let position = Position::try_from(pos as usize).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to convert pos: {}", e))
                })?;

                Ok::<_, DataFusionError>(
                    region_name == chrom && region.interval().contains(position),
                )
            });

        for ar in array {
            let ar = ar?;
            new_bool_array.append_value(ar);
        }

        let bool_array = new_bool_array.finish();

        let col_val = ColumnarValue::Array(Arc::new(bool_array) as ArrayRef);

        Ok(col_val)
    }
}

#[derive(Debug)]
struct VCFChromMatch {
    signature: datafusion::logical_expr::Signature,
}

impl Default for VCFChromMatch {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for VCFChromMatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "chrom_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> DataFusionResult<datafusion::physical_plan::ColumnarValue> {
        let chrom_array = match args.first() {
            Some(ColumnarValue::Array(array)) => as_string_array(array),
            _ => {
                return Err(DataFusionError::Execution(
                    "Failed to get chrom".to_string(),
                ))
            }
        };

        let value = match args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => s,
            _ => {
                return Err(DataFusionError::Execution(
                    "Failed to get value".to_string(),
                ))
            }
        };

        let array = chrom_array
            .iter()
            .map(|chrom| chrom.map(|chrom| chrom == value))
            .collect::<BooleanArray>();

        let col_value = ColumnarValue::Array(Arc::new(array) as ArrayRef);

        Ok(col_value)
    }
}

#[derive(Debug)]
struct IntervalMatch {
    signature: datafusion::logical_expr::Signature,
}

impl Default for IntervalMatch {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![DataType::Int64, DataType::Utf8],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for IntervalMatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "interval_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> DataFusionResult<ColumnarValue> {
        let position = if let Some(ColumnarValue::Array(array)) = args.first() {
            as_int64_array(array)?
        } else {
            return Err(DataFusionError::Execution(
                "Failed to get position".to_string(),
            ));
        };

        let interval = if let Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))) = args.get(1)
        {
            v
        } else {
            return Err(DataFusionError::Execution(
                "Failed to get interval".to_string(),
            ));
        };

        let interval: Interval = interval
            .parse()
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse interval: {}", e)))?;

        let intersects = position
            .iter()
            .map(|pos| match pos {
                Some(pos) => {
                    let position = Position::try_from(pos as usize).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to convert pos: {}", e))
                    })?;

                    Ok::<_, DataFusionError>(Some(interval.contains(position)))
                }
                _ => Ok(Some(false)),
            })
            .collect::<DataFusionResult<BooleanArray>>()?;

        let intersects = ColumnarValue::Array(Arc::new(intersects) as ArrayRef);

        Ok(intersects)
    }
}

/// Create the interval_match UDF.

/// Register the VCF UDFs.
pub fn register_vcf_udfs(ctx: &SessionContext) {
    let udfs = vec![
        ScalarUDF::from(VCFChromMatch::default()),
        ScalarUDF::from(VCFRegionMatch::default()),
        ScalarUDF::from(IntervalMatch::default()),
    ];

    for udf in udfs {
        ctx.register_udf(udf);
    }
}

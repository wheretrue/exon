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
    array::{Array, ArrayRef},
    datatypes::{DataType, Field},
};

use datafusion::{
    common::cast::{as_float64_array, as_list_array},
    error::Result as DataFusionResult,
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};

/// Check if a spectrum contains a peak within a tolerance.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a ListArray.
///     The ListArray should contain Float64Arrays and be a single column.
///     The second element should be a Float64Array. The Float64Array should
///     contain the peak mz value. The third element should be a Float64Array.
///     The Float64Array should contain the tolerance, this is absolute not
///     PPM.
///
/// # Returns
///
/// * `result` - A BooleanArray. The BooleanArray contains true if the spectrum
///    contains a peak within the tolerance and false otherwise.
fn contains_peak(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 3 {
        return Err(datafusion::error::DataFusionError::Execution(
            "contains_peak takes three arguments".to_string(),
        ));
    }

    let mz_array = as_list_array(&args[0])?;

    // TODO: These should be scalar / constants
    let peak_mz = as_float64_array(&args[1])?;
    let tolerance = as_float64_array(&args[2])?;

    let mut bool_builder = arrow::array::BooleanBuilder::new();

    // for ((())) in mz_array.iter().zip(peak_mz.iter()).zip(tolerance.iter()) {
    for ((mz_array, peak_mz), tolerance) in
        mz_array.iter().zip(peak_mz.iter()).zip(tolerance.iter())
    {
        let mz_array = mz_array.ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("mz_array should not be null".to_string())
        })?;

        let mz_array = match mz_array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
        {
            Some(mz_array) => mz_array,
            None => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "mz_array should be a Float64Array".to_string(),
                ))
            }
        };

        let mut found = false;
        for mz in mz_array.iter() {
            let mz = match mz {
                Some(mz) => mz,
                None => continue,
            };

            let peak_mz = match peak_mz {
                Some(peak_mz) => peak_mz,
                None => continue,
            };

            let tolerance = match tolerance {
                Some(tolerance) => tolerance,
                None => continue,
            };

            if (mz - peak_mz).abs() < tolerance {
                found = true;
                break;
            }
        }

        bool_builder.append_value(found);
    }

    Ok(Arc::new(bool_builder.finish()))
}

/// Create a scalar UDF for contains_peak.
///
/// # Returns
///
/// * `contains_peak` - A ScalarUDF for contains_peak.
pub fn contains_peak_udf() -> ScalarUDF {
    let contains_peak = make_scalar_function(contains_peak);

    create_udf(
        "contains_peak",
        vec![
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            DataType::Float64,
            DataType::Float64,
        ],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        contains_peak,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Float64Builder};

    #[test]
    fn test_contains_peak() {
        let mut mz_builder = arrow::array::ListBuilder::new(Float64Builder::new());

        mz_builder.values().append_slice(&[1.0, 2.0, 3.0]);
        mz_builder.append(true);

        mz_builder.values().append_slice(&[1.0, 3.0]);
        mz_builder.append(true);

        let mz = mz_builder.finish();

        let peak_mz = Float64Array::from(vec![2.0, 2.0]);
        let tolerance = Float64Array::from(vec![0.1, 0.1]);

        let result =
            super::contains_peak(&[Arc::new(mz), Arc::new(peak_mz), Arc::new(tolerance)]).unwrap();

        let expected_values = vec![true, false];

        for (result, expected) in result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap()
            .iter()
            .zip(expected_values)
        {
            assert_eq!(result.unwrap(), expected);
        }
    }
}

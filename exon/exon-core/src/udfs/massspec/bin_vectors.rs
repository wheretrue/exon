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
    array::{ArrayRef, Float64Builder},
    datatypes::Field,
};

use datafusion::{
    common::cast::{as_float64_array, as_int64_array, as_list_array},
    error::{DataFusionError, Result},
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};

/// Bin the values in a vector by summing the values in each bin.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a ListArray.
///     The ListArray should contain Float64Arrays and be a single column.
///     The second element should be a Float64Array. The Float64Array should
///     contain the minimum mz value for each spectrum. The third element should
///     be a Int64Array. The Int64Array should contain the number of bins for
///     each spectrum. The fourth element should be a Float64Array. The
///     Float64Array should contain the bin width for each spectrum.
///
/// # Returns
///
/// * `result` - A ListArray containing Float64Arrays. The Float64Arrays contain
///     the binned values for each spectrum. The number of Float64Arrays is equal
///     to the number of spectra. The number of elements in each Float64Array is
///     equal to the number of bins for the spectrum. The values in each
///     Float64Array are the sum of the array values that fall into the bin.
fn bin_vectors(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 4 {
        return Err(datafusion::error::DataFusionError::Execution(
            "bin_vectors takes four arguments".to_string(),
        ));
    }

    let mz_array = as_list_array(&args[0])?;
    let intensity_array = as_list_array(&args[1])?;

    let min_mz = as_float64_array(&args[2])?;
    let min_mz = min_mz.value(0);

    let numb_bins = as_int64_array(&args[3])?;
    let numb_bins = numb_bins.value(0);

    let bin_width = as_float64_array(&args[4])?;
    let bin_width = bin_width.value(0);

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

    Ok(Arc::new(bin_builder.finish()))
}

/// Create a scalar UDF for bin_vectors.
///
/// # Returns
///
/// * `bin_vectors` - A scalar UDF that bins the values in a vector by summing the values in each bin.
pub fn bin_vectors_udf() -> ScalarUDF {
    let bin_vectors = make_scalar_function(bin_vectors);

    create_udf(
        "bin_vectors",
        vec![
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Float64,
                true,
            ))),
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Float64,
                true,
            ))),
            arrow::datatypes::DataType::Float64,
            arrow::datatypes::DataType::Int64,
            arrow::datatypes::DataType::Float64,
        ],
        Arc::new(arrow::datatypes::DataType::List(Arc::new(Field::new(
            "item",
            arrow::datatypes::DataType::Float64,
            true,
        )))),
        Volatility::Immutable,
        bin_vectors,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Float64Builder, Int64Array};

    use super::bin_vectors;

    #[test]
    fn test_bin_vectors_function() -> Result<(), Box<dyn std::error::Error>> {
        let mut mz_builder = arrow::array::ListBuilder::new(Float64Builder::new());

        mz_builder.values().append_slice(&[1.0, 2.0, 3.0]);
        mz_builder.append(true);

        mz_builder.values().append_slice(&[4.0, 5.0, 6.0]);
        mz_builder.append(true);

        let mz_array = mz_builder.finish();

        let mut intensity_builder = arrow::array::ListBuilder::new(Float64Builder::new());

        intensity_builder.values().append_slice(&[1.0, 2.0, 3.0]);
        intensity_builder.append(true);

        intensity_builder.values().append_slice(&[4.0, 5.0, 6.0]);
        intensity_builder.append(true);

        let intensity_array = intensity_builder.finish();

        let test_min_mz = Float64Array::from(vec![Some(0.0), Some(0.0)]);
        let test_numb_bins = Int64Array::from(vec![Some(2), Some(2)]);
        let test_bin_width = Float64Array::from(vec![Some(2.0), Some(2.0)]);

        let result = bin_vectors(&[
            Arc::new(mz_array) as arrow::array::ArrayRef,
            Arc::new(intensity_array) as arrow::array::ArrayRef,
            Arc::new(test_min_mz) as arrow::array::ArrayRef,
            Arc::new(test_numb_bins) as arrow::array::ArrayRef,
            Arc::new(test_bin_width) as arrow::array::ArrayRef,
        ])?;

        // Downcast ArrayRef to ListArray
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .ok_or("Unexpected array type")?;

        let expected_values = vec![vec![Some(1.0), Some(5.0)], vec![Some(0.0), Some(0.0)]];

        // rewrite the above map to be a for loop
        for (result, expected) in result.iter().zip(expected_values.iter()) {
            let result = result.ok_or("Unexpected null value")?;

            // Downcast ArrayRef to Float64Array
            let result = result
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or("Unexpected array type")?;

            // collect the results into a vector
            let actual = result.iter().collect::<Vec<_>>();

            // assert that the results are what we expect
            assert_eq!(&actual, expected);
        }

        Ok(())
    }
}

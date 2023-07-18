use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float32Builder};

use datafusion::{
    common::cast::as_float32_array,
    common::cast::{as_int32_array, as_list_array},
    error::Result,
};

/// Bin the values in a vector by summing the values in each bin.
///
/// # Arguments
///
/// * `args` - A slice of ArrayRefs. The first element should be a ListArray.
///     The ListArray should contain Float32Arrays and be a single column.
///     The second element should be a Float32Array. The Float32Array should
///     contain the minimum mz value for each spectrum. The third element should
///     be a Int32Array. The Int32Array should contain the number of bins for
///     each spectrum. The fourth element should be a Float32Array. The
///     Float32Array should contain the bin width for each spectrum.
///
/// # Returns
///
/// * `result` - A ListArray containing Float32Arrays. The Float32Arrays contain
///     the binned values for each spectrum. The number of Float32Arrays is equal
///     to the number of spectra. The number of elements in each Float32Array is
///     equal to the number of bins for the spectrum. The values in each
///     Float32Array are the sum of the array values that fall into the bin.
pub fn bin_vectors(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 4 {
        return Err(datafusion::error::DataFusionError::Execution(
            "bin_vectors takes at least four arguments".to_string(),
        ));
    }

    let mz_array = as_list_array(&args[0])?;

    let min_mz = as_float32_array(&args[1])?;
    let numb_bins = as_int32_array(&args[2])?;
    let bin_width = as_float32_array(&args[3])?;

    let value_iter = mz_array
        .iter()
        .zip(min_mz.iter())
        .zip(numb_bins.iter())
        .zip(bin_width.iter());

    let mut bin_builder = arrow::array::ListBuilder::new(Float32Builder::new());

    for (((mz_array, min_mz), numb_bins), bin_width) in value_iter {
        let mz_array = mz_array.unwrap();
        let mz_array = mz_array
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();

        let min_mz = min_mz.unwrap();
        let numb_bins = numb_bins.unwrap();
        let bin_width = bin_width.unwrap();

        // Iterate through the mz values and bin them by placing the sum of all the mz values in the bin
        // that they belong to.
        let mut bins = vec![0.0; numb_bins as usize];

        for mz in mz_array.iter() {
            let mz = mz.unwrap();
            let bin = ((mz - min_mz) / bin_width) as usize;
            if bin < numb_bins as usize {
                bins[bin] += mz;
            }
        }

        bin_builder.values().append_slice(&bins);
        bin_builder.append(true);
    }

    Ok(Arc::new(bin_builder.finish()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, Float32Builder, Int32Array};

    use super::bin_vectors;

    #[test]
    fn test_bin_vectors_function() {
        let mut mz_builder = arrow::array::ListBuilder::new(Float32Builder::new());

        mz_builder.values().append_slice(&[1.0, 2.0, 3.0]);
        mz_builder.append(true);

        mz_builder.values().append_slice(&[4.0, 5.0, 6.0]);
        mz_builder.append(true);

        let mz_array = mz_builder.finish();

        let test_min_mz = Float32Array::from(vec![Some(0.0), Some(0.0)]);
        let test_numb_bins = Int32Array::from(vec![Some(2), Some(2)]);
        let test_bin_width = Float32Array::from(vec![Some(2.0), Some(2.0)]);

        let result = bin_vectors(&[
            Arc::new(mz_array) as arrow::array::ArrayRef,
            Arc::new(test_min_mz) as arrow::array::ArrayRef,
            Arc::new(test_numb_bins) as arrow::array::ArrayRef,
            Arc::new(test_bin_width) as arrow::array::ArrayRef,
        ])
        .unwrap();

        // Downcast ArrayRef to ListArray
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let expected_values = vec![vec![Some(1.0), Some(5.0)], vec![Some(0.0), Some(0.0)]];

        // rewrite the above map to be a for loop
        for (result, expected) in result.iter().zip(expected_values.iter()) {
            let result = result.unwrap();

            // Downcast ArrayRef to Float32Array
            let result = result
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap();

            // collect the results into a vector
            let actual = result.iter().collect::<Vec<_>>();

            // assert that the results are what we expect
            assert_eq!(&actual, expected);
        }
    }
}

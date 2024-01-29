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
    error::{DataFusionError, Result},
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};
use noodles::core::{region::Interval, Position, Region};

/// A UDF that takes a chrom, pos, and region string and returns true if the chrom and pos are in the region.
pub fn region_match(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return Err(datafusion::error::DataFusionError::Execution(
            "region_match takes three arguments, the chrom col, the pos col, and the region string"
                .to_string(),
        ));
    }

    let chrom_array = as_string_array(&args[0]);
    let position_array = as_int64_array(&args[1])?;
    let region_array = as_string_array(&args[2]);

    let mut new_bool_array = BooleanBuilder::new();

    let array = chrom_array
        .iter()
        .zip(position_array.iter())
        .zip(region_array.iter())
        .map(|((chrom, pos), region)| {
            let chrom = chrom.ok_or(DataFusionError::Execution(
                "Failed to get chrom".to_string(),
            ))?;
            let pos = pos.ok_or(DataFusionError::Execution("Failed to get pos".to_string()))?;

            let region: Region = region
                .ok_or(DataFusionError::Execution(
                    "Failed to get region".to_string(),
                ))?
                .parse()
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to parse region: {}", e))
                })?;

            let position = Position::try_from(pos as usize)
                .map_err(|e| DataFusionError::Execution(format!("Failed to convert pos: {}", e)))?;

            let region_name = std::str::from_utf8(region.name()).map_err(|e| {
                DataFusionError::Execution(format!("Failed to convert region name: {}", e))
            })?;

            Ok::<_, DataFusionError>(region_name == chrom && region.interval().contains(position))
        });

    for ar in array {
        let ar = ar?;
        new_bool_array.append_value(ar);
    }

    Ok(Arc::new(new_bool_array.finish()))
}

/// A UDF that takes a chrom and chrom string and returns true if the chrom is in the region.
pub fn chrom_match(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "chrom_match takes two arguments, the chrom col and the region string".to_string(),
        ));
    }

    let chrom_array = as_string_array(&args[0]);
    let value_array = as_string_array(&args[1]);

    let array = chrom_array
        .iter()
        .zip(value_array.iter())
        .map(|(chrom, value)| match (chrom, value) {
            (Some(chrom), Some(value)) => Some(chrom == value),
            _ => None,
        })
        .collect::<BooleanArray>();

    Ok(Arc::new(array))
}

/// A UDF that takes a pos and interval string and returns true if the pos is in the region.
pub fn interval_match(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "interval_match takes two arguments, the pos col and the interval string".to_string(),
        ));
    }

    let position = as_int64_array(&args[0])?;
    let interval = as_string_array(&args[1]);

    let intersects = position
        .iter()
        .zip(interval.iter())
        .map(|(pos, interval)| match (pos, interval) {
            (Some(pos), Some(interval)) => {
                let interval: Interval = interval.parse().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to parse interval: {}", e))
                })?;

                let position = Position::try_from(pos as usize).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to convert pos: {}", e))
                })?;

                Ok::<_, DataFusionError>(Some(interval.contains(position)))
            }
            _ => Ok(Some(false)),
        })
        .collect::<Result<BooleanArray>>()?;

    Ok(Arc::new(intersects))
}

/// Create the interval_match UDF.
pub fn create_interval_udf() -> ScalarUDF {
    create_udf(
        "interval_match",
        vec![DataType::Int64, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(interval_match),
    )
}

/// Create the chrom_match UDF.
pub fn create_chrom_udf() -> ScalarUDF {
    create_udf(
        "chrom_match",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(chrom_match),
    )
}

/// Create the region_match UDF.
pub fn create_region_udf() -> ScalarUDF {
    create_udf(
        "region_match",
        vec![DataType::Utf8, DataType::Int64, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(region_match),
    )
}

/// Register the VCF UDFs.
pub fn register_vcf_udfs() -> Vec<ScalarUDF> {
    vec![
        create_udf(
            "region_match",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(region_match),
        ),
        create_udf(
            "chrom_match",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(chrom_match),
        ),
        create_udf(
            "interval_match",
            vec![DataType::Int64, DataType::Utf8],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(interval_match),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn test_region_match() -> Result<(), Box<dyn std::error::Error>> {
        let chroms = vec!["1", "1", "1", "2", "2"];
        let positions = vec![1, 1, 2, 2, 3];
        let region_strings = vec!["1:1-1", "2:1-2", "1:1-2", "1:1-2", "2:2-3"];

        assert_eq!(chroms.len(), positions.len());
        assert_eq!(positions.len(), region_strings.len());

        let chrom_array = arrow::array::StringArray::from(chroms);
        let position_array = arrow::array::Int64Array::from(positions);
        let region_array = arrow::array::StringArray::from(region_strings);

        let result = super::region_match(&[
            Arc::new(chrom_array),
            Arc::new(position_array),
            Arc::new(region_array),
        ])?;

        // Check the result
        let expected = vec![true, false, true, false, true];
        let expected_array = arrow::array::BooleanArray::from(expected);

        // Downcast the result to a BooleanArray
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or("Failed to downcast result")?;

        assert_eq!(result, &expected_array);

        Ok(())
    }

    #[test]
    fn test_interval_match() -> Result<(), Box<dyn std::error::Error>> {
        let positions = vec![1, 1, 2, 2, 3];
        let region_intervals = vec!["1-1", "1-2", "1-2", "2-3", "1-2"];

        assert_eq!(positions.len(), region_intervals.len());

        let position_array = arrow::array::Int64Array::from(positions);
        let region_array = arrow::array::StringArray::from(region_intervals);

        let result = super::interval_match(&[Arc::new(position_array), Arc::new(region_array)])?;

        // Check the result
        let expected = vec![true, true, true, true, false];
        let expected_array = arrow::array::BooleanArray::from(expected);

        // Downcast the result to a BooleanArray
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or("Failed to downcast result")?;

        assert_eq!(result, &expected_array);

        Ok(())
    }

    #[test]
    fn test_chrom_match() -> Result<(), Box<dyn std::error::Error>> {
        // Test the happy path of chrom_match
        let chrom_val = vec!["1", "1", "1", "1", "2", "2", "2", "2", "2"];
        let region_string = vec!["1", "1", "1", "1", "1", "1", "1", "1", "1"];

        let chrom_array = arrow::array::StringArray::from(chrom_val);
        let region_array = arrow::array::StringArray::from(region_string);

        let result = super::chrom_match(&[Arc::new(chrom_array), Arc::new(region_array)])?;

        // Check the result
        let expected = vec![true, true, true, true, false, false, false, false, false];
        let expected_array = arrow::array::BooleanArray::from(expected);

        // Downcast the result to a BooleanArray
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or("Failed to downcast result")?;

        assert_eq!(result, &expected_array);

        Ok(())
    }

    #[test]
    fn test_chrom_match_bad_arg_count() -> Result<(), Box<dyn std::error::Error>> {
        // Check we throw an error if chrom_match is called with anything but two arguments
        let dummy = vec!["1", "1", "1", "1", "2", "2", "2", "2", "2"];
        let dummy = arrow::array::StringArray::from(dummy);
        let dummy = Arc::new(dummy);

        let result = super::chrom_match(&[dummy.clone(), dummy.clone(), dummy.clone()]);
        assert!(result.is_err());

        let result = super::chrom_match(&[dummy.clone()]);
        assert!(result.is_err());

        let result = super::chrom_match(&[]);
        assert!(result.is_err());

        Ok(())
    }
}

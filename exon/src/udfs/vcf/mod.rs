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
    array::{as_string_array, ArrayRef, BooleanArray, BooleanBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::cast::as_int64_array,
    error::Result,
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};
use noodles::core::{Position, Region};

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
            let chrom = chrom.unwrap();
            let pos = pos.unwrap();

            let region: Region = region.unwrap().parse().unwrap();

            let position = Position::try_from(pos as usize).unwrap();

            region.name() == chrom && region.interval().contains(position)
        });

    for ar in array {
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
        .map(|(chrom, value)| {
            let chrom = chrom.unwrap();
            let value = value.unwrap();

            Some(chrom == value)
        })
        .collect::<BooleanArray>();

    Ok(Arc::new(array))
}

/// A UDF that takes a pos and interval string and returns true if the pos is in the region.
pub fn interval_match(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "interval_match takes two arguments, the pos col and the region string".to_string(),
        ));
    }

    let array = as_int64_array(&args[0]);
    let array = array.iter().map(|_| Some(true)).collect::<BooleanArray>();

    Ok(Arc::new(array))
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

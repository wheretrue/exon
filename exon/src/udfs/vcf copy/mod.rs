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
    array::{as_string_array, ArrayRef, BooleanArray},
    datatypes::DataType,
};
use datafusion::{
    error::Result,
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::create_udf,
};

pub fn region_match(args: &[ArrayRef]) -> Result<ArrayRef> {
    // if args.len() != 2 {
    //     return Err(datafusion::error::DataFusionError::Execution(
    //         "region_match takes three arguments".to_string(),
    //     ));
    // }

    let array = as_string_array(&args[0]);
    let array = array.iter().map(|_| Some(true)).collect::<BooleanArray>();

    Ok(Arc::new(array))
}

pub fn register_vcf_udfs() -> Vec<ScalarUDF> {
    vec![
        create_udf(
            "region_match",
            vec![DataType::Utf8],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(region_match),
        ),
        create_udf(
            "region_match",
            vec![DataType::Utf8, DataType::Utf8],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(region_match),
        ),
        create_udf(
            "region_match",
            vec![DataType::Utf8, DataType::Utf8, DataType::Int32],
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            make_scalar_function(region_match),
        ),
    ]
}

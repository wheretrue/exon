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

use arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::functions::make_scalar_function,
    prelude::SessionContext,
};

/// Return true if the GFF record is in the region. This should not be called directly.
fn gff_region_filter(_args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    Err(DataFusionError::Plan(
        "gff_region_filter should not be called, check your query".to_string(),
    ))
}

/// Create a scalar UDF for GFF region filtering.
pub fn register_gff_region_filter_udf(ctx: &SessionContext) {
    let func = make_scalar_function(gff_region_filter);

    let volatility = Volatility::Immutable;
    let return_type = Arc::new(DataType::Boolean);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));

    let signatures = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
        ],
        volatility,
    );

    let scalar = ScalarUDF::new("gff_region_filter", &signatures, &return_type, &func);

    ctx.register_udf(scalar);
}

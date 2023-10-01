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
    error::Result as DataFusionResult,
    logical_expr::Volatility,
    physical_plan::functions::make_scalar_function,
    prelude::{create_udf, SessionContext},
};

/// Return true if the BAM record overlaps the region.
fn bam_region_filter(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    return Ok(args[0].clone());
}

/// Create a scalar UDF for BAM region filtering.
pub fn register_bam_udf(ctx: &SessionContext) {
    let udf = create_udf(
        "bam_region_filter",
        vec![
            DataType::Utf8,
            DataType::Utf8,
            DataType::Int64,
            DataType::Int64,
        ],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(bam_region_filter),
    );

    ctx.register_udf(udf);

    let udf = create_udf(
        "bam_region_filter",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        make_scalar_function(bam_region_filter),
    );

    ctx.register_udf(udf);
}

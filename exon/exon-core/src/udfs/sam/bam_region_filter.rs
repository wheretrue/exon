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

use arrow::datatypes::DataType;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility},
    prelude::SessionContext,
};

#[derive(Debug)]
pub struct BAMRegionFilterUDF {
    signature: Signature,
}

impl Default for BAMRegionFilterUDF {
    fn default() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int32,
                    DataType::Int32,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                ]),
            ],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for BAMRegionFilterUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "bam_region_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(
        &self,
        _args: &[datafusion::physical_plan::ColumnarValue],
    ) -> DataFusionResult<datafusion::physical_plan::ColumnarValue> {
        Err(DataFusionError::Plan(
            "bam_region_filter should not be called, check your query".to_string(),
        ))
    }
}

/// Create a scalar UDF for BAM region filtering.
pub fn register_bam_region_filter_udf(ctx: &SessionContext) {
    let scalar_impl = BAMRegionFilterUDF::default();
    let scalar = ScalarUDF::from(scalar_impl);

    ctx.register_udf(scalar);
}

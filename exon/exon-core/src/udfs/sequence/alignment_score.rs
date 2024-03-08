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

use arrow::{array::Int32Builder, datatypes::DataType};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

use stringzilla::sz;

#[derive(Debug)]
pub(crate) struct AlignmentScore {
    signature: datafusion::logical_expr::Signature,
}

impl Default for AlignmentScore {
    fn default() -> Self {
        let signature = datafusion::logical_expr::Signature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

impl ScalarUDFImpl for AlignmentScore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "alignment_score"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        if args.len() < 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes two arguments, but got {}",
                self.name(),
                args.len()
            )));
        }

        let first = &args[0];
        let second = &args[1];

        match (first, second) {
            (ColumnarValue::Scalar(first), ColumnarValue::Scalar(second)) => {
                match (first, second) {
                    (ScalarValue::Utf8(Some(first)), ScalarValue::Utf8(Some(second))) => {
                        let score = sz::alignment_score(
                            first.as_bytes(),
                            second.as_bytes(),
                            sz::unary_substitution_costs(),
                            -1,
                        );

                        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                            score as i32,
                        ))))
                    }
                    (_, _) => Err(datafusion::error::DataFusionError::Execution(
                        "alignment_score takes two strings".to_string(),
                    )),
                }
            }
            (ColumnarValue::Array(first), ColumnarValue::Array(second)) => {
                let first = as_string_array(first)?;
                let second = as_string_array(second)?;

                let score = first
                    .iter()
                    .zip(second.iter())
                    .map(|(a, b)| {
                        let a = a.unwrap();
                        let b = b.unwrap();

                        let s = sz::alignment_score(
                            a.as_bytes(),
                            b.as_bytes(),
                            sz::unary_substitution_costs(),
                            -1,
                        );

                        s as i32
                    })
                    .collect::<Vec<i32>>();

                let mut score_builder = Int32Builder::with_capacity(score.len());
                score_builder.append_slice(&score);

                Ok(ColumnarValue::Array(Arc::new(score_builder.finish())))
            }
            (_, _) => Err(datafusion::error::DataFusionError::Execution(
                "alignment_score takes two arrays".to_string(),
            )),
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        if arg_types.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "alignment_score takes two arguments, got {}",
                arg_types.len()
            )));
        }

        Ok(DataType::Int32)
    }
}

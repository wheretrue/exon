// Copyright 2024 WHERE TRUE Technologies.
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

use arrow::{array::StringArray, datatypes::DataType};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ReverseComplement {
    signature: datafusion::logical_expr::Signature,
}

impl Default for ReverseComplement {
    fn default() -> Self {
        let signature =
            datafusion::logical_expr::Signature::exact(vec![DataType::Utf8], Volatility::Immutable);

        Self { signature }
    }
}

/// Returns the reverse complement of a DNA sequence.
pub fn reverse_complement(sequence: &str) -> String {
    sequence
        .chars()
        .rev()
        .map(|base| match base {
            'A' => 'T',
            'a' => 't',
            'T' => 'A',
            't' => 'a',
            'C' => 'G',
            'c' => 'g',
            'G' => 'C',
            'g' => 'c',
            _ => base,
        })
        .collect::<String>()
}

impl ScalarUDFImpl for ReverseComplement {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "reverse_complement"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        match args.first() {
            Some(ColumnarValue::Array(array)) => {
                let array = as_string_array(array)?
                    .iter()
                    .map(|sequence| match sequence {
                        Some(sequence) => {
                            let reverse_complement = reverse_complement(sequence);
                            Some(reverse_complement)
                        }
                        None => None,
                    })
                    .collect::<StringArray>();

                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            Some(ColumnarValue::Scalar(s)) => match s {
                ScalarValue::Utf8(Some(sequence)) => {
                    let reverse_complement = reverse_complement(sequence);

                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        reverse_complement,
                    ))))
                }
                _ => Err(datafusion::error::DataFusionError::Execution(
                    "reverse_complement takes one string argument".to_string(),
                )),
            },
            _ => Err(datafusion::error::DataFusionError::Execution(
                "reverse_complement takes one string array argument".to_string(),
            )),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "reverse_complement takes one argument".to_string(),
            ));
        }

        Ok(DataType::Utf8)
    }
}

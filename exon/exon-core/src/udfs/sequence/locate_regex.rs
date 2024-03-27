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

use core::str;
use std::sync::Arc;

use arrow::{
    array::{Array, GenericStringBuilder, Int32Builder, ListBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields},
};
use datafusion::{
    common::cast::as_string_array,
    error::Result,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Volatility},
    scalar::ScalarValue,
};

#[derive(Debug)]
pub(crate) struct LocateRegex {
    signature: datafusion::logical_expr::Signature,
}

impl Default for LocateRegex {
    fn default() -> Self {
        let sequence = DataType::Utf8;
        let pattern = DataType::Utf8;

        let signature = datafusion::logical_expr::Signature::exact(
            vec![sequence, pattern],
            Volatility::Immutable,
        );

        Self { signature }
    }
}

// a macro that is ok_or a DataFusionError
macro_rules! ok_or {
    ($e:expr, $msg:expr) => {
        $e.ok_or(datafusion::error::DataFusionError::Execution(
            $msg.to_string(),
        ))
    };
}

impl ScalarUDFImpl for LocateRegex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "locate_regex"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        if args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes two arguments",
                self.name()
            )));
        }

        let sequence = &args[0];
        let pattern = &args[1];

        match (sequence, pattern) {
            (
                ColumnarValue::Array(sequence_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern_scalar))),
            ) => {
                let regex_pattern = regex::Regex::new(pattern_scalar).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Error creating regex pattern: {}",
                        e
                    ))
                })?;

                let sequence_arr = as_string_array(sequence_arr)?;

                let struct_builder = StructBuilder::from_fields(
                    vec![
                        Field::new("start", DataType::Int32, true),
                        Field::new("end", DataType::Int32, true),
                        Field::new("match", DataType::Utf8, true),
                    ],
                    1,
                );

                let mut list_builder =
                    ListBuilder::with_capacity(struct_builder, sequence_arr.len());

                for sequence in sequence_arr.iter() {
                    if let Some(sequence) = sequence {
                        let struct_builder = list_builder.values();

                        for m in regex_pattern.find_iter(sequence) {
                            let start_builder = ok_or!(
                                struct_builder.field_builder::<Int32Builder>(0),
                                "Error creating start builder"
                            )?;
                            start_builder.append_value((m.start() as i32) + 1);

                            let end_builder = ok_or!(
                                struct_builder.field_builder::<Int32Builder>(1),
                                "Error creating end builder"
                            )?;
                            end_builder.append_value((m.end() as i32) + 1);

                            let match_builder = ok_or!(
                                struct_builder.field_builder::<GenericStringBuilder<i32>>(2),
                                "Error creating match builder"
                            )?;
                            match_builder.append_value(&sequence[m.start()..m.end()]);

                            struct_builder.append(true);
                        }

                        list_builder.append(true);
                    } else {
                        list_builder.append_null();
                        continue;
                    }
                }

                let list_array = list_builder.finish();

                Ok(ColumnarValue::Array(Arc::new(list_array)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(scalar_arr))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern_scalar))),
            ) => {
                let regex_pattern = regex::Regex::new(pattern_scalar).unwrap();

                let struct_builder = StructBuilder::from_fields(
                    vec![
                        Field::new("start", DataType::Int32, true),
                        Field::new("end", DataType::Int32, true),
                        Field::new("match", DataType::Utf8, true),
                    ],
                    1,
                );
                let mut list_builder = ListBuilder::new(struct_builder);

                let struct_builder = list_builder.values();

                regex_pattern.find_iter(scalar_arr).for_each(|m| {
                    let start_builder = struct_builder.field_builder::<Int32Builder>(0).unwrap();
                    start_builder.append_value((m.start() as i32) + 1);

                    let end_builder = struct_builder.field_builder::<Int32Builder>(1).unwrap();
                    end_builder.append_value((m.end() as i32) + 1);

                    let match_builder = struct_builder
                        .field_builder::<GenericStringBuilder<i32>>(2)
                        .unwrap();
                    match_builder.append_value(&scalar_arr[m.start()..m.end()]);

                    struct_builder.append(true);
                });

                list_builder.append(true);

                let list_array = list_builder.finish();

                Ok(ColumnarValue::Array(Arc::new(list_array)))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes different types of arguments, the a sequence and a pattern",
                self.name()
            ))),
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        if arg_types.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} takes two arguments",
                self.name()
            )));
        }

        // List of structs with start and end fields
        let fields = Fields::from(vec![
            Field::new("start", DataType::Int32, true),
            Field::new("end", DataType::Int32, true),
            Field::new("match", DataType::Utf8, true),
        ]);
        let struct_type = DataType::Struct(fields);

        let dt = DataType::List(Arc::new(Field::new("item", struct_type, true)));

        Ok(dt)
    }
}

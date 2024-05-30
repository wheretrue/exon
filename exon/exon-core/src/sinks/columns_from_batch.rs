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

pub(crate) fn get_array_column<'a, T>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a T, datafusion::error::DataFusionError>
where
    T: arrow::array::Array + 'static,
{
    if let Some(column) = batch.column_by_name(column_name) {
        column.as_any().downcast_ref::<T>().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "{} should be a string array",
                column_name
            ))
        })
    } else {
        Err(datafusion::error::DataFusionError::Execution(format!(
            "{} column not found",
            column_name
        )))
    }
}

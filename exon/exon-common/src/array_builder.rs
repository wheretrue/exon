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

use arrow::{array::ArrayRef, datatypes::Schema, record_batch::RecordBatchOptions};

/// The `ExonArrayBuilder` trait defines the interface for building data arrays.
pub trait ExonArrayBuilder {
    /// Finishes building the internal data structures and returns the built arrays.
    fn finish(&mut self) -> Vec<ArrayRef>;

    /// Creates a record batch from the built arrays.
    fn try_into_record_batch(
        &mut self,
        schema: Arc<Schema>,
    ) -> arrow::error::Result<arrow::record_batch::RecordBatch> {
        let columns = self.finish();
        let options = RecordBatchOptions::default().with_row_count(Some(self.len()));

        let record_batch =
            arrow::record_batch::RecordBatch::try_new_with_options(schema, columns, &options)?;

        Ok(record_batch)
    }

    /// Returns the number of elements in the array.
    fn len(&self) -> usize;

    /// Returns `true` if the array contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

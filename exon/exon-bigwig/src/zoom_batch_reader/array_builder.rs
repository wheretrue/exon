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

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, GenericStringBuilder, Int32Builder};
use bigtools::ZoomRecord;

pub struct ZoomArrayBuilder {
    names: GenericStringBuilder<i32>,
    start: Int32Builder,
    end: Int32Builder,

    total_items: Int32Builder,
    bases_covered: Int32Builder,
    max_value: Float64Builder,
    min_value: Float64Builder,
    sum_squares: Float64Builder,
    sum: Float64Builder,

    n_rows: usize,
}

impl ZoomArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            start: Int32Builder::with_capacity(capacity),
            end: Int32Builder::with_capacity(capacity),
            total_items: Int32Builder::with_capacity(capacity),
            bases_covered: Int32Builder::with_capacity(capacity),
            max_value: Float64Builder::with_capacity(capacity),
            min_value: Float64Builder::with_capacity(capacity),
            sum_squares: Float64Builder::with_capacity(capacity),
            sum: Float64Builder::with_capacity(capacity),
            n_rows: 0,
        }
    }

    pub fn append(&mut self, name: &str, zoom_record: ZoomRecord) {
        self.names.append_value(name);

        self.start.append_value(zoom_record.start as i32);
        self.end.append_value(zoom_record.end as i32);

        let summary = zoom_record.summary;

        self.total_items.append_value(summary.total_items as i32);
        self.bases_covered
            .append_value(summary.bases_covered as i32);
        self.max_value.append_value(summary.max_val);
        self.min_value.append_value(summary.min_val);
        self.sum_squares.append_value(summary.sum_squares);
        self.sum.append_value(summary.sum);

        self.n_rows += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let start = self.start.finish();
        let end = self.end.finish();
        let total_items = self.total_items.finish();
        let bases_covered = self.bases_covered.finish();
        let max_value = self.max_value.finish();
        let min_value = self.min_value.finish();
        let sum_squares = self.sum_squares.finish();
        let sum = self.sum.finish();

        vec![
            Arc::new(names),
            Arc::new(start),
            Arc::new(end),
            Arc::new(total_items),
            Arc::new(bases_covered),
            Arc::new(max_value),
            Arc::new(min_value),
            Arc::new(sum_squares),
            Arc::new(sum),
        ]
    }
}

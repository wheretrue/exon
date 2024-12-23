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

use arrow::{array::RecordBatch, error::ArrowError};
use exon_common::ExonArrayBuilder;
use tokio::io::AsyncBufRead;

use crate::config::SDFConfig;

pub struct BatchReader<R> {
    reader: crate::io::Reader<R>,
    n_records: usize,
    config: Arc<crate::config::SDFConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(inner: R, config: Arc<SDFConfig>) -> Self {
        BatchReader {
            reader: crate::io::Reader::new(inner),
            config,
            n_records: 0,
        }
    }

    pub async fn read_batch(&mut self) -> crate::Result<Option<RecordBatch>> {
        if self.n_records >= self.config.limit.unwrap_or(usize::MAX) {
            return Ok(None);
        }

        let file_schema = self.config.file_schema.clone();
        let mut array_builder = crate::array_builder::SDFArrayBuilder::new(
            file_schema.fields().clone(),
            self.config.clone(),
        )?;

        for _ in 0..self.config.effective_batch_size() {
            match self.reader.read_record().await? {
                Some(record) => array_builder.append_value(record)?,
                None => break,
            }
        }

        self.n_records += array_builder.len();

        if array_builder.is_empty() {
            Ok(None)
        } else {
            // let finished_builder = array_builder.finish();

            let schema = self.config.projected_schema()?;
            let rb = array_builder.try_into_record_batch(schema)?;

            Ok(Some(rb))
        }
    }

    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => {
                    let arrow_error = e.into();
                    Some((Err(arrow_error), reader))
                }
            }
        })
    }
}

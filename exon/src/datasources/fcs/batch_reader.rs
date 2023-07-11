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

use arrow::{error::ArrowError, error::Result as ArrowResult, record_batch::RecordBatch};
use futures::Stream;

use tokio::io::AsyncBufRead;

use super::{array_builder::FCSArrayBuilder, config::FCSConfig, reader::FcsReader};

/// A FCS batch reader.
pub struct BatchReader<R> {
    /// The underlying FCS reader.
    reader: FcsReader<R>,

    /// The FCS configuration.
    config: Arc<FCSConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    /// Creates a FCS batch reader.
    pub fn new(reader: FcsReader<R>, config: Arc<FCSConfig>) -> Self {
        Self { reader, config }
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = FCSArrayBuilder::create(self.config.file_schema.fields().len());

        for _ in 0..self.config.batch_size {
            match self.reader.read_record().await? {
                Some(record) => record_batch.append(record.data),
                None => break,
            }
        }

        if record_batch.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }

    /// Converts the reader into a stream of batches.
    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }
}

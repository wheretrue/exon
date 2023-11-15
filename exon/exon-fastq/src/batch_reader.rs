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

use arrow::record_batch::RecordBatch;
use noodles::fastq;

use tokio::io::AsyncBufRead;

use crate::error::ExonFastqResult;

use super::{array_builder::FASTQArrayBuilder, FASTQConfig};

pub struct BatchReader<R> {
    /// The underlying FASTQ reader.
    reader: noodles::fastq::AsyncReader<R>,
    /// The FASTQ configuration.
    config: Arc<FASTQConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(inner: R, config: Arc<FASTQConfig>) -> Self {
        Self {
            reader: noodles::fastq::AsyncReader::new(inner),
            config,
        }
    }

    /// Stream built `RecordBatch`es from the underlying FASTQ reader.
    pub fn into_stream(self) -> impl futures::Stream<Item = ExonFastqResult<RecordBatch>> {
        futures::stream::try_unfold(self, |mut reader| async move {
            match reader.read_batch(reader.config.batch_size).await? {
                Some(batch) => Ok(Some((batch, reader))),
                None => Ok(None),
            }
        })
    }

    async fn read_record(&mut self) -> ExonFastqResult<Option<noodles::fastq::Record>> {
        let mut record = fastq::Record::default();

        match self.reader.read_record(&mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self, batch_size: usize) -> ExonFastqResult<Option<RecordBatch>> {
        let mut array = FASTQArrayBuilder::create();

        for _ in 0..batch_size {
            match self.read_record().await? {
                Some(record) => array.append(&record)?,
                None => break,
            }
        }

        if array.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), array.finish())?;

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

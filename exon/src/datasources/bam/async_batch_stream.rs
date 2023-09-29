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

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::Stream;
use tokio::io::AsyncBufRead;

use super::{array_builder::BAMArrayBuilder, BAMConfig};

pub struct AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    /// The underlying reader.
    reader: noodles::bam::AsyncReader<R>,

    /// The BAM configuration.
    config: Arc<BAMConfig>,

    /// The reference sequences.
    reference_sequences: Arc<noodles::sam::header::ReferenceSequences>,
}

impl<R> AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(
        reader: noodles::bam::AsyncReader<R>,
        config: Arc<BAMConfig>,
        reference_sequences: Arc<noodles::sam::header::ReferenceSequences>,
    ) -> Self {
        Self {
            reader,
            config,
            reference_sequences,
        }
    }

    /// Stream the record batches from the VCF file.
    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_record_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::bam::lazy::Record>> {
        let mut record = noodles::bam::lazy::Record::default();
        match self.reader.read_lazy_record(&mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_record_batch(&mut self) -> ArrowResult<Option<arrow::record_batch::RecordBatch>> {
        let mut builder =
            BAMArrayBuilder::create(self.reference_sequences.clone(), self.config.projection());

        for i in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => builder.append(&record).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid record: {e}"),
                    )
                })?,
                None => {
                    if i == 0 {
                        return Ok(None);
                    } else {
                        break;
                    }
                }
            }
        }

        let schema = self.config.projected_schema()?;
        let batch = RecordBatch::try_new(schema, builder.finish())?;

        tracing::debug!("Read batch with {} rows", batch.num_rows());

        Ok(Some(batch))
    }
}

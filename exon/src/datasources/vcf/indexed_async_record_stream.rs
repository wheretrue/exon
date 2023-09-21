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

use super::{array_builder::LazyVCFArrayBuilder, config::VCFConfig};

/// A VCF record batch reader.
pub struct AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
    // R: AsyncRead + Unpin,
{
    /// The underlying record stream.
    // reader: noodles::vcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,
    reader: noodles::vcf::AsyncReader<R>,

    /// The VCF configuration.
    config: Arc<VCFConfig>,

    /// The VCF header.
    header: Arc<noodles::vcf::Header>,
}

impl<R> AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
    // R: AsyncRead + Unpin,
{
    /// Create a new VCF record batch reader.
    pub fn new(
        reader: noodles::vcf::AsyncReader<R>,
        config: Arc<VCFConfig>,
        header: Arc<noodles::vcf::Header>,
    ) -> Self {
        Self {
            reader,
            config,
            header,
        }
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::lazy::Record>> {
        let mut record = noodles::vcf::lazy::Record::default();

        match self.reader.read_lazy_record(&mut record).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(record)),
            Err(e) => Err(e),
        }
    }

    /// Stream the record batches from the VCF file.
    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = LazyVCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.clone(),
            self.header.clone(),
        )?;

        for i in 0..self.config.batch_size {
            let record = self.read_record().await?;

            match record {
                Some(record) => {
                    // tracing::debug!("Read record {:?}", record);

                    record_batch.append(&record)?;
                }
                None => {
                    tracing::debug!("Reached end of batch on record {}", i);
                    break;
                }
            }
        }

        if record_batch.is_empty() {
            tracing::debug!("Empty Batch");
            return Ok(None);
        }

        let schema = self.config.projected_schema();
        let batch = RecordBatch::try_new(schema, record_batch.finish())?;

        tracing::debug!("Finished batch with {} records", batch.num_rows());

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

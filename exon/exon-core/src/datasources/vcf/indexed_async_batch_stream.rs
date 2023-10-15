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

use std::{str::FromStr, sync::Arc};

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::Stream;
use noodles::core::{Position, Region};
use tokio::io::AsyncBufRead;

use super::{array_builder::LazyVCFArrayBuilder, config::VCFConfig};

/// A VCF record batch reader.
pub struct IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    /// The underlying record stream.
    reader: noodles::vcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// The VCF configuration.
    config: Arc<VCFConfig>,

    /// The VCF header.
    header: Arc<noodles::vcf::Header>,

    /// The region to use for filtering.
    region: Arc<Region>,

    /// The max uncompressed bytes from the BGZF reader.
    max_bytes: usize,
}

impl<R> IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    /// Create a new VCF record batch reader.
    pub fn new(
        reader: noodles::vcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,
        config: Arc<VCFConfig>,
        header: Arc<noodles::vcf::Header>,
        region: Arc<Region>,
    ) -> Self {
        Self {
            reader,
            config,
            header,
            region,
            max_bytes: usize::MAX,
        }
    }

    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::lazy::Record>> {
        if self.reader.virtual_position().uncompressed() as usize >= self.max_bytes {
            return Ok(None);
        }

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

    fn filter(&self, record: &noodles::vcf::lazy::Record) -> Result<bool, ArrowError> {
        let chrom = record.chromosome();

        if chrom != self.region.name() {
            return Ok(false);
        }

        let position = Position::from_str(record.position())
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        let contains_position = self.region.interval().contains(position);

        Ok(contains_position)
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = LazyVCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.clone(),
            self.header.clone(),
        )?;

        let mut record_count = 0;
        while record_count < self.config.batch_size {
            let record = self.read_record().await?;

            match record {
                Some(record) => {
                    if self.filter(&record)? {
                        record_batch.append(&record)?;
                        record_count += 1;
                    }
                }
                None => {
                    tracing::debug!("Reached end of batch on record {}", record_count);
                    break;
                }
            }
        }

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

        Ok(Some(batch))
    }
}

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

use super::{array_builder::VCFArrayBuilder, config::VCFConfig};

/// A VCF record batch reader.
pub struct AsyncBatchReader<R> {
    /// The underlying VCF reader.
    reader: noodles::vcf::AsyncReader<R>,

    /// The VCF header.
    header: noodles::vcf::Header,

    /// The VCF configuration.
    config: Arc<VCFConfig>,
}

impl<R> AsyncBatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub async fn new(inner: R, config: Arc<VCFConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::vcf::AsyncReader::new(inner);

        let header = reader.read_header().await?;

        Ok(Self {
            reader,
            header,
            config,
        })
    }

    pub async fn new_with_header(
        inner: R,
        config: Arc<VCFConfig>,
        header: noodles::vcf::Header,
    ) -> std::io::Result<Self> {
        let reader = noodles::vcf::AsyncReader::new(inner);

        Ok(Self {
            reader,
            header,
            config,
        })
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::Record>> {
        let mut record = noodles::vcf::Record::default();

        match self.reader.read_record(&self.header, &mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = VCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            None,
        )?;

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(&record),
                None => break,
            }
        }

        if record_batch.is_empty() {
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
}

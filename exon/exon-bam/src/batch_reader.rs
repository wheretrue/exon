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

use arrow::{error::ArrowError, record_batch::RecordBatch};

use exon_common::ExonArrayBuilder;
use futures::Stream;
use noodles::sam::{alignment::RecordBuf, Header};
use tokio::io::{AsyncBufRead, AsyncRead};

use crate::{indexed_async_batch_stream::SemiLazyRecord, BAMArrayBuilder, BAMConfig};

/// A batch reader for BAM files.
pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying BAM reader.
    reader: noodles::bam::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// The BAM configuration.
    config: Arc<BAMConfig>,

    header: Arc<Header>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send + AsyncRead,
{
    pub async fn new(inner: R, config: Arc<BAMConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::bam::AsyncReader::new(inner);

        let header = reader.read_header().await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid BAM header: {}", e),
            )
        })?;

        Ok(Self {
            reader,
            config,
            header: Arc::new(header),
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

    async fn read_record(&mut self) -> std::io::Result<Option<RecordBuf>> {
        let mut record_buf = RecordBuf::default();

        match self
            .reader
            .read_record_buf(&self.header, &mut record_buf)
            .await?
        {
            0 => Ok(None),
            _ => Ok(Some(record_buf)),
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut builder = BAMArrayBuilder::create(self.header.clone(), self.config.projection());

        for i in 0..self.config.batch_size {
            if let Some(record) = self.read_record().await? {
                let semi_lazy_record = SemiLazyRecord::try_from(record)?;

                builder.append(&semi_lazy_record)?;
            } else if i == 0 {
                return Ok(None);
            } else {
                break;
            }
        }

        let schema = self.config.projected_schema()?;
        let batch = builder.try_into_record_batch(schema)?;

        Ok(Some(batch))
    }
}

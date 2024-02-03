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
use noodles::sam::alignment::RecordBuf;
use tokio::io::{AsyncBufRead, AsyncRead};

use super::{array_builder::SAMArrayBuilder, config::SAMConfig};

/// A batch reader for SAM files.
pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying SAM reader.
    reader: noodles::sam::AsyncReader<R>,

    /// The configuration for this reader.
    config: Arc<SAMConfig>,

    /// The header of the SAM file.
    header: noodles::sam::Header,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send + AsyncRead,
{
    pub async fn new(inner: R, config: Arc<SAMConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::sam::AsyncReader::new(inner);

        let header = reader.read_header().await?;

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

    async fn read_record(&mut self) -> std::io::Result<Option<RecordBuf>> {
        let mut record = RecordBuf::default();

        match self
            .reader
            .read_record_buf(&self.header, &mut record)
            .await?
        {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut array_builder = SAMArrayBuilder::create(self.header.clone(), self.config.clone());

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => array_builder.append(&record).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid record: {e}"),
                    )
                })?,
                None => break,
            }
        }

        if array_builder.is_empty() {
            return Ok(None);
        }

        let schema = self.config.projected_schema()?;
        let batch = array_builder.try_into_record_batch(schema)?;

        Ok(Some(batch))
    }
}

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

use std::{pin::Pin, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};

use futures::{Stream, StreamExt};
use noodles::sam::alignment::Record;
use tokio::io::{AsyncBufRead, AsyncRead};

use crate::datasources::sam::SAMArrayBuilder;

use super::config::BAMConfig;

/// A batch reader for BAM files.
pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying BAM reader.
    reader: noodles::bam::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// The BAM configuration.
    config: Arc<BAMConfig>,

    /// The BAM header.
    header: noodles::sam::Header,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send + AsyncRead,
{
    pub async fn new(inner: R, config: Arc<BAMConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::bam::AsyncReader::new(inner);

        let header = reader.read_header().await?.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid header: {e}"),
            )
        })?;

        reader.read_reference_sequences().await?;

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

    async fn read_record(&mut self) -> std::io::Result<Option<Record>> {
        let mut record = Record::default();

        match self.reader.read_record(&self.header, &mut record).await? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = SAMArrayBuilder::create(self.header.clone());

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(&record)?,
                None => break,
            }
        }

        if record_batch.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }
}

/// Given an input stream of records, adapt it to a stream of `RecordBatch`s.
pub struct StreamRecordBatchAdapter {
    stream: Pin<Box<dyn Stream<Item = std::io::Result<Record>> + Send>>,
    header: noodles::sam::Header,
    config: Arc<BAMConfig>,
}

impl StreamRecordBatchAdapter {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = std::io::Result<Record>> + Send>>,
        header: noodles::sam::Header,
        config: Arc<BAMConfig>,
    ) -> Self {
        Self {
            stream,
            header,
            config,
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = SAMArrayBuilder::create(self.header.clone());

        for _ in 0..self.config.batch_size {
            match self.stream.next().await {
                Some(Ok(record)) => record_batch.append(&record)?,
                Some(Err(e)) => return Err(ArrowError::ExternalError(Box::new(e))),
                None => break,
            }
        }

        if record_batch.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
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
}

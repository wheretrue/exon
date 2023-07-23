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

use arrow::{error::ArrowError, record_batch::RecordBatch};

use futures::Stream;
use noodles::bed::Record;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use super::{array_builder::BEDArrayBuilder, bed_record_builder::BEDRecord, config::BEDConfig};

/// A batch reader for BED files.
pub struct BatchReader<R> {
    /// The underlying BED reader.
    reader: R,

    /// The BED configuration.
    config: Arc<BEDConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(inner: R, config: Arc<BEDConfig>) -> Self {
        Self {
            reader: inner,
            config,
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

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut array_builder = BEDArrayBuilder::create();

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => array_builder.append(record).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid record: {e}"),
                    )
                })?,
                None => break,
            }
        }

        if array_builder.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), array_builder.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }

    pub async fn read_record(&mut self) -> std::io::Result<Option<BEDRecord>> {
        let mut buf = String::new();
        if self.reader.read_line(&mut buf).await? == 0 {
            return Ok(None);
        }

        // Get the number of tab separated fields
        let num_fields = buf.split('\t').count();

        // Remove the newline
        buf.pop();

        // Remove the carriage return if present and on windows
        #[cfg(target_os = "windows")]
        if buf.ends_with(CARRIAGE_RETURN) {
            buf.pop();
        }

        let bed_record = match num_fields {
            12 => {
                let r: Record<12> = match Record::from_str(&buf) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid record: {e}"),
                        ));
                    }
                };

                r.into()
            }
            9 => {
                let r: Record<9> = match Record::from_str(&buf) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid record: {e}"),
                        ));
                    }
                };
                r.into()
            }
            6 => {
                let r: Record<6> = match Record::from_str(&buf) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid record: {e}"),
                        ));
                    }
                };
                r.into()
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid number of fields: {num_fields}"),
                ));
            }
        };

        buf.clear();

        Ok(Some(bed_record))
    }
}

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
use exon_common::ExonArrayBuilder;
use futures::Stream;

use tokio::io::AsyncBufRead;

use crate::{error::ExonFastaResult, ExonFastaError};

use super::{array_builder::FASTAArrayBuilder, config::FASTAConfig};

/// A FASTA batch reader.
pub struct BatchReader<R> {
    /// The underlying FASTA reader.
    reader: noodles::fasta::AsyncReader<R>,

    /// The FASTA configuration.
    config: Arc<FASTAConfig>,

    /// Internal buffer for the sequence.
    sequence_buffer: Vec<u8>,

    /// Internal buffer for the definition.
    buf: String,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    /// Creates a FASTA batch reader.
    pub fn new(inner: R, config: Arc<FASTAConfig>) -> Self {
        let buffer_size = config.fasta_sequence_buffer_capacity;

        Self {
            reader: noodles::fasta::AsyncReader::new(inner),
            config,
            buf: String::with_capacity(50),
            sequence_buffer: Vec::with_capacity(buffer_size),
        }
    }

    async fn read_record(&mut self) -> ExonFastaResult<Option<()>> {
        self.buf.clear();
        if self.reader.read_definition(&mut self.buf).await? == 0 {
            return Ok(None);
        }

        self.sequence_buffer.clear();
        if self.reader.read_sequence(&mut self.sequence_buffer).await? == 0 {
            return Err(ExonFastaError::ParseError("invalid sequence".to_string()));
        }

        Ok(Some(()))
    }

    async fn read_batch(&mut self) -> ExonFastaResult<Option<RecordBatch>> {
        let mut array_builder = FASTAArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.projection.clone(),
            self.config.batch_size,
            &self.config.sequence_data_type,
        )?;

        for _ in 0..self.config.batch_size {
            self.buf.clear();
            self.sequence_buffer.clear();

            if (self.read_record().await?).is_some() {
                array_builder.append(&self.buf, &self.sequence_buffer)?;
            } else {
                break;
            }
        }

        if array_builder.is_empty() {
            return Ok(None);
        }

        let schema = self.config.projected_schema()?;
        let batch = array_builder.try_into_record_batch(schema)?;

        Ok(Some(batch))
    }

    /// Converts the reader into a stream of batches.
    pub fn into_stream(self) -> impl Stream<Item = ExonFastaResult<RecordBatch>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        })
    }
}

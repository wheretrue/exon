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

use arrow::record_batch::RecordBatch;
use futures::Stream;
use noodles::fasta::{
    record::{Definition, Sequence},
    Record,
};

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
            sequence_buffer: Vec::with_capacity(buffer_size),
        }
    }

    async fn read_record(&mut self) -> ExonFastaResult<Option<noodles::fasta::Record>> {
        let mut buf = String::with_capacity(50); // 50 is somewhat arbitrary

        if self.reader.read_definition(&mut buf).await? == 0 {
            return Ok(None);
        }

        let definition = Definition::from_str(&buf)?;

        // Allow for options?
        // let mut sequence = Vec::with_capacity(self.config.fasta_sequence_buffer_capacity);
        self.sequence_buffer.clear();
        if self.reader.read_sequence(&mut self.sequence_buffer).await? == 0 {
            return Err(ExonFastaError::ParseError("invalid sequence".to_string()));
        }
        let sequence = Sequence::from_iter(self.sequence_buffer.iter().copied());
        let record = Record::new(definition, sequence);

        Ok(Some(record))
    }

    async fn read_batch(&mut self) -> ExonFastaResult<Option<RecordBatch>> {
        let mut record_batch =
            FASTAArrayBuilder::create(self.config.file_schema.clone(), self.config.batch_size)?;

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(&record)?,
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

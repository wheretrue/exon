// Copyright 2024 WHERE TRUE Technologies.
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
    array::{ArrayRef, GenericStringBuilder},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use exon_common::{ExonArrayBuilder, DEFAULT_BATCH_SIZE};
use futures::Stream;
use noodles::{
    cram::{AsyncReader, Record},
    sam::alignment::record::Name,
};
use tokio::io::AsyncBufRead;

use crate::CRAMConfig;

pub struct AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    /// The underlying stream of CRAM records.
    reader: AsyncReader<R>,

    /// The header.
    header: noodles::sam::Header,

    /// The CRAM config.
    config: Arc<CRAMConfig>,

    /// The reference repository
    reference_sequence_repository: noodles::fasta::Repository,
}

impl<R> AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(
        reader: AsyncReader<R>,
        header: noodles::sam::Header,
        config: Arc<CRAMConfig>,
    ) -> Self {
        Self {
            reader,
            header,
            config,
            reference_sequence_repository: noodles::fasta::Repository::default(),
        }
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut array_builder = CRAMArrayBuilder::new(DEFAULT_BATCH_SIZE);

        if let Some(container) = self.reader.read_data_container().await? {
            let records = container
                .slices()
                .iter()
                .map(|slice| {
                    let compression_header = container.compression_header();

                    slice.records(compression_header).and_then(|mut records| {
                        slice.resolve_records(
                            &self.reference_sequence_repository,
                            &self.header,
                            compression_header,
                            &mut records,
                        )?;

                        Ok(records)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten();

            // iterate through the records and append them to the array builder
            for record in records {
                array_builder.append(record)?;
            }
        } else {
            return Ok(None);
        }

        let schema = self.config.projected_schema();
        let batch = array_builder.try_into_record_batch(schema)?;

        Ok(Some(batch))
    }

    pub fn into_stream(self) -> impl Stream<Item = ArrowResult<RecordBatch>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }
}

struct CRAMArrayBuilder {
    rows: usize,
    names: GenericStringBuilder<i32>,
}

impl CRAMArrayBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: 0,
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity * 8),
        }
    }

    pub fn append(&mut self, record: Record) -> Result<(), ArrowError> {
        if let Some(name) = record.name() {
            let sam_read_name = name.as_bytes();
            self.names.append_value(std::str::from_utf8(sam_read_name)?);
        } else {
            self.names.append_null();
        }

        self.rows += 1;

        Ok(())
    }
}

impl ExonArrayBuilder for CRAMArrayBuilder {
    fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();

        vec![Arc::new(names)]
    }

    fn len(&self) -> usize {
        self.rows
    }
}

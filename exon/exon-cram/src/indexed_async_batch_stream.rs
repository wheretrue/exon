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
    array::RecordBatch,
    error::{ArrowError, Result as ArrowResult},
};
use coitrees::{BasicCOITree, Interval, IntervalTree};
use exon_common::{ExonArrayBuilder, DEFAULT_BATCH_SIZE};
use futures::Stream;
use noodles::cram::{
    crai::{self, Record},
    AsyncReader,
};
use tokio::io::{AsyncBufRead, AsyncSeek};

use crate::{array_builder::CRAMArrayBuilder, CRAMConfig, ObjectStoreFastaRepositoryAdapter};

pub struct IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + AsyncSeek + Unpin,
{
    /// The underlying stream of CRAM records.
    reader: AsyncReader<R>,

    /// The header.
    header: noodles::sam::Header,

    /// The CRAM config.
    config: Arc<CRAMConfig>,

    /// The reference repository.
    reference_sequence_repository: noodles::fasta::Repository,

    /// The CRAM index record.
    // index_records: Vec<Record>,
    ranges: BasicCOITree<crai::Record, u32>,
}

impl<R> IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + AsyncSeek + Unpin,
{
    pub async fn try_new(
        reader: AsyncReader<R>,
        header: noodles::sam::Header,
        config: Arc<CRAMConfig>,
        index_records: Vec<Record>,
    ) -> ArrowResult<Self> {
        let reference_sequence_repository = match &config.fasta_reference {
            Some(reference) => {
                let object_store_repo = ObjectStoreFastaRepositoryAdapter::try_new(
                    config.object_store.clone(),
                    reference.to_string(),
                )
                .await?;

                noodles::fasta::Repository::new(object_store_repo)
            }
            None => noodles::fasta::Repository::default(),
        };

        let ranges = index_records
            .iter()
            .map(|r| {
                let start = r.alignment_start().unwrap().get();
                let end = start + r.alignment_span();

                Interval::new(start as i32, end as i32, r.clone())
            })
            .collect::<Vec<_>>();

        let trees = BasicCOITree::new(&ranges);

        Ok(Self {
            reader,
            header,
            config,
            reference_sequence_repository,
            ranges: trees,
        })
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut array_builder =
            CRAMArrayBuilder::new(self.header.clone(), DEFAULT_BATCH_SIZE, &self.config);

        let container = if let Some(container) = self.reader.read_data_container().await? {
            container
        } else {
            return Ok(None);
        };

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
            .flatten()
            .filter(|record| {
                let start = record.alignment_start().unwrap().get();
                let end = start + record.alignment_end().unwrap().get();

                self.ranges.query_count(start as i32, end as i32) > 0
            });

        for record in records {
            array_builder.append(record)?;
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

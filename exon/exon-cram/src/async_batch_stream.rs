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
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use exon_common::{ExonArrayBuilder, DEFAULT_BATCH_SIZE};
use futures::Stream;
use noodles::cram::AsyncReader;
use object_store::{path::Path, ObjectStore};
use tokio::io::AsyncBufRead;

use crate::{array_builder::CRAMArrayBuilder, CRAMConfig, ObjectStoreFastaRepositoryAdapter};

pub struct AsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
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
    pub async fn try_new(
        reader: AsyncReader<R>,
        object_store: Arc<dyn ObjectStore>,
        header: noodles::sam::Header,
        config: Arc<CRAMConfig>,
    ) -> ArrowResult<Self> {
        let reference_sequence_repository = match &config.fasta_reference {
            Some(reference) => {
                let object_store_adapter = ObjectStoreFastaRepositoryAdapter::try_new(
                    object_store.clone(),
                    reference.clone(),
                )
                .await?;

                noodles::fasta::Repository::new(object_store_adapter)
            }
            None => noodles::fasta::Repository::default(),
        };

        Ok(Self {
            reader,
            header,
            config,
            reference_sequence_repository,
        })
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut array_builder =
            CRAMArrayBuilder::new(self.header.clone(), DEFAULT_BATCH_SIZE, &self.config);

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

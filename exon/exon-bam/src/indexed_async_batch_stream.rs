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

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use exon_common::ExonArrayBuilder;
use futures::Stream;
use noodles::{
    core::{region::Interval, Position, Region},
    sam::{alignment::RecordBuf, header::ReferenceSequences, Header},
};
use tokio::io::AsyncBufRead;

use super::{array_builder::BAMArrayBuilder, BAMConfig};

/// This is a semi-lazy record that can be used to filter on the region without
/// having to decode the entire record or re-decode the cigar.
pub(crate) struct SemiLazyRecord {
    inner: RecordBuf,
    alignment_end: Option<Position>,
}

impl TryFrom<RecordBuf> for SemiLazyRecord {
    type Error = arrow::error::ArrowError;

    fn try_from(record: RecordBuf) -> Result<Self, Self::Error> {
        let start = record.alignment_start();

        let alignment_end = start.map(|s| usize::from(s) + record.cigar().alignment_span() - 1);
        let alignment_end = alignment_end
            .map(Position::try_from)
            .transpose()
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        Ok(Self {
            inner: record,
            alignment_end,
        })
    }
}

impl SemiLazyRecord {
    pub fn alignment_start(&self) -> Option<Position> {
        self.inner.alignment_start()
    }

    pub fn alignment_end(&self) -> Option<Position> {
        self.alignment_end
    }

    pub fn record(&self) -> &RecordBuf {
        &self.inner
    }

    pub fn intersects(
        &self,
        region_sequence_id: usize,
        region_interval: &Interval,
    ) -> std::io::Result<bool> {
        let reference_sequence_id = self.inner.reference_sequence_id();

        let alignment_start = self.alignment_start();
        let alignment_end = self.alignment_end();

        match (reference_sequence_id, alignment_start, alignment_end) {
            (Some(id), Some(start), Some(end)) => {
                let alignment_interval = (start..=end).into();
                let intersects = region_interval.intersects(alignment_interval);

                let same_sequence = id == region_sequence_id;

                Ok(intersects && same_sequence)
            }
            _ => Ok(false),
        }
    }
}

pub struct IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    /// The underlying reader.
    reader: noodles::bam::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// The BAM configuration.
    config: Arc<BAMConfig>,

    /// The reference sequences.
    header: Arc<Header>,

    /// The region reference sequence.
    region_reference: usize,

    /// The region interval.
    region_interval: Interval,

    /// The max uncompressed bytes read.
    max_bytes: Option<u16>,
}

fn get_reference_sequence_for_region(
    reference_sequences: &ReferenceSequences,
    region: &Region,
) -> std::io::Result<usize> {
    reference_sequences
        .get_index_of(region.name())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid reference sequence", // format!("invalid reference sequence: {}", region.name()),
            )
        })
}

impl<R> IndexedAsyncBatchStream<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn try_new(
        reader: noodles::bam::AsyncReader<noodles::bgzf::AsyncReader<R>>,
        config: Arc<BAMConfig>,
        header: Arc<Header>,
        region: Arc<Region>,
    ) -> std::io::Result<Self> {
        let region_reference =
            get_reference_sequence_for_region(header.reference_sequences(), &region)?;
        let region_interval = region.interval();

        Ok(Self {
            reader,
            config,
            header,
            region_reference,
            region_interval,
            max_bytes: None,
        })
    }

    pub fn set_max_bytes(&mut self, max_bytes: u16) {
        self.max_bytes = Some(max_bytes);
    }

    /// Stream the record batches from the VCF file.
    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_record_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    async fn read_record(&mut self, record: &mut RecordBuf) -> std::io::Result<Option<()>> {
        if let Some(max_bytes) = self.max_bytes {
            if self.reader.get_ref().virtual_position().uncompressed() >= max_bytes {
                return Ok(None);
            }
        }

        let bytes_read = self.reader.read_record_buf(&self.header, record).await?;

        if bytes_read == 0 {
            Ok(None)
        } else {
            Ok(Some(()))
        }
    }

    async fn read_record_batch(&mut self) -> ArrowResult<Option<arrow::record_batch::RecordBatch>> {
        let mut builder = BAMArrayBuilder::create(self.header.clone(), self.config.clone());
        let mut record = RecordBuf::default();

        for i in 0..self.config.batch_size {
            if self.read_record(&mut record).await?.is_some() {
                let semi_lazy_record = SemiLazyRecord::try_from(record.clone())?;

                if semi_lazy_record.intersects(self.region_reference, &self.region_interval)? {
                    builder.append(&semi_lazy_record)?;
                }
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

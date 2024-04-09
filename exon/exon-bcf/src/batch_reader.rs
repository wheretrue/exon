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

use exon_vcf::VCFArrayBuilder;
use noodles::{bcf::Record, vcf::variant::RecordBuf};
use tokio::io::{AsyncBufRead, AsyncRead};

use crate::config::BCFConfig;

pub struct BatchReader<R>
where
    R: AsyncRead,
{
    /// The underlying BCF reader.
    reader: noodles::bcf::AsyncReader<noodles::bgzf::AsyncReader<R>>,

    /// Configuration for how to batch records.
    config: Arc<BCFConfig>,

    /// The VCF header.
    header: Arc<noodles::vcf::Header>,
    // The VCF header string maps.
    // string_maps: noodles::bcf::header::StringMaps,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub async fn new(inner: R, config: Arc<BCFConfig>) -> std::io::Result<Self> {
        let mut reader = noodles::bcf::AsyncReader::new(inner);
        // reader.read_file_format().await?;

        let header = reader.read_header().await?;
        // let header = header_str.parse::<noodles::vcf::Header>().map_err(|e| {
        //     std::io::Error::new(
        //         std::io::ErrorKind::InvalidData,
        //         format!("invalid header: {e}"),
        //     )
        // })?;

        // let string_maps = match header_str.parse() {
        //     Ok(string_maps) => string_maps,
        //     Err(e) => {
        //         return Err(std::io::Error::new(
        //             std::io::ErrorKind::InvalidData,
        //             format!("invalid header: {e}"),
        //         ))
        //     }
        // };

        Ok(Self {
            reader,
            config,
            header: Arc::new(header),
            // string_maps,
        })
    }

    async fn read_record(&mut self) -> std::io::Result<Option<RecordBuf>> {
        let mut lazy_record = Record::default();

        match self.reader.read_record(&mut lazy_record).await? {
            0 => Ok(None),
            _ => {
                let vcf_record = RecordBuf::try_from_variant_record(&self.header, &lazy_record)?;

                Ok(Some(vcf_record))
            }
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = VCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            None,
            self.header.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => record_batch.append(record)?,
                None => break,
            }
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }

    /// Returns a stream of batches from the underlying BCF reader.
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        })
    }
}

pub struct BatchAdapter {
    /// The underlying record iterator.
    record_iterator: Box<dyn Iterator<Item = Result<noodles::bcf::Record, std::io::Error>> + Send>,

    /// Configuration for how to batch records.
    config: Arc<BCFConfig>,

    /// The header.
    header: Arc<noodles::vcf::Header>,
}

impl BatchAdapter {
    pub fn new(
        record_iterator: Box<
            dyn Iterator<Item = Result<noodles::bcf::Record, std::io::Error>> + Send,
        >,
        config: Arc<BCFConfig>,
        header: Arc<noodles::vcf::Header>,
    ) -> Self {
        Self {
            record_iterator,
            config,
            header,
        }
    }

    fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut record_batch = VCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.as_deref(),
            self.header.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            match self.record_iterator.next() {
                Some(Ok(record)) => record_batch.append(record)?,
                Some(Err(e)) => return Err(ArrowError::ExternalError(Box::new(e))),
                None => break,
            }
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }
}

impl Iterator for BatchAdapter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_batch().transpose()
    }
}

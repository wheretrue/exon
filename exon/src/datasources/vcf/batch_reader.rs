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

use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};

use super::{array_builder::VCFArrayBuilder, config::VCFConfig};

trait VCFRecordIterator: Iterator<Item = std::io::Result<noodles::vcf::Record>> + Send {
    fn header(&self) -> &noodles::vcf::Header;
}

pub struct UnIndexedRecordIterator<R> {
    reader: noodles::vcf::Reader<R>,
    header: noodles::vcf::Header,
}

impl<R> UnIndexedRecordIterator<R>
where
    R: std::io::BufRead,
{
    pub fn try_new(reader: R) -> std::io::Result<Self> {
        let mut reader = noodles::vcf::Reader::new(reader);
        let header = reader.read_header()?;
        Ok(Self { reader, header })
    }

    fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::Record>> {
        let mut record = noodles::vcf::Record::default();

        match self.reader.read_record(&self.header, &mut record)? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }
}

impl<R> Iterator for UnIndexedRecordIterator<R>
where
    R: std::io::BufRead,
{
    type Item = std::io::Result<noodles::vcf::Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_record().transpose()
    }
}

/// A VCF record batch reader.
pub struct BatchReader {
    /// The underlying VCF record iterator.
    record_iterator: Box<dyn Iterator<Item = std::io::Result<noodles::vcf::Record>> + Send>,

    /// The VCF configuration.
    config: Arc<VCFConfig>,
}

impl BatchReader {
    pub fn new(
        record_iterator: Box<dyn Iterator<Item = std::io::Result<noodles::vcf::Record>> + Send>,
        config: Arc<VCFConfig>,
    ) -> Self {
        Self {
            record_iterator,
            config,
        }
    }

    fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = VCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            let record = self.record_iterator.next().transpose()?;

            match record {
                Some(record) => record_batch.append(&record),
                None => break,
            }
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let schema = self.config.projected_schema();
        let batch = RecordBatch::try_new(schema, record_batch.finish())?;

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

impl Iterator for BatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_batch().transpose()
    }
}

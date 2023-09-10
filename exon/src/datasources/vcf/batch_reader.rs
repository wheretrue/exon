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

use super::{array_builder::LazyVCFArrayBuilder, config::VCFConfig};

trait VCFRecordIterator: Iterator<Item = std::io::Result<noodles::vcf::Record>> + Send {
    fn header(&self) -> &noodles::vcf::Header;
}

pub struct UnIndexedRecordIterator<R> {
    reader: noodles::vcf::Reader<R>,
}

impl<R> UnIndexedRecordIterator<R>
where
    R: std::io::BufRead,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: noodles::vcf::Reader::new(reader),
        }
    }

    fn read_record(&mut self) -> std::io::Result<Option<noodles::vcf::lazy::Record>> {
        let mut record = noodles::vcf::lazy::Record::default();

        let record = match self.reader.read_lazy_record(&mut record) {
            Ok(0) => None,
            Ok(_) => Some(record),
            Err(e) => return Err(e),
        };

        Ok(record)
    }
}

impl<R> Iterator for UnIndexedRecordIterator<R>
where
    R: std::io::BufRead,
{
    type Item = std::io::Result<noodles::vcf::lazy::Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_record().transpose()
    }
}

/// A VCF record batch reader.
pub struct BatchReader {
    /// The underlying VCF record iterator.
    record_iterator: Box<dyn Iterator<Item = std::io::Result<noodles::vcf::lazy::Record>> + Send>,

    /// The VCF configuration.
    config: Arc<VCFConfig>,

    /// The VCF header.
    header: Arc<noodles::vcf::Header>,
}

impl BatchReader {
    pub fn new(
        record_iterator: Box<
            dyn Iterator<Item = std::io::Result<noodles::vcf::lazy::Record>> + Send,
        >,
        config: Arc<VCFConfig>,
        header: Arc<noodles::vcf::Header>,
    ) -> Self {
        Self {
            record_iterator,
            config,
            header,
        }
    }

    fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = LazyVCFArrayBuilder::create(
            self.config.file_schema.clone(),
            self.config.batch_size,
            self.config.projection.clone(),
            self.header.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            let record = self.record_iterator.next().transpose()?;
            match record {
                Some(record) => record_batch.append(&record)?,
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

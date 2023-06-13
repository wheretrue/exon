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

use std::{
    io::{Read, Seek},
    sync::Arc,
};

use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};

use futures::Stream;
use noodles::{
    core::Region,
    vcf::{reader::Query, Header},
};

use super::{array_builder::VCFArrayBuilder, config::VCFConfig};

trait VCFRecordIterator: Iterator<Item = std::io::Result<noodles::vcf::Record>> + Send {
    fn header(&self) -> &noodles::vcf::Header;
}

/// A stream of records from an indexed reader.
/// pub struct VCFIndexedRecordStream<R> {
pub struct VCFIndexedRecordStream<R>
where
    R: Read + Seek + Unpin + Send + 'static,
{
    reader: noodles::vcf::Reader<noodles::bgzf::Reader<R>>,
    index: noodles::csi::Index,
    header: noodles::vcf::Header,
    region: Region,
}

impl<R> VCFIndexedRecordStream<R>
where
    R: Read + Seek + Unpin + Send,
{
    pub fn new(inner: R, index: noodles::csi::Index, region: Region) -> std::io::Result<Self> {
        let mut reader = noodles::vcf::Reader::new(noodles::bgzf::Reader::new(inner));

        let header = reader.read_header()?;

        let query = reader.query(&header, &index, &region).unwrap();

        let boxed_query = Box::new(query.into_iter());

        Ok(Self {
            reader,
            index,
            header,
            region,
        })
    }
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

impl<R> VCFRecordIterator for UnIndexedRecordIterator<R>
where
    R: std::io::BufRead + Send,
{
    fn header(&self) -> &noodles::vcf::Header {
        &self.header
    }
}

/// A VCF record batch reader.
pub struct BatchReader {
    /// The underlying VCF record iterator.
    // record_iterator: Box<dyn VCFRecordIterator<Item = std::io::Result<noodles::vcf::Record>>>,
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
        let mut record_batch =
            VCFArrayBuilder::create(self.config.file_schema.clone(), self.config.batch_size)?;

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

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), record_batch.finish())?;

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

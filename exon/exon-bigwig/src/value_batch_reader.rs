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

use bigtools::{utils::reopen::ReopenableFile, BigWigRead, ChromInfo, Value};

use arrow::{
    array::RecordBatch,
    error::{ArrowError, Result as ArrowResult},
};
use noodles::core::Region;

mod array_builder;
mod config;

use self::{array_builder::ValueArrayBuilder, config::ValueReadType};

pub use self::config::BigWigValueConfig;
pub use self::config::SchemaBuilder;

pub struct ValueRecordBatchReader {
    config: Arc<BigWigValueConfig>,

    scanner: ValueScanner,
}

impl ValueRecordBatchReader {
    pub fn try_new(file_path: &str, config: Arc<BigWigValueConfig>) -> ArrowResult<Self> {
        let reader = BigWigRead::open_file(file_path).map_err(|e| {
            ArrowError::IoError(
                "failed to open bigwig file".to_string(),
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;

        let interval = match config.read_type {
            ValueReadType::Interval(ref interval) => Some(interval.clone()),
            _ => None,
        };

        let scanner = ValueScanner::try_new(reader, interval)?;

        Ok(Self { config, scanner })
    }

    pub fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = ValueArrayBuilder::with_capacity(self.config.batch_size);

        for val in self.scanner.by_ref().take(self.config.batch_size) {
            let (chrom, record) = val?;
            record_batch.append(&chrom, record);
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let arrays = record_batch.finish();

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), arrays)?;

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }

    /// Return a stream of record_batches.
    pub fn into_stream(self) -> impl futures::Stream<Item = ArrowResult<RecordBatch>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch() {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        })
    }
}

struct ValueScanner {
    reader: BigWigRead<ReopenableFile>,
    chroms: Vec<ChromInfo>,
    chrom_position: usize,
    current_records: Vec<Value>,
    within_batch_position: usize,
}

impl ValueScanner {
    pub fn try_new(
        mut reader: BigWigRead<ReopenableFile>,
        interval: Option<Region>,
    ) -> ArrowResult<Self> {
        if let Some(region) = interval {
            let name = std::str::from_utf8(region.name())?;

            let chroms = reader
                .chroms()
                .iter()
                .filter(|c| c.name == name)
                .cloned()
                .collect::<Vec<_>>();

            let chrom = chroms.first().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("chromosome {} not found", name))
            })?;

            let start = region.interval().start().map_or(0, |s| s.get() as u32);
            let end = region
                .interval()
                .end()
                .map_or(chrom.length, |e| e.get() as u32);

            let inter = reader.get_interval(name, start, end).map_err(|e| {
                ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                )
            })?;

            let records = inter.collect::<Result<Vec<Value>, _>>().map_err(|e| {
                ArrowError::IoError(
                    "failed to read bigwig records".to_string(),
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                )
            })?;

            Ok(Self {
                reader,
                chroms,
                current_records: records,
                chrom_position: 0,
                within_batch_position: 0,
            })
        } else {
            let chroms = reader.chroms().to_vec();

            if chroms.is_empty() {
                return Err(ArrowError::InvalidArgumentError(
                    "no chromosomes found in bigwig file".to_string(),
                ));
            }

            let c = &chroms[0];
            let chrom_name = &c.name;
            let start = 0;
            let end = c.length;

            let inter = reader.get_interval(chrom_name, start, end).map_err(|e| {
                ArrowError::IoError(
                    e.to_string(),
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                )
            })?;

            let records = inter.collect::<Result<Vec<Value>, _>>().map_err(|e| {
                ArrowError::IoError(
                    "failed to read bigwig records".to_string(),
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                )
            })?;

            Ok(Self {
                reader,
                current_records: records,
                chroms,
                chrom_position: 0,
                within_batch_position: 0,
            })
        }
    }

    pub fn chrom_name(&self) -> &str {
        self.chroms[self.chrom_position].name.as_str()
    }
}

impl Iterator for ValueScanner {
    type Item = ArrowResult<(String, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.within_batch_position >= self.current_records.len() {
            self.chrom_position += 1;
            if self.chrom_position >= self.chroms.len() {
                return None;
            }

            let c = &self.chroms[self.chrom_position];
            let i = self
                .reader
                .get_interval(&c.name, 0, c.length)
                .map_err(|e| {
                    ArrowError::IoError(
                        e.to_string(),
                        std::io::Error::new(std::io::ErrorKind::Other, e),
                    )
                })
                .ok()?
                .collect::<Result<Vec<Value>, _>>()
                .unwrap();

            self.current_records = i;
            self.within_batch_position = 0;
        }

        let record = self.current_records[self.within_batch_position];
        self.within_batch_position += 1;

        Some(Ok((self.chrom_name().to_string(), record)))
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use noodles::core::Region;
    use object_store::local::LocalFileSystem;

    use crate::value_batch_reader::{config::BigWigValueConfig, ValueRecordBatchReader};

    // Test reading from a bigwig file
    #[tokio::test]
    async fn test_read_bigwig() -> Result<(), Box<dyn std::error::Error>> {
        let cargo_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let file_path = format!(
            "{}/../../exon/exon-core/test-data/datasources/bigwig/test.bw",
            cargo_path
        );

        let object_store = Arc::new(LocalFileSystem::default());

        let region = Region::from_str("1:1-1000")?;
        let config = BigWigValueConfig::new(object_store).with_some_interval(Some(region));

        let mut reader = ValueRecordBatchReader::try_new(&file_path, Arc::new(config))?;

        let batch = reader.read_batch()?.ok_or("no batch")?;

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 4);

        Ok(())
    }
}

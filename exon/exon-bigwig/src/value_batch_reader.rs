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

use std::{str::FromStr, sync::Arc};

use bigtools::{utils::reopen::ReopenableFile, BigWigRead, ChromInfo, Value};

use arrow::{
    array::RecordBatch,
    error::{ArrowError, Result as ArrowResult},
};
use noodles::core::{Position, Region};

use crate::config::{BigWigConfig, ValueReadType};

use self::array_builder::BigWigArrayBuilder;

mod array_builder;

pub struct RecordBatchReader {
    config: Arc<BigWigConfig>,

    scanner: BigWigReadScanner,
}

impl RecordBatchReader {
    pub fn try_new(file_path: &str, config: Arc<BigWigConfig>) -> ArrowResult<Self> {
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

        let scanner = BigWigReadScanner::try_new(reader, interval)?;

        Ok(Self { config, scanner })
    }

    pub fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut record_batch = BigWigArrayBuilder::with_capacity(self.config.batch_size);

        for (chrom, record) in self.scanner.by_ref().take(self.config.batch_size) {
            record_batch.append(&chrom, record);
        }

        if record_batch.is_empty() {
            return Ok(None);
        }

        let arrays = record_batch.finish();

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), arrays)?;

        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(&projection)?;

                Ok(Some(projected_batch))
            }
            None => Ok(Some(batch)),
        }
    }
}

struct BigWigReadScanner {
    reader: BigWigRead<ReopenableFile>,
    chroms: Vec<ChromInfo>,
    chrom_position: usize,
    current_records: Vec<Value>,
    within_batch_position: usize,
}

impl BigWigReadScanner {
    pub fn try_new(
        mut reader: BigWigRead<ReopenableFile>,
        interval: Option<String>,
    ) -> ArrowResult<Self> {
        if let Some(interval) = interval {
            let region = Region::from_str(interval.as_str()).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("invalid interval: {}", e))
            })?;

            let name = std::str::from_utf8(region.name().as_ref()).unwrap();

            let chroms = reader
                .chroms()
                .iter()
                .filter(|c| c.name == name)
                .map(|c| c.clone())
                .collect::<Vec<ChromInfo>>();

            let chrom = chroms.first().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("chromosome {} not found", name))
            })?;

            let start = region.interval().start().map_or(0, |s| s.get() as u32);
            let end = region
                .interval()
                .end()
                .map_or(chrom.length, |e| e.get() as u32);

            let inter = reader.get_interval(&name, start, end).unwrap();

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

            if chroms.len() <= 0 {
                return Err(ArrowError::InvalidArgumentError(
                    "no chromosomes found in bigwig file".to_string(),
                ));
            }

            let c = &chroms[0];
            let chrom_name = &c.name;
            let start = 0;
            let end = c.length;

            let inter = reader.get_interval(chrom_name, start, end).unwrap();
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

impl Iterator for BigWigReadScanner {
    type Item = (String, Value);

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
                .unwrap()
                .collect::<Result<Vec<Value>, _>>()
                .unwrap();

            self.current_records = i;
            self.within_batch_position = 0;
        }

        let record = self.current_records[self.within_batch_position].clone();
        self.within_batch_position += 1;

        Some((self.chrom_name().to_string(), record))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;

    use crate::value_batch_reader::RecordBatchReader;

    // Test reading from a bigwig file
    #[tokio::test]
    async fn test_read_bigwig() -> Result<(), Box<dyn std::error::Error>> {
        let cargo_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let file_path = format!(
            "{}/../../exon/exon-core/src/datasources/bigwig/test.bw",
            cargo_path
        );

        let object_store = Arc::new(LocalFileSystem::default());
        let config = crate::config::BigWigConfig::new(object_store).with_interval("1".to_string());

        let mut reader = RecordBatchReader::try_new(&file_path, Arc::new(config))?;

        let batch = reader.read_batch()?.ok_or("no batch")?;

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 4);

        Ok(())
    }
}

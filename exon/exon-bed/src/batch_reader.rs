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

use arrow::{error::ArrowError, record_batch::RecordBatch};

use exon_common::ExonArrayBuilder;
use futures::Stream;
use noodles::{
    bed::feature::{record::Strand, record_buf::OtherFields, RecordBuf},
    core::Position,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use super::{array_builder::BEDArrayBuilder, bed_record_builder::BEDRecord, config::BEDConfig};

/// A batch reader for BED files.
pub struct BatchReader<R> {
    /// The underlying BED reader.
    reader: R,

    /// The BED configuration.
    config: Arc<BEDConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(inner: R, config: Arc<BEDConfig>) -> Self {
        Self {
            reader: inner,
            config,
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut array_builder = BEDArrayBuilder::create(
            self.config.file_schema.clone(),
            Some(self.config.projection()),
        );

        for _ in 0..self.config.batch_size {
            match self.read_record().await? {
                Some(record) => array_builder.append(record).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid record: {e}"),
                    )
                })?,
                None => break,
            }
        }

        if array_builder.is_empty() {
            return Ok(None);
        }

        let schema = self.config.projected_schema().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid schema: {e}"),
            )
        })?;
        let batch = array_builder.try_into_record_batch(schema)?;

        Ok(Some(batch))
    }

    pub async fn read_record(&mut self) -> std::io::Result<Option<BEDRecord>> {
        let mut buf = String::new();
        if self.reader.read_line(&mut buf).await? == 0 {
            return Ok(None);
        }

        // Skip commented lines
        while buf.starts_with('#') {
            buf.clear();
            if self.reader.read_line(&mut buf).await? == 0 {
                return Ok(None);
            }
        }

        // Remove the newline
        buf.pop();

        // Remove the carriage return if present and on windows
        #[cfg(target_os = "windows")]
        if buf.ends_with('\r') {
            buf.pop();
        }

        // Get the number of tab separated fields
        let split = buf.split('\t').collect::<Vec<&str>>();
        let num_fields = split.len();

        let bed_record = match num_fields {
            12 => {
                let buf_builder = RecordBuf::<6>::builder();

                let other_fields = OtherFields::default();

                let mut record = buf_builder
                    .set_reference_sequence_name(split[0].as_bytes().to_vec())
                    .set_feature_start(Position::from_str(split[1]).unwrap())
                    .set_feature_end(Position::from_str(split[2]).unwrap())
                    .set_name(split[3].as_bytes().to_vec())
                    .set_score(split[4].parse().unwrap());

                match split[5] {
                    "+" => {
                        record = record.set_strand(Strand::Forward);
                    }
                    "-" => {
                        record = record.set_strand(Strand::Reverse);
                    }
                    "." => {}
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid strand: {}", split[5]),
                        ))
                    }
                };

                let record = record.set_other_fields(other_fields).build();

                BEDRecord::from(record)
            }
            // 9 => extract_record!(buf, 9),
            // 8 => extract_record!(buf, 8),
            // 7 => extract_record!(buf, 7),
            6 => {
                // let record = read_record_6(buf.as_bytes())?;
                let mut buf_builder = RecordBuf::<6>::builder()
                    .set_reference_sequence_name(split[0].as_bytes().to_vec())
                    .set_feature_start(Position::from_str(split[1]).unwrap())
                    .set_feature_end(Position::from_str(split[2]).unwrap())
                    .set_name(split[3].as_bytes().to_vec())
                    .set_score(split[4].parse().unwrap());

                match split[5] {
                    "+" => {
                        buf_builder = buf_builder.set_strand(Strand::Forward);
                    }
                    "-" => {
                        buf_builder = buf_builder.set_strand(Strand::Reverse);
                    }
                    "." => {}
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid strand: {}", split[5]),
                        ))
                    }
                };

                let record = buf_builder.build();

                BEDRecord::from(record)
            }
            5 => {
                let buf_builder = RecordBuf::<5>::builder();

                let record = buf_builder
                    .set_reference_sequence_name(split[0].as_bytes().to_vec())
                    .set_feature_start(Position::from_str(split[1]).unwrap())
                    .set_feature_end(Position::from_str(split[2]).unwrap())
                    .set_name(split[3].as_bytes().to_vec())
                    .set_score(split[4].parse().unwrap())
                    .build();

                BEDRecord::from(record)
            }
            4 => {
                let buf_builder = RecordBuf::<4>::builder();

                let record = buf_builder
                    .set_reference_sequence_name(split[0].as_bytes().to_vec())
                    .set_feature_start(Position::from_str(split[1]).unwrap())
                    .set_feature_end(Position::from_str(split[2]).unwrap())
                    .set_name(split[3].as_bytes().to_vec())
                    .build();

                BEDRecord::from(record)
            }
            3 => {
                let buf_builder = RecordBuf::<3>::builder();

                let record = buf_builder
                    .set_reference_sequence_name(split[0].as_bytes().to_vec())
                    .set_feature_start(Position::from_str(split[1]).unwrap())
                    .set_feature_end(Position::from_str(split[2]).unwrap())
                    .build();

                BEDRecord::from(record)
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid number of fields: {num_fields}"),
                ));
            }
        };

        buf.clear();

        Ok(Some(bed_record))
    }
}

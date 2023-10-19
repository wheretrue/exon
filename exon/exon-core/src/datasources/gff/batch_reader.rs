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

use std::{str::FromStr, sync::Arc};

use arrow::{error::ArrowError, error::Result as ArrowResult, record_batch::RecordBatch};

use futures::Stream;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use super::{array_builder::GFFArrayBuilder, GFFConfig};

/// Reads a GFF file into arrow record batches.
pub struct BatchReader<R> {
    /// The reader to read from.
    reader: R,

    /// The configuration for this reader.
    config: Arc<GFFConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(reader: R, config: Arc<GFFConfig>) -> Self {
        Self { reader, config }
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

    async fn read_line(&mut self) -> std::io::Result<Option<noodles::gff::Line>> {
        loop {
            let mut buf = String::new();
            match self.reader.read_line(&mut buf).await {
                Ok(0) => return Ok(None),
                Ok(_) => {
                    buf.pop();

                    #[cfg(target_os = "windows")]
                    if buf.ends_with('\r') {
                        buf.pop();
                    }

                    let line = match noodles::gff::Line::from_str(&buf) {
                        Ok(line) => line,
                        Err(e) => match e {
                            noodles::gff::line::ParseError::InvalidDirective(_) => {
                                continue;
                            }
                            noodles::gff::line::ParseError::InvalidRecord(e) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("invalid record: {buf} error: {e}"),
                                ))
                            }
                        },
                    };
                    buf.clear();
                    return Ok(Some(line));
                }
                Err(e) => return Err(e),
            };
        }
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut gff_array_builder = GFFArrayBuilder::new();

        for _ in 0..self.config.batch_size {
            match self.read_line().await? {
                None => break,
                Some(line) => match line {
                    noodles::gff::Line::Comment(_) => {}
                    noodles::gff::Line::Directive(_) => {}
                    noodles::gff::Line::Record(record) => {
                        gff_array_builder.append(&record)?;
                    }
                },
            }
        }

        if gff_array_builder.len() == 0 {
            return Ok(None);
        }

        let batch =
            RecordBatch::try_new(self.config.file_schema.clone(), gff_array_builder.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use exon_test::test_listing_table_dir;
    use futures::StreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use tokio_util::io::StreamReader;

    #[tokio::test]
    async fn test_streaming_batch_reader() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = Arc::new(super::GFFConfig::new(object_store.clone()));

        let path = test_listing_table_dir("gff", "test.gff");
        let reader = object_store.get(&path).await.unwrap();

        let stream = reader.into_stream();
        let buf_reader = StreamReader::new(stream);

        let batch_reader = super::BatchReader::new(buf_reader, config);

        let batch_stream = batch_reader.into_stream().boxed();

        let batches = batch_stream
            .map(|batch| batch.unwrap())
            .collect::<Vec<_>>()
            .await;

        assert_eq!(batches.len(), 1);

        // Check the first 2 rows of the first batch are what's expected.
        let batch = &batches[0];
        let smaller_batch = batch.slice(0, 2);

        let expected = r#"+---------+--------+------+-------+-----+-------+--------+-------+----------------------------------------+
| seqname | source | type | start | end | score | strand | phase | attributes                             |
+---------+--------+------+-------+-----+-------+--------+-------+----------------------------------------+
| sq0     | caat   | gene | 8     | 13  |       | +      |       | {gene_id: [caat1], gene_name: [gene0]} |
| sq0     | caat   | gene | 8     | 13  |       | +      |       | {gene_id: [caat1], gene_name: [gene0]} |
+---------+--------+------+-------+-----+-------+--------+-------+----------------------------------------+"#;

        let formatted = pretty_format_batches(&[smaller_batch]).unwrap().to_string();

        assert_eq!(formatted, expected);
    }
}

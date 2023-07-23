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

use super::{array_builder::GTFArrayBuilder, GTFConfig};

/// Reads a GTF file into arrow record batches.
pub struct BatchReader<R> {
    /// The reader to read from.
    reader: R,

    /// The configuration for this reader.
    config: Arc<GTFConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    pub fn new(reader: R, config: Arc<GTFConfig>) -> Self {
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

    async fn read_line(&mut self) -> std::io::Result<Option<noodles::gtf::Line>> {
        let mut buf = String::new();
        match self.reader.read_line(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(_) => {
                // remove the new line character, needs to work for both windows and unix
                buf.pop();

                // remove the carriage return character if it exists on windows
                #[cfg(target_os = "windows")]
                if buf.ends_with('\r') {
                    buf.pop();
                }

                let line = match noodles::gtf::Line::from_str(&buf) {
                    Ok(line) => line,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid line: {buf} error: {e}"),
                        ));
                    }
                };
                buf.clear();
                Ok(Some(line))
            }
            Err(e) => Err(e),
        }
    }

    async fn read_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        let mut gtf_array_builder = GTFArrayBuilder::new();

        for _ in 0..self.config.batch_size {
            match self.read_line().await? {
                None => break,
                Some(line) => match line {
                    noodles::gtf::Line::Comment(_) => {}
                    noodles::gtf::Line::Record(record) => {
                        gtf_array_builder.append(&record)?;
                    }
                },
            }
        }

        if gtf_array_builder.len() == 0 {
            return Ok(None);
        }

        let batch =
            RecordBatch::try_new(self.config.file_schema.clone(), gtf_array_builder.finish())?;

        match &self.config.projection {
            Some(projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use tokio_util::io::StreamReader;

    use crate::tests::test_listing_table_dir;

    #[tokio::test]
    async fn test_streaming_batch_reader() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = Arc::new(super::GTFConfig::new(object_store.clone()));

        let path = test_listing_table_dir("gtf", "test.gtf");
        let reader = object_store.get(&path).await.unwrap();

        let stream = reader.into_stream();
        let buf_reader = StreamReader::new(stream);

        let batch_reader = super::BatchReader::new(buf_reader, config);

        let mut batch_stream = batch_reader.into_stream().boxed();

        let mut n_rows = 0;
        while let Some(batch) = batch_stream.next().await {
            let batch = batch.unwrap();
            n_rows += batch.num_rows();
        }
        assert_eq!(n_rows, 77);
    }
}

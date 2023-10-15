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

use std::{io::BufRead, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};

use futures::Stream;
use gb_io::reader;

use super::{array_builder::GenbankArrayBuilder, config::GenbankConfig};

/// A batch reader for Genbank files.
pub struct BatchReader<R>
where
    R: BufRead,
{
    /// The underlying Genbank reader.
    reader: reader::SeqReader<R>,

    /// The Genbank configuration.
    config: Arc<GenbankConfig>,
}

impl<R> BatchReader<R>
where
    R: BufRead,
{
    pub fn new(inner_reader: R, config: Arc<GenbankConfig>) -> Self {
        let reader = reader::SeqReader::new(inner_reader);
        Self { reader, config }
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch() {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut genbank_array_builder = GenbankArrayBuilder::new();

        for _ in 0..self.config.batch_size {
            match self.reader.next() {
                None => break,
                Some(Ok(seq)) => {
                    genbank_array_builder.append(&seq);
                }
                Some(Err(e)) => {
                    return Err(ArrowError::ExternalError(Box::new(e)));
                }
            }
        }

        if genbank_array_builder.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(
            self.config.file_schema.clone(),
            genbank_array_builder.finish(),
        )?;

        match self.config.projection {
            Some(ref projection) => Ok(Some(batch.project(projection)?)),
            None => Ok(Some(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::tests::test_listing_table_dir;

    #[tokio::test]
    async fn test_streaming_batch_reader() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = Arc::new(super::GenbankConfig::new(object_store.clone()));

        let path = test_listing_table_dir("genbank", "test.gb");
        let reader = object_store.get(&path).await.unwrap();

        let bytes = reader.bytes().await.unwrap();

        let cursor = std::io::Cursor::new(bytes);
        let buf_reader = std::io::BufReader::new(cursor);

        let batch_reader = super::BatchReader::new(buf_reader, config);

        let mut batch_stream = batch_reader.into_stream().boxed();

        let mut n_rows = 0;
        while let Some(batch) = batch_stream.next().await {
            let batch = batch.unwrap();
            n_rows += batch.num_rows();
        }
        assert_eq!(n_rows, 1);
    }
}

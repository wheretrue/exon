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
use tokio::io::AsyncBufRead;

use super::{array_builder::MzMLArrayBuilder, config::MzMLConfig, mzml_reader::parser::MzMLReader};

/// A reader for MzML files that reads in batches.
pub struct BatchReader<R>
where
    R: AsyncBufRead + Unpin,
{
    /// The underlying MzML reader.
    reader: MzMLReader<R>,

    /// The configuration for this reader.
    config: Arc<MzMLConfig>,
}

impl<R> BatchReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R, config: Arc<MzMLConfig>) -> Self {
        let reader = MzMLReader::from_reader(reader);

        Self { reader, config }
    }

    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch().await {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(ArrowError::ExternalError(Box::new(e))), reader)),
            }
        })
    }

    pub async fn read_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut array_builder = MzMLArrayBuilder::new();

        for _ in 0..self.config.batch_size {
            match self.reader.read_spectrum().await? {
                Some(spectrum) => {
                    array_builder.append(&spectrum).unwrap();
                }
                None => {
                    break;
                }
            }
        }

        if array_builder.len() == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(self.config.file_schema.clone(), array_builder.finish())?;
        match &self.config.projection {
            Some(projection) => {
                let projected_batch = batch.project(projection)?;

                Ok(Some(projected_batch))
            }

            None => Ok(Some(batch)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use futures::StreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use tokio_util::io::StreamReader;

    use crate::tests::test_listing_table_dir;

    #[tokio::test]
    async fn test_mzml_batch_reader() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = Arc::new(super::MzMLConfig::new(object_store.clone()));

        let path = test_listing_table_dir("mzml", "test.mzML");
        let reader = object_store.get(&path).await.unwrap();

        let stream = reader.into_stream();
        let buf_reader = StreamReader::new(stream);

        let mut batch_reader = super::BatchReader::new(buf_reader, config)
            .into_stream()
            .boxed();

        while let Some(batch) = batch_reader.next().await {
            let batch = batch.unwrap();

            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 6);

            let pretty_print = pretty_format_batches(&[batch]).unwrap();
            eprintln!("{}", pretty_print);
        }
    }
}

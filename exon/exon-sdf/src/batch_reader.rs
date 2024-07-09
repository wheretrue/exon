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

use std::{io::BufRead, sync::Arc};

use arrow::{array::RecordBatch, error::ArrowError};
use exon_common::ExonArrayBuilder;

use crate::config::SDFConfig;

pub struct BatchReader<R> {
    reader: crate::io::Reader<R>,
    config: Arc<crate::config::SDFConfig>,
}

impl<R> BatchReader<R>
where
    R: BufRead,
{
    pub fn new(inner: R, config: Arc<SDFConfig>) -> Self {
        BatchReader {
            reader: crate::io::Reader::new(inner),
            config,
        }
    }

    pub fn read_batch(&mut self) -> crate::Result<Option<RecordBatch>> {
        let file_schema = self.config.file_schema.clone();
        let mut array_builder = crate::array_builder::SDFArrayBuilder::new(
            file_schema.fields().clone(),
            self.config.clone(),
        )?;

        for _ in 0..self.config.batch_size {
            match self.reader.read_record()? {
                Some(record) => array_builder.append_value(record)?,
                None => break,
            }
        }

        if array_builder.is_empty() {
            Ok(None)
        } else {
            // let finished_builder = array_builder.finish();

            let schema = self.config.projected_schema()?;
            let rb = array_builder.try_into_record_batch(schema)?;

            Ok(Some(rb))
        }
    }

    pub fn into_stream(self) -> impl futures::Stream<Item = Result<RecordBatch, ArrowError>> {
        futures::stream::unfold(self, |mut reader| async move {
            match reader.read_batch() {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => {
                    let arrow_error = e.into();
                    Some((Err(arrow_error), reader))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    use crate::schema_builder::SDFSchemaBuilder;

    use super::*;

    #[test]
    fn test_read_batch() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        let cargo_manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let file_path = format!("{}/test-data/tox_benchmark_N6512.sdf", cargo_manifest);
        let mut file = std::fs::File::open(file_path)
            .map(std::io::BufReader::new)
            .unwrap();

        let sdf_schema = SDFSchemaBuilder::default().build();
        let file_schema = sdf_schema.file_schema().unwrap();

        let local_store = Arc::new(object_store::local::LocalFileSystem::new());
        let config = crate::config::SDFConfig::new(local_store, 1, file_schema);

        let mut batch_reader = BatchReader::new(&mut file, Arc::new(config));

        let batch = batch_reader.read_batch().unwrap().unwrap();

        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);
    }
}

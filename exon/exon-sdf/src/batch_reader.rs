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

use std::io::BufRead;

use arrow::array::RecordBatch;

use crate::config::SDFConfig;

pub struct BatchReader<R> {
    reader: crate::io::Reader<R>,
    config: crate::config::SDFConfig,
}

impl<R> BatchReader<R>
where
    R: BufRead,
{
    pub fn new(inner: R, config: SDFConfig) -> Self {
        BatchReader {
            reader: crate::io::Reader::new(inner),
            config,
        }
    }

    pub fn read_batch(&mut self) -> crate::Result<Option<RecordBatch>> {
        let file_schema = self.config.file_schema.clone();
        let mut array_builder =
            crate::array_builder::SDFArrayBuilder::new(file_schema.fields().clone())?;

        for _ in 0..self.config.batch_size {
            match self.reader.read_record()? {
                Some(record) => array_builder.append_value(record)?,
                None => break,
            }
        }

        if array_builder.is_empty() {
            Ok(None)
        } else {
            let finished_builder = array_builder.finish();

            let rb = RecordBatch::try_new(self.config.file_schema.clone(), finished_builder)?;
            Ok(Some(rb))
        }
    }
}

#[cfg(test)]
mod tests {
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

        let molfile_content = r#"
Methane
Example

2  1  0  0  0  0            999 V2000
    0.0000    0.0000    0.0000 C   0  0  0  0  0  0
    0.0000    1.0000    0.0000 H   0  0  0  0  0  0
1  2  1  0  0  0
M  END
>  <canonical_smiles>
CCC

$$$$
"#
        .trim();

        let mut cursor = std::io::Cursor::new(molfile_content);

        let sdf_schema = SDFSchemaBuilder::default().build();
        let file_schema = sdf_schema.file_schema().unwrap();
        let config = crate::config::SDFConfig::new(1, file_schema);

        let mut batch_reader = BatchReader::new(&mut cursor, config);

        let batch = batch_reader.read_batch().unwrap().unwrap();

        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);
    }
}

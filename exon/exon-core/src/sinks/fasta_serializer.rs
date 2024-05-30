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

use bytes::Bytes;
use datafusion::datasource::file_format::write::BatchSerializer;
use noodles::fasta::{
    record::{Definition, Sequence},
    Record,
};

#[derive(Debug, Default)]
pub(crate) struct FASTASerializer {}

impl BatchSerializer for FASTASerializer {
    fn serialize(
        &self,
        batch: arrow::array::RecordBatch,
        _initial: bool,
    ) -> datafusion::error::Result<bytes::Bytes> {
        let ids = if let Some(column) = batch.column_by_name("ids") {
            column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "ids should be a string array".to_string(),
                    )
                })?
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "ids column not found".to_string(),
            ));
        };

        let descriptions = if let Some(column) = batch.column_by_name("descriptions") {
            column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "descriptions should be a string array".to_string(),
                    )
                })?
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "descriptions column not found".to_string(),
            ));
        };

        let sequences = if let Some(column) = batch.column_by_name("sequences") {
            column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "sequences should be a string array".to_string(),
                    )
                })?
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                "sequences column not found".to_string(),
            ));
        };

        let b = Vec::new();
        let mut fasta_writer = noodles::fasta::writer::Writer::new(b);

        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            let description = descriptions.value(i);
            let sequence = sequences.value(i);

            let definition = Definition::new(id, Some(Vec::from(description)));
            let sequence = Sequence::from(Vec::from(sequence));

            let record = Record::new(definition, sequence);
            fasta_writer.write_record(&record)?;
        }

        // todo benchmark this
        Ok(Bytes::from(fasta_writer.get_ref().to_vec()))
    }
}

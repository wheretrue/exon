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

use arrow::array::StringArray;
use bytes::Bytes;
use datafusion::datasource::file_format::write::BatchSerializer;

use super::columns_from_batch::get_array_column;

#[derive(Debug, Default)]
pub(crate) struct FASTQSerializer {}

impl BatchSerializer for FASTQSerializer {
    fn serialize(
        &self,
        batch: arrow::array::RecordBatch,
        _initial: bool,
    ) -> datafusion::error::Result<bytes::Bytes> {
        let names = get_array_column::<StringArray>(&batch, "name")?;
        let descriptions = get_array_column::<StringArray>(&batch, "description")?;
        let sequences = get_array_column::<StringArray>(&batch, "sequence")?;
        let quality_scores = get_array_column::<StringArray>(&batch, "quality_scores")?;

        let b = Vec::new();
        let mut fasta_writer = noodles::fastq::Writer::new(b);

        for i in 0..batch.num_rows() {
            let id = names.value(i);
            let description = descriptions.value(i);
            let sequence = sequences.value(i);
            let quality_scores = quality_scores.value(i);

            let definition = noodles::fastq::record::Definition::new(id, description);
            let record = noodles::fastq::Record::new(definition, sequence, quality_scores);

            fasta_writer.write_record(&record)?;
        }

        // todo benchmark this
        Ok(Bytes::from(fasta_writer.get_ref().to_vec()))
    }
}

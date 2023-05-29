use std::sync::Arc;

use arrow::{
    array::{ArrayBuilder, ArrayRef, GenericStringBuilder},
    error::ArrowError,
};
use noodles::fastq::Record;

/// A FASTQ record array builder.
pub struct FASTQArrayBuilder {
    /// A builder for the names of the records.
    names: GenericStringBuilder<i32>,
    /// A builder for the descriptions of the records.
    descriptions: GenericStringBuilder<i32>,
    /// A builder for the sequences of the records.
    sequences: GenericStringBuilder<i32>,
    /// A builder for the quality scores of the records.
    quality_scores: GenericStringBuilder<i32>,
}

impl FASTQArrayBuilder {
    pub fn create() -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            quality_scores: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Appends a record.
    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        let name = std::str::from_utf8(record.name()).unwrap();
        self.names.append_value(name);

        let desc = record.description();
        if desc.is_empty() {
            self.descriptions.append_null();
        } else {
            let desc_str = std::str::from_utf8(desc).unwrap();
            self.descriptions.append_value(desc_str);
        }

        let record_sequence = record.sequence();
        let sequence = std::str::from_utf8(record_sequence).unwrap();
        self.sequences.append_value(sequence);

        let record_quality = record.quality_scores();
        let quality = std::str::from_utf8(record_quality).unwrap();
        self.quality_scores.append_value(quality);

        Ok(())
    }

    /// Builds a record array.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();
        let qualities = self.quality_scores.finish();

        vec![
            Arc::new(names),
            Arc::new(descriptions),
            Arc::new(sequences),
            Arc::new(qualities),
        ]
    }
}

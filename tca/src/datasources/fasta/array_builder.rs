use std::sync::Arc;

use arrow::{
    array::{ArrayBuilder, ArrayRef, GenericStringBuilder},
    error::ArrowError,
};
use noodles::fasta::Record;

pub struct FASTAArrayBuilder {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
}

impl FASTAArrayBuilder {
    pub fn create() -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        self.names.append_value(record.name());
        self.descriptions.append_option(record.description());

        let sequence_str = std::str::from_utf8(record.sequence().as_ref()).map_err(|e| {
            ArrowError::ExternalError(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;

        self.sequences.append_value(sequence_str);

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();

        vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)]
    }
}

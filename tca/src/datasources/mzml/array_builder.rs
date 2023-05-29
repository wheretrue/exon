use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef, GenericStringBuilder};

use super::mzml_reader::types::Spectrum;

pub struct MzMLArrayBuilder {
    id: GenericStringBuilder<i32>,
}

impl MzMLArrayBuilder {
    pub fn new() -> Self {
        Self {
            id: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn append(&mut self, record: &Spectrum) {
        self.id.append_value(&record.id);
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let id = self.id.finish();

        vec![Arc::new(id)]
    }
}

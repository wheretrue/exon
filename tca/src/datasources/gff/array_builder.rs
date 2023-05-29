use std::sync::Arc;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, Float32Builder, GenericStringBuilder, Int64Builder, MapBuilder,
    },
    error::ArrowError,
};
use noodles::gff::Record;

pub struct GFFArrayBuilder {
    seqnames: GenericStringBuilder<i32>,
    sources: GenericStringBuilder<i32>,
    feature_types: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    scores: Float32Builder,
    strands: GenericStringBuilder<i32>,
    phases: GenericStringBuilder<i32>,
    attributes: MapBuilder<GenericStringBuilder<i32>, GenericStringBuilder<i32>>,
}

impl GFFArrayBuilder {
    pub fn new() -> Self {
        Self {
            seqnames: GenericStringBuilder::<i32>::new(),
            sources: GenericStringBuilder::<i32>::new(),
            feature_types: GenericStringBuilder::<i32>::new(),
            starts: Int64Builder::new(),
            ends: Int64Builder::new(),
            scores: Float32Builder::new(),
            strands: GenericStringBuilder::<i32>::new(),
            phases: GenericStringBuilder::<i32>::new(),
            attributes: MapBuilder::new(
                None,
                GenericStringBuilder::<i32>::new(),
                GenericStringBuilder::<i32>::new(),
            ),
        }
    }

    pub fn len(&self) -> usize {
        self.seqnames.len()
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        self.seqnames.append_value(record.reference_sequence_name());
        self.sources.append_value(record.source());
        self.feature_types.append_value(record.ty());
        self.starts.append_value(record.start().get() as i64);
        self.ends.append_value(record.end().get() as i64);
        self.scores.append_option(record.score());
        self.strands.append_value(record.strand());
        self.phases
            .append_option(record.phase().map(|p| p.to_string()));

        for entry in record.attributes().iter() {
            self.attributes.keys().append_value(entry.key());
            self.attributes.values().append_value(entry.value());
        }

        self.attributes.append(true)?;

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let seqnames = self.seqnames.finish();
        let sources = self.sources.finish();
        let feature_types = self.feature_types.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let scores = self.scores.finish();
        let strands = self.strands.finish();
        let phases = self.phases.finish();
        let attributes = self.attributes.finish();

        vec![
            Arc::new(seqnames),
            Arc::new(sources),
            Arc::new(feature_types),
            Arc::new(starts),
            Arc::new(ends),
            Arc::new(scores),
            Arc::new(strands),
            Arc::new(phases),
            Arc::new(attributes),
        ]
    }
}

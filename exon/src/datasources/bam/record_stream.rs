use std::fs::File;

use noodles::sam::{alignment::Record, Header};

pub struct RecordIterator {
    /// The underlying SAM reader.
    reader: noodles::bam::Reader<noodles::bgzf::Reader<File>>,

    /// The underlying SAM header.
    header: noodles::sam::Header,
}

impl RecordIterator {
    pub fn new(
        reader: noodles::bam::Reader<noodles::bgzf::Reader<File>>,
        header: Header,
    ) -> std::io::Result<Self> {
        Ok(Self { reader, header })
    }

    fn read_record(&mut self) -> std::io::Result<Option<noodles::sam::alignment::Record>> {
        let mut record = noodles::sam::alignment::Record::default();

        match self.reader.read_record(&self.header, &mut record)? {
            0 => Ok(None),
            _ => Ok(Some(record)),
        }
    }
}

impl Iterator for RecordIterator {
    type Item = std::io::Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

use std::ops::Range;

use noodles::{
    core::{region::Interval, Region},
    fasta::fai::Record,
};

pub(crate) struct FAIFileRange {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) region_name: String,
}

pub(crate) fn compute_fai_range(region: &Region, index_record: &Record) -> Option<FAIFileRange> {
    if region.name() != index_record.name() {
        return None;
    }

    let range = interval_to_slice_range(region.interval(), index_record.length() as usize);

    let start = index_record.offset()
        + range.start as u64 / index_record.line_bases() * index_record.line_width()
        + range.start as u64 % index_record.line_bases();

    let end = start + range.len() as u64;

    Some(FAIFileRange {
        start: start as i64,
        end: end as i64,
        region_name: region.to_string(),
    })
}

// Shifts a 1-based interval to a 0-based range for slicing.
// Based on the noodles function.
fn interval_to_slice_range<I>(interval: I, len: usize) -> Range<usize>
where
    I: Into<Interval>,
{
    let interval = interval.into();

    let start = interval
        .start()
        .map(|position| usize::from(position) - 1)
        .unwrap_or(usize::MIN);

    let end = interval.end().map(usize::from).unwrap_or(len);

    start..end
}

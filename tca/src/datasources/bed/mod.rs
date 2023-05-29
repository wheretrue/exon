//! BED data source.
//!
//! This module provides functionality for working with BED files as a data source.

mod array_builder;
mod batch_reader;
mod bed_record_builder;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use self::config::BEDConfig;
pub use self::file_format::BEDFormat;
pub use self::file_opener::BEDOpener;
pub use self::scanner::BEDScan;

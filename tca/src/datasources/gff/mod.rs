//! GFF Datasource Module
//!
//! This module provides functionality for working with GFF files as a data source.

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use self::config::GFFConfig;
pub use self::file_format::GFFFormat;
pub use self::file_opener::GFFOpener;
pub use self::scanner::GFFScan;

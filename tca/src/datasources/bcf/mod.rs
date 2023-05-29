//! BCF data source.
//!
//! This module provides functionality for working with BCF files as a data source.

mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use self::config::BCFConfig;
pub use self::file_format::BCFFormat;
pub use self::file_opener::BCFOpener;
pub use self::scanner::BCFScan;

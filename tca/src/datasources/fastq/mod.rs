//! FASTQ data source.
//!
//! This module provides functionality for working with FASTQ files as a data source.

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use self::config::FASTQConfig;
pub use self::file_format::FASTQFormat;
pub use self::file_opener::FASTQOpener;
pub use self::scanner::FASTQScan;

//! Genbank Datasource Module

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use self::config::GenbankConfig;
pub use self::file_format::GenbankFormat;
pub use self::file_opener::GenbankOpener;
pub use self::scanner::GenbankScan;

//! VCF data source.
//!
//! This module provides functionality for working with VCF files as a data source.

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;
mod schema_builder;

pub use self::array_builder::VCFArrayBuilder;
pub use self::config::VCFConfig;
pub use self::file_format::VCFFormat;
pub use self::file_opener::VCFOpener;
pub use self::scanner::VCFScan;
pub use self::schema_builder::VCFSchemaBuilder;

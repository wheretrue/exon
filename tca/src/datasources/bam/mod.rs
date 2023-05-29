//! Data source for BAM files.

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod scanner;

pub use config::BAMConfig;
pub use file_format::BAMFormat;
pub use file_opener::BAMOpener;
pub use scanner::BAMScan;

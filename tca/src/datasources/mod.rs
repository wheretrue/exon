//! Datasources module.

/// BAM module.
pub mod bam;

/// BCF module.
pub mod bcf;

/// BED module.
pub mod bed;

/// FASTA module.
pub mod fasta;

/// FASTQ module.
pub mod fastq;

/// GenBank module.
pub mod genbank;

/// GFF module.
pub mod gff;

/// HMMDOMTAB module.
pub mod hmmdomtab;

/// MzML module.
#[cfg(feature = "mzml")]
pub mod mzml;

/// SAM module.
pub mod sam;

/// VCF module.
pub mod vcf;

/// Default batch size.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// File types.
mod tca_file_type;
pub use self::tca_file_type::infer_tca_format;
pub use self::tca_file_type::TCAFileType;

/// ListingTableFactory
mod tca_listing_table_factory;
pub use self::tca_listing_table_factory::TCAListingTableFactory;

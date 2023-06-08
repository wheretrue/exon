// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
mod exon_file_type;
pub use self::exon_file_type::infer_exon_format;
pub use self::exon_file_type::ExonFileType;

/// ListingTableFactory
mod exon_listing_table_factory;
pub use self::exon_listing_table_factory::ExonListingTableFactory;

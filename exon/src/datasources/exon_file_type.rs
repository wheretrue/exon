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

use std::{fmt::Display, str::FromStr, sync::Arc};

use datafusion::{
    datasource::file_format::{file_type::FileCompressionType, FileFormat},
    error::DataFusionError,
};

use super::{
    bam::BAMFormat, bcf::BCFFormat, bed::BEDFormat, fasta::FASTAFormat, fastq::FASTQFormat,
    genbank::GenbankFormat, gff::GFFFormat, hmmdomtab::HMMDomTabFormat, sam::SAMFormat,
    vcf::VCFFormat,
};

#[cfg(feature = "mzml")]
use super::mzml::MzMLFormat;

/// The type of file.
pub enum ExonFileType {
    /// FASTA file format.
    FASTA,
    /// FASTQ file format.
    FASTQ,
    /// VCF file format.
    VCF,
    /// BCF file format.
    BCF,
    /// GFF file format.
    GFF,
    /// BAM file format.
    BAM,
    /// SAM file format.
    SAM,
    /// Genbank file format.
    GENBANK,
    /// HMMER file format.
    HMMER,
    /// BED file format.
    BED,

    /// mzML file format.
    #[cfg(feature = "mzml")]
    MZML,
}

impl FromStr for ExonFileType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();

        match s.as_str() {
            "FASTA" | "FA" | "FNA" => Ok(Self::FASTA),
            "FASTQ" | "FQ" => Ok(Self::FASTQ),
            "VCF" => Ok(Self::VCF),
            "BCF" => Ok(Self::BCF),
            "GFF" => Ok(Self::GFF),
            "BAM" => Ok(Self::BAM),
            "SAM" => Ok(Self::SAM),
            #[cfg(feature = "mzml")]
            "MZML" => Ok(Self::MZML),
            "GENBANK" | "GBK" | "GB" => Ok(Self::GENBANK),
            "HMMDOMTAB" => Ok(Self::HMMER),
            "BED" => Ok(Self::BED),
            _ => Err(()),
        }
    }
}

impl Display for ExonFileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FASTA => write!(f, "FASTA"),
            Self::FASTQ => write!(f, "FASTQ"),
            Self::VCF => write!(f, "VCF"),
            Self::BCF => write!(f, "BCF"),
            Self::GFF => write!(f, "GFF"),
            Self::BAM => write!(f, "BAM"),
            Self::SAM => write!(f, "SAM"),
            #[cfg(feature = "mzml")]
            Self::MZML => write!(f, "MZML"),
            Self::GENBANK => write!(f, "GENBANK"),
            Self::HMMER => write!(f, "HMMER"),
            Self::BED => write!(f, "BED"),
        }
    }
}

impl ExonFileType {
    /// Get the file format for the given file type.
    pub fn get_file_format(
        self,
        file_compression_type: FileCompressionType,
    ) -> Result<Arc<dyn FileFormat>, DataFusionError> {
        match self {
            Self::BAM => Ok(Arc::new(BAMFormat::default())),
            Self::BCF => Ok(Arc::new(BCFFormat::default())),
            Self::BED => Ok(Arc::new(BEDFormat::new(file_compression_type))),
            Self::FASTA => Ok(Arc::new(FASTAFormat::new(file_compression_type))),
            Self::FASTQ => Ok(Arc::new(FASTQFormat::new(file_compression_type))),
            Self::GENBANK => Ok(Arc::new(GenbankFormat::new(file_compression_type))),
            Self::GFF => Ok(Arc::new(GFFFormat::new(file_compression_type))),
            Self::HMMER => Ok(Arc::new(HMMDomTabFormat::new(file_compression_type))),
            Self::SAM => Ok(Arc::new(SAMFormat::default())),
            Self::VCF => Ok(Arc::new(VCFFormat::new(file_compression_type))),
            #[cfg(feature = "mzml")]
            Self::MZML => Ok(Arc::new(MzMLFormat::new(file_compression_type))),
        }
    }
}

/// Infer the file type from the file extension.
pub fn infer_exon_format(path: &str) -> Result<Arc<dyn FileFormat>, DataFusionError> {
    let mut exts = path.rsplit('.');
    let mut splitted = exts.next().unwrap_or("");

    let file_compression_type =
        FileCompressionType::from_str(splitted).unwrap_or(FileCompressionType::UNCOMPRESSED);

    if file_compression_type.is_compressed() {
        splitted = exts.next().unwrap_or("");
    }

    let file_type = ExonFileType::from_str(splitted).map_err(|_| {
        DataFusionError::Execution(format!(
            "Unable to infer file type from file extension: {}",
            path
        ))
    })?;

    let file_format = file_type.get_file_format(file_compression_type)?;

    Ok(file_format)
}

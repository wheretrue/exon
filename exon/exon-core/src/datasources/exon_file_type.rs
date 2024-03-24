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

use std::{fmt::Display, str::FromStr};

use datafusion::{
    datasource::file_format::file_compression_type::FileCompressionType, error::DataFusionError,
};

use crate::error::ExonError;

/// The type of file.
#[derive(Debug, Clone)]
pub enum ExonFileType {
    /// FASTA file format.
    FASTA,

    /// FASTA file format.
    FA,

    /// FAA file format (FASTA with amino acids).
    FAA,

    /// FNA file format (FASTA with nucleotides).
    FNA,

    /// FASTQ file format.
    FASTQ,

    /// FQ (FASTQ) file format.
    FQ,

    /// Indexed VCF file format.
    /// This is a special case of VCF file format that must be indexed.
    IndexedVCF,

    /// VCF file format.
    VCF,

    /// BCF file format.
    BCF,

    /// GFF file format.
    GFF,

    /// Indexed GFF file format.
    /// This is a special case of GFF file format that must be indexed.
    IndexedGFF,

    /// BAM file format.
    BAM,

    /// Indexed BAM file format.
    /// This is a special case of BAM file format that must be indexed.
    IndexedBAM,

    /// SAM file format.
    SAM,

    /// HMMER file format.
    HMMDOMTAB,

    /// BED file format.
    BED,

    /// GTF file format.
    GTF,

    /// CRAM file format.
    CRAM,

    /// Genbank file format.
    #[cfg(feature = "genbank")]
    GENBANK,

    /// mzML file format.
    #[cfg(feature = "mzml")]
    MZML,

    /// FCS file format.
    #[cfg(feature = "fcs")]
    FCS,
}

impl FromStr for ExonFileType {
    type Err = ExonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();

        match s.as_str() {
            "FASTA" => Ok(Self::FASTA),
            "FAA" => Ok(Self::FAA),
            "FNA" => Ok(Self::FNA),
            "FASTQ" => Ok(Self::FASTQ),
            "FQ" => Ok(Self::FQ),
            "VCF" => Ok(Self::VCF),
            "INDEXED_VCF" => Ok(Self::IndexedVCF),
            "INDEXED_GFF" => Ok(Self::IndexedGFF),
            "BCF" => Ok(Self::BCF),
            "GFF" => Ok(Self::GFF),
            "BAM" => Ok(Self::BAM),
            "INDEXED_BAM" => Ok(Self::IndexedBAM),
            "SAM" => Ok(Self::SAM),
            #[cfg(feature = "mzml")]
            "MZML" => Ok(Self::MZML),
            #[cfg(feature = "genbank")]
            "GENBANK" | "GBK" | "GB" => Ok(Self::GENBANK),
            "HMMDOMTAB" => Ok(Self::HMMDOMTAB),
            "BED" => Ok(Self::BED),
            "GTF" => Ok(Self::GTF),
            #[cfg(feature = "fcs")]
            "FCS" => Ok(Self::FCS),
            "CRAM" => Ok(Self::CRAM),
            "FA" => Ok(Self::FASTA),
            _ => Err(ExonError::InvalidFileType(s)),
        }
    }
}

impl Display for ExonFileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FQ => write!(f, "FQ"),
            Self::FASTA => write!(f, "FASTA"),
            Self::FAA => write!(f, "FAA"),
            Self::FNA => write!(f, "FNA"),
            Self::FASTQ => write!(f, "FASTQ"),
            Self::VCF => write!(f, "VCF"),
            Self::IndexedVCF => write!(f, "INDEXED_VCF"),
            Self::IndexedBAM => write!(f, "INDEXED_BAM"),
            Self::IndexedGFF => write!(f, "INDEXED_GFF"),
            Self::BCF => write!(f, "BCF"),
            Self::GFF => write!(f, "GFF"),
            Self::BAM => write!(f, "BAM"),
            Self::SAM => write!(f, "SAM"),
            #[cfg(feature = "mzml")]
            Self::MZML => write!(f, "MZML"),
            #[cfg(feature = "genbank")]
            Self::GENBANK => write!(f, "GENBANK"),
            Self::HMMDOMTAB => write!(f, "HMMDOMTAB"),
            Self::BED => write!(f, "BED"),
            Self::GTF => write!(f, "GTF"),
            #[cfg(feature = "fcs")]
            Self::FCS => write!(f, "FCS"),
            Self::CRAM => write!(f, "CRAM"),
            Self::FA => write!(f, "FA"),
        }
    }
}

impl ExonFileType {
    /// Get the file extension for the given file type with the given compression.
    pub fn get_file_extension(&self, file_compression_type: FileCompressionType) -> String {
        match (self, file_compression_type) {
            (_, FileCompressionType::UNCOMPRESSED) => self.to_string(),
            (_, FileCompressionType::GZIP) => format!("{}.gz", self),
            (_, FileCompressionType::ZSTD) => format!("{}.zst", self),
            (_, FileCompressionType::BZIP2) => format!("{}.bz2", self),
            (_, FileCompressionType::XZ) => format!("{}.xz", self),
        }
        .to_lowercase()
    }
}

/// Infer the file type from the file extension.
pub fn infer_file_type_and_compression(
    path: &str,
) -> Result<(ExonFileType, FileCompressionType), DataFusionError> {
    let mut exts = path.rsplit('.');
    let mut splitted = exts.next().unwrap_or("");

    let file_compression_type =
        FileCompressionType::from_str(splitted).unwrap_or(FileCompressionType::UNCOMPRESSED);

    if file_compression_type.is_compressed() {
        splitted = exts.next().unwrap_or("");
    }

    let file_type = ExonFileType::from_str(splitted).map_err(|_| {
        DataFusionError::Execution(format!(
            "Unable to infer file type from file extension: {path}"
        ))
    })?;

    Ok((file_type, file_compression_type))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::ExonFileType;

    #[test]
    fn test_display() {
        assert_eq!(ExonFileType::FASTA.to_string(), "FASTA");
        assert_eq!(ExonFileType::FASTQ.to_string(), "FASTQ");
        assert_eq!(ExonFileType::VCF.to_string(), "VCF");
        assert_eq!(ExonFileType::IndexedVCF.to_string(), "INDEXED_VCF");
        assert_eq!(ExonFileType::BCF.to_string(), "BCF");
        assert_eq!(ExonFileType::GFF.to_string(), "GFF");
        assert_eq!(ExonFileType::BAM.to_string(), "BAM");
        assert_eq!(ExonFileType::IndexedBAM.to_string(), "INDEXED_BAM");
        assert_eq!(ExonFileType::SAM.to_string(), "SAM");
        #[cfg(feature = "genbank")]
        assert_eq!(ExonFileType::GENBANK.to_string(), "GENBANK");
        assert_eq!(ExonFileType::HMMDOMTAB.to_string(), "HMMDOMTAB");
        assert_eq!(ExonFileType::BED.to_string(), "BED");
        #[cfg(feature = "mzml")]
        assert_eq!(ExonFileType::MZML.to_string(), "MZML");
        assert_eq!(ExonFileType::GTF.to_string(), "GTF");
        #[cfg(feature = "fcs")]
        assert_eq!(ExonFileType::FCS.to_string(), "FCS");
        assert_eq!(ExonFileType::FQ.to_string(), "FQ");
        assert_eq!(ExonFileType::FAA.to_string(), "FAA");
        assert_eq!(ExonFileType::FNA.to_string(), "FNA");
        assert_eq!(ExonFileType::CRAM.to_string(), "CRAM");
    }

    #[test]
    fn test_from_str_errors() {
        assert!(ExonFileType::from_str("foo").is_err());
    }
}

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
    common::FileCompressionType, datasource::file_format::FileFormat, error::DataFusionError,
};

use super::{
    bam::BAMFormat, bcf::BCFFormat, bed::BEDFormat, fasta::FASTAFormat, fastq::FASTQFormat,
    gff::GFFFormat, gtf::GTFFormat, hmmdomtab::HMMDomTabFormat, sam::SAMFormat, vcf::VCFFormat,
};

#[cfg(feature = "genbank")]
use super::genbank::GenbankFormat;

#[cfg(feature = "mzml")]
use super::mzml::MzMLFormat;

/// The type of file.
#[derive(Debug, Clone)]
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
    /// HMMER file format.
    HMMER,
    /// BED file format.
    BED,
    /// GTF file format.
    GTF,

    /// Genbank file format.
    #[cfg(feature = "genbank")]
    GENBANK,

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
            #[cfg(feature = "genbank")]
            "GENBANK" | "GBK" | "GB" => Ok(Self::GENBANK),
            "HMMDOMTAB" => Ok(Self::HMMER),
            "BED" => Ok(Self::BED),
            "GTF" => Ok(Self::GTF),
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
            #[cfg(feature = "genbank")]
            Self::GENBANK => write!(f, "GENBANK"),
            Self::HMMER => write!(f, "HMMER"),
            Self::BED => write!(f, "BED"),
            Self::GTF => write!(f, "GTF"),
        }
    }
}

impl ExonFileType {
    /// Get the file format for the given file type.
    pub fn get_file_format(
        self,
        file_compression_type: FileCompressionType,
    ) -> Arc<dyn FileFormat> {
        match self {
            Self::BAM => Arc::new(BAMFormat::default()),
            Self::BCF => Arc::new(BCFFormat::default()),
            Self::BED => Arc::new(BEDFormat::new(file_compression_type)),
            Self::FASTA => Arc::new(FASTAFormat::new(file_compression_type)),
            Self::FASTQ => Arc::new(FASTQFormat::new(file_compression_type)),
            #[cfg(feature = "genbank")]
            Self::GENBANK => Arc::new(GenbankFormat::new(file_compression_type)),
            Self::GFF => Arc::new(GFFFormat::new(file_compression_type)),
            Self::HMMER => Arc::new(HMMDomTabFormat::new(file_compression_type)),
            Self::SAM => Arc::new(SAMFormat::default()),
            Self::VCF => Arc::new(VCFFormat::new(file_compression_type)),
            Self::GTF => Arc::new(GTFFormat::new(file_compression_type)),
            #[cfg(feature = "mzml")]
            Self::MZML => Arc::new(MzMLFormat::new(file_compression_type)),
        }
    }

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
            "Unable to infer file type from file extension: {path}"
        ))
    })?;

    let file_format = file_type.get_file_format(file_compression_type);

    Ok(file_format)
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
        assert_eq!(ExonFileType::BCF.to_string(), "BCF");
        assert_eq!(ExonFileType::GFF.to_string(), "GFF");
        assert_eq!(ExonFileType::BAM.to_string(), "BAM");
        assert_eq!(ExonFileType::SAM.to_string(), "SAM");
        #[cfg(feature = "genbank")]
        assert_eq!(ExonFileType::GENBANK.to_string(), "GENBANK");
        assert_eq!(ExonFileType::HMMER.to_string(), "HMMER");
        assert_eq!(ExonFileType::BED.to_string(), "BED");
        #[cfg(feature = "mzml")]
        assert_eq!(ExonFileType::MZML.to_string(), "MZML");
    }

    #[test]
    fn test_from_str_errors() {
        assert!(ExonFileType::from_str("foo").is_err());
    }
}

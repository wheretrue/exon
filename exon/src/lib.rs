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

#![warn(missing_docs)]

//! Exon is a library to facilitate open-ended analysis of scientific data, ease the application of ML models, and provide a common data interface for science and engineering teams.
//!
//! # Overview
//!
//! The main interface for users is through datafusion's `SessionContext` plus the [`ExonSessionExt`] extension trait. This has a number of convenience methods for loading data from various sources.
//!
//! See the `read_*` methods on [`ExonSessionExt`] for more information. For example, `read_fasta`, or `read_gff`. There's also a `read_inferred_exon_table` method that will attempt to infer the data type and compression from the file extension for ease of use.
//!
//! To facilitate those methods, Exon implements a number of traits for DataFusion that serve as a good base for scientific data work. See the [`datasources`] module for more information.
//!
//! [`ExonSessionExt`]: context::ExonSessionExt
//! [`datasources`]: datasources
//!
//! # Examples
//!
//! ## Loading a FASTQ file
//!
//! ```rust
//! use exon::ExonSessionExt;
//!
//! use datafusion::prelude::*;
//! use datafusion::error::Result;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! let df = ctx.read_fastq("test-data/datasources/fastq/test.fastq", None).await?;
//!
//! assert_eq!(df.schema().fields().len(), 4);
//! assert_eq!(df.schema().field(0).name(), "name");
//! assert_eq!(df.schema().field(1).name(), "description");
//! assert_eq!(df.schema().field(2).name(), "sequence");
//! assert_eq!(df.schema().field(3).name(), "quality_scores");
//! # Ok(())
//! # }
//! ```
//!
//! ## Loading a ZSTD-compressed FASTA file
//!
//! ```rust
//! use exon::ExonSessionExt;
//!
//! use datafusion::prelude::*;
//! use datafusion::error::Result;
//! use datafusion::datasource::file_format::file_type::FileCompressionType;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! let file_compression = FileCompressionType::ZSTD;
//! let df = ctx.read_fasta("test-data/datasources/fasta/test.fasta.zstd", Some(file_compression)).await?;
//!
//! assert_eq!(df.schema().fields().len(), 3);
//! assert_eq!(df.schema().field(0).name(), "id");
//! assert_eq!(df.schema().field(1).name(), "description");
//! assert_eq!(df.schema().field(2).name(), "sequence");
//!
//! let results = df.collect().await?;
//! assert_eq!(results.len(), 1);  // 1 batch, small dataset
//!
//! # Ok(())
//! # }
//! ```

mod context;
pub use context::ExonSessionExt;

/// Data sources for Exon.
pub mod datasources;

/// UDFs for Exon.
pub mod udfs;

/// Utilities for moving data across the FFI boundary.
#[cfg(feature = "ffi")]
pub mod ffi;

/// I/O module for Exon.
#[cfg(feature = "aws")]
mod io;

/// Runtime environment for Exon.
#[cfg(any(feature = "aws", feature = "gcp"))]
mod runtime_env;

#[cfg(any(feature = "aws", feature = "gcp"))]
pub use runtime_env::ExonRuntimeEnvExt;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use datafusion::datasource::listing::ListingTableUrl;
    use object_store::path::Path;

    pub fn test_path(data_type: &str, file_name: &str) -> PathBuf {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

        PathBuf::from(manifest_dir)
            .join("test-data")
            .join("datasources")
            .join(data_type)
            .join(file_name)
    }

    /// Get a path to a test listing table directory. A helper function not for main use.
    pub fn test_listing_table_dir(data_type: &str, file_name: &str) -> Path {
        let abs_file_path = test_path(data_type, file_name);
        Path::from_absolute_path(abs_file_path).unwrap()
    }

    pub fn test_listing_table_url(data_type: &str) -> ListingTableUrl {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

        let abs_file_path = PathBuf::from(manifest_dir)
            .join("test-data")
            .join("datasources")
            .join(data_type);

        let ltu = ListingTableUrl::parse(abs_file_path.to_str().unwrap()).unwrap();

        ltu
    }
}

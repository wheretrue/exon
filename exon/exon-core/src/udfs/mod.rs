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

//! User-defined functions (UDFs) for Exon.

/// sequence UDFs for Exon.
pub mod sequence;

/// UDFs for Mass Spectrometry.
pub mod massspec;

/// UDFs for VCF files.
pub mod vcf;

/// UDFs for SAM/BAM files.
pub mod sam;

/// UDFs for GFF files.
pub mod gff;

mod bigwig_region_filter;
pub use bigwig_region_filter::register_bigwig_region_filter_udf;

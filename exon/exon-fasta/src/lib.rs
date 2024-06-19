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
//! Crate to support reading FASTA files into Arrow arrays.
//!
//! Part of the Exon project. See the [Exon documentation](https://www.wheretrue.dev/docs/exon/) for more information.

mod array_builder;
mod batch_reader;
mod config;
mod error;

pub use array_builder::SequenceBuilder;
pub use batch_reader::BatchReader;
pub use config::FASTAConfig;
pub use config::FASTASchemaBuilder;
pub use config::SequenceDataType;
pub use error::ExonFASTAError;
pub use error::ExonFASTAResult;

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

//! Data source for BAM files.
//!
//! Reads BAM files. It is the binary version of SAM files.

mod array_builder;
mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod lazy_array_builder;
mod lazy_record_stream;
mod record_stream;
mod scanner;
pub(crate) mod table_provider;
pub(crate) mod table_provider_factory;

pub use config::BAMConfig;
pub use file_format::BAMFormat;
pub use file_opener::BAMOpener;
pub use scanner::BAMScan;

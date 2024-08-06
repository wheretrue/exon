// Copyright 2024 WHERE TRUE Technologies.
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

mod io;
pub use io::Reader;

mod error;
pub use error::ExonSDFError;
pub use error::Result;

mod record;
pub(crate) use record::Record;

mod array_builder;
mod batch_reader;
mod config;
mod schema_builder;

pub use batch_reader::BatchReader;
pub use config::SDFConfig;
pub use record::parse_to_record;
pub use schema_builder::SDFSchemaBuilder;

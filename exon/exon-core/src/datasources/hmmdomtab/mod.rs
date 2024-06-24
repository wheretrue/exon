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

//! A datafusion compatible datasource for HMMER3 domain tabular output files.

mod hmm_dom_schema_builder;
mod hmm_dom_tab_config;
mod hmm_dom_tab_opener;
mod hmm_dom_tab_scanner;

/// Table provider for HMMER3 domain tabular output files.
pub mod table_provider;

pub use self::hmm_dom_tab_config::HMMDomTabConfig;
pub use self::hmm_dom_tab_opener::HMMDomTabOpener;
pub use self::hmm_dom_tab_scanner::HMMDomTabScan;

mod udtf;
pub use self::udtf::HMMDomTabScanFunction;

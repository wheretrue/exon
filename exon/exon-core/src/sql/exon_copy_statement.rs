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

use datafusion::sql::{
    parser::{CopyToSource, CopyToStatement},
    sqlparser::ast::Value,
};

#[derive(Debug)]
pub(crate) struct ExonCopyToStatement {
    pub source: CopyToSource,
    pub target: String,
    pub stored_as: Option<String>,
    pub options: Vec<(String, Value)>,
}

impl From<CopyToStatement> for ExonCopyToStatement {
    fn from(s: CopyToStatement) -> Self {
        Self {
            source: s.source,
            target: s.target,
            stored_as: s.stored_as,
            options: s.options,
        }
    }
}

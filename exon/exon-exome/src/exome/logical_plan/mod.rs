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

mod create_catalog;
mod create_schema;
mod create_table;
mod drop_catalog;

pub(crate) use create_catalog::CreateExomeCatalog;
pub(crate) use create_schema::CreateExomeSchema;
pub(crate) use create_table::CreateExomeTable;
pub(crate) use drop_catalog::DropExomeCatalog;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    DataFusion(datafusion::logical_expr::LogicalPlan),
}

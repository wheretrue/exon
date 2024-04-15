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

use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::{
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
    },
    physical_plan::ExecutionPlan,
};

pub trait ExonListingOptions {
    fn table_partition_cols(&self) -> Vec<Field>;

    fn file_extension(&self) -> &str;

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug, Clone)]
pub struct ExonListingConfig<T> {
    pub inner: ListingTableConfig,

    pub options: T,
}

impl<T> ExonListingConfig<T>
where
    T: ExonListingOptions + Default + Send + Sync,
{
    pub fn new_with_options(table_path: ListingTableUrl, options: T) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options,
        }
    }
}

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

use std::{str::FromStr, sync::Arc};

use arrow::datatypes::Field;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
    },
    execution::runtime_env::RuntimeEnv,
    physical_plan::ExecutionPlan,
};
use noodles::core::Region;
use object_store::{path::Path, ObjectStore};

use crate::{
    error::ExonError,
    physical_plan::object_store::{parse_url, url_to_object_store_url},
};

pub trait ExonListingOptions: Default + Send + Sync {
    fn table_partition_cols(&self) -> Vec<Field>;

    fn file_extension(&self) -> &str;

    fn file_compression_type(&self) -> FileCompressionType;

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>>;
}

pub trait ExonIndexedListingOptions: ExonListingOptions {
    fn indexed(&self) -> bool;

    fn regions(&self) -> Vec<Region>;

    fn coalesce_regions(&self, regions: Vec<Region>) -> Vec<Region> {
        let mut all_regions = self.regions().clone();
        all_regions.extend(regions);

        all_regions
    }

    async fn create_physical_plan_with_regions(
        &self,
        conf: FileScanConfig,
        region: Vec<Region>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>>;
}

pub trait ExonFileIndexedListingOptions: ExonIndexedListingOptions {
    fn region_file(&self) -> crate::Result<&str>;

    async fn get_regions_from_file(
        &self,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> crate::Result<Vec<Region>> {
        let region_file = self.region_file()?;

        let region_url = parse_url(region_file)?;
        let object_store_url = url_to_object_store_url(&region_url)?;

        let object_store = runtime_env.object_store(object_store_url)?;

        let region_bytes = object_store
            .get(&Path::from_url_path(region_url.path())?)
            .await?
            .bytes()
            .await?;

        // iterate through the lines of the region file and parse them into regions, assume one region per line
        let regions = std::str::from_utf8(&region_bytes)
            .map_err(|e| ExonError::ExecutionError(format!("Error parsing region file: {}", e)))?
            .lines()
            .map(|line| {
                // Strip any whitespace from the line
                let line = line.trim();

                let region = Region::from_str(line).unwrap();

                Ok(region)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(regions)
    }
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

    pub fn first_table_path(&self) -> Option<&ListingTableUrl> {
        self.inner.table_paths.first()
    }
}

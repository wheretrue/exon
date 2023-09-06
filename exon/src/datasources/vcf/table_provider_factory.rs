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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::{listing::ListingTableUrl, provider::TableProviderFactory, TableProvider},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};

use super::{
    table_provider::{ListingVCFTable, VCFListingTableConfig},
    ListingVCFTableOptions,
};

/// A `ListingTableFactory` that adapts Exon FileFormats to `TableProvider`s.
#[derive(Debug, Clone, Default)]
pub struct VCFTableProviderFactory {}

#[async_trait]
impl TableProviderFactory for VCFTableProviderFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let file_compression_type = cmd.file_compression_type.into();

        let options = ListingVCFTableOptions::new(file_compression_type);

        let table_path = ListingTableUrl::parse(&cmd.location)?;
        let schema = options.infer_schema(state, &table_path).await?;

        let config = VCFListingTableConfig::new(table_path).with_options(options);

        let table = ListingVCFTable::try_new(config, schema)?;
        Ok(Arc::new(table))
    }
}

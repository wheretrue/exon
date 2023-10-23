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

use datafusion::error::DataFusionError;

#[async_trait::async_trait]
pub trait ExonClient {
    async fn register_library(&mut self, library_id: String) -> Result<(), DataFusionError>;

    async fn register_library_by_name(
        &mut self,
        library_name: String,
    ) -> Result<(), DataFusionError>;

    async fn create_catalog(
        &mut self,
        catalog_name: String,
        library_id: String,
    ) -> Result<(), DataFusionError>;
}

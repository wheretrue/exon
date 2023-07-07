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
use datafusion::{error::DataFusionError, execution::runtime_env::RuntimeEnv};
use object_store::ObjectStore;

use crate::io::build_s3_object_store;

/// Extension trait for [`RuntimeEnv`] that provides additional methods for Exon use-cases.
#[async_trait]
pub trait ExonRuntimeEnvExt {
    /// Register an S3 object store with the given URL, that's backed by the default AWS credential chain.
    ///
    /// Returns the previous object store registered for this URL, if any.
    async fn register_s3_object_store(
        &self,
        url: &url::Url,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError>;

    /// Register an object store "intelligently" given the URL.
    async fn exon_register_object_store_url(
        &self,
        url: &url::Url,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError>;

    /// Register an object store with the given URI.
    async fn exon_register_object_store_uri(
        &self,
        uri: &str,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError>;
}

#[async_trait]
impl ExonRuntimeEnvExt for Arc<RuntimeEnv> {
    async fn register_s3_object_store(
        &self,
        url: &url::Url,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError> {
        let object_store = match build_s3_object_store(url).await {
            Ok(object_store) => object_store,
            Err(e) => return Err(DataFusionError::Execution(e.to_string())),
        };

        let previous = self.register_object_store(url, object_store);

        Ok(previous)
    }

    /// Register an object store "intelligently" given the URL.
    async fn exon_register_object_store_url(
        &self,
        url: &url::Url,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError> {
        match url.scheme() {
            #[cfg(feature = "aws")]
            "s3" => self.register_s3_object_store(url).await,

            #[cfg(feature = "gcs")]
            "gcs" => {
                use object_store::gcs::GoogleCloudStorageBuilder;

                let gcs = Arc::new(
                    GoogleCloudStorageBuilder::from_env()
                        .with_url(url.to_string())
                        .build()?,
                );

                let previous = self.register_object_store(url, gcs);

                Ok(previous)
            }
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported scheme: {}",
                url.scheme()
            ))),
        }
    }

    /// Register an object store with the given URI.
    async fn exon_register_object_store_uri(
        &self,
        uri: &str,
    ) -> Result<Option<Arc<dyn ObjectStore>>, DataFusionError> {
        match url::Url::parse(uri) {
            Ok(url) => self.exon_register_object_store_url(&url).await,
            // TODO: have this handle tilde expansion
            _ => Ok(None),
        }
    }
}

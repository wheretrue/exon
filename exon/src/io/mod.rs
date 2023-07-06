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

//! I/O module for Exon.

// Code from arrow and lance projects that are Apache 2.0 licensed.

use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::AwsCredential as ObjectStoreAwsCredential;
use object_store::Result as ObjectStoreResult;
use object_store::{aws::AmazonS3Builder, CredentialProvider, ObjectStore};
use tokio::sync::RwLock;
use url::Url;

const AWS_CREDS_CACHE_KEY: &str = "aws_credentials";

#[derive(Debug)]
struct AwsCredentialAdapter {
    pub inner: Arc<dyn ProvideCredentials>,

    cache: Arc<RwLock<HashMap<String, Arc<aws_credential_types::Credentials>>>>,
    credentials_refresh_offset: Duration,
}

impl AwsCredentialAdapter {
    fn new(provider: Arc<dyn ProvideCredentials>, credentials_refresh_offset: Duration) -> Self {
        Self {
            inner: provider,
            cache: Arc::new(RwLock::new(HashMap::new())),
            credentials_refresh_offset,
        }
    }
}

#[async_trait]
impl CredentialProvider for AwsCredentialAdapter {
    type Credential = ObjectStoreAwsCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
        let cached_creds = {
            let cache_value = self.cache.read().await.get(AWS_CREDS_CACHE_KEY).cloned();
            let expired = cache_value
                .clone()
                .map(|cred| {
                    cred.expiry()
                        .map(|exp| {
                            exp.checked_sub(self.credentials_refresh_offset)
                                .expect("this time should always be valid")
                                < SystemTime::now()
                        })
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            if expired {
                None
            } else {
                cache_value.clone()
            }
        };

        if let Some(creds) = cached_creds {
            Ok(Arc::new(Self::Credential {
                key_id: creds.access_key_id().to_string(),
                secret_key: creds.secret_access_key().to_string(),
                token: creds.session_token().map(|s| s.to_string()),
            }))
        } else {
            let refreshed_creds = Arc::new(self.inner.provide_credentials().await.unwrap());

            self.cache
                .write()
                .await
                .insert(AWS_CREDS_CACHE_KEY.to_string(), refreshed_creds.clone());

            Ok(Arc::new(Self::Credential {
                key_id: refreshed_creds.access_key_id().to_string(),
                secret_key: refreshed_creds.secret_access_key().to_string(),
                token: refreshed_creds.session_token().map(|s| s.to_string()),
            }))
        }
    }
}

/// Build an S3 object store from a URL.
pub async fn build_s3_object_store(uri: &Url) -> std::io::Result<Arc<dyn ObjectStore>> {
    use aws_config::meta::region::RegionProviderChain;

    const DEFAULT_REGION: &str = "us-west-2";

    let region_provider = RegionProviderChain::default_provider().or_else(DEFAULT_REGION);

    let credentials_provider = DefaultCredentialsChain::builder()
        .region(region_provider.region().await)
        .build()
        .await;

    let credentials_refresh_offset = Duration::from_secs(60);

    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_url(uri.to_owned())
            .with_credentials(Arc::new(AwsCredentialAdapter::new(
                Arc::new(credentials_provider),
                credentials_refresh_offset,
            )))
            .with_region(
                region_provider
                    .region()
                    .await
                    .map(|r| r.as_ref().to_string())
                    .unwrap_or(DEFAULT_REGION.to_string()),
            )
            .build()?,
    ))
}

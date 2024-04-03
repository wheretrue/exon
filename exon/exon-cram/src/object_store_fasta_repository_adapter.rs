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

use std::{collections::HashMap, sync::Arc};

use noodles::fasta::{self, fai, repository::Adapter};
use object_store::{path::Path, ObjectStore};

pub struct ObjectStoreFastaRepositoryAdapter {
    object_store: Arc<dyn ObjectStore>,
    fasta_path: Path,
    index_records: HashMap<String, fai::Record>,
    internal_cache: HashMap<String, fasta::Record>,
}

impl ObjectStoreFastaRepositoryAdapter {
    pub async fn try_new(
        object_store: Arc<dyn ObjectStore>,
        fasta_path: String,
    ) -> std::io::Result<Self> {
        // if the fasta_path is on S3, we need to remove the s3:// and bucket
        // prefix from the path

        let fasta_path = if fasta_path.starts_with("s3://") {
            let mut parts = fasta_path.split('/');
            let _ = parts.next();
            let _ = parts.next();
            let bucket = parts.next().unwrap();
            let key = parts.collect::<Vec<&str>>().join("/");

            let key = key.trim_start_matches('/');
            let key = key.trim_end_matches('/');

            tracing::info!(key = &key, "Setting key to new key");
            Path::parse(key)
        } else {
            Path::parse(fasta_path)
        };

        let fasta_path = match fasta_path {
            Ok(path) => path,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid path: {}", e),
                ))
            }
        };

        let index_path_string = format!("{}.fai", fasta_path);
        let index_path = Path::from(index_path_string);

        let index_bytes = object_store.get(&index_path).await?.bytes().await?;
        let index = fai::Reader::new(std::io::Cursor::new(index_bytes)).read_index()?;

        let mut index_records = HashMap::new();
        for record in index {
            let record_name = std::str::from_utf8(record.name()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid name")
            })?;

            index_records.insert(record_name.to_string(), record);
        }

        Ok(Self {
            object_store,
            fasta_path,
            index_records,
            internal_cache: HashMap::new(),
        })
    }
}

impl Adapter for ObjectStoreFastaRepositoryAdapter {
    fn get(&mut self, name: &[u8]) -> Option<std::io::Result<noodles::fasta::Record>> {
        let name = if let Ok(name) = std::str::from_utf8(name) {
            name
        } else {
            return Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid name",
            )));
        };

        if let Some(record) = self.internal_cache.get(name) {
            return Some(Ok(record.clone()));
        }

        let _index_record = match self.index_records.get(name) {
            Some(index_record) => index_record,
            None => return None,
        };

        tracing::info!(
            fasta_path = &self.fasta_path.to_string(),
            "Fetching FASTA record for CRAM Adapter"
        );

        let sequence_bytes_result = futures::executor::block_on(async {
            let byte_stream = self
                .object_store
                .get(&self.fasta_path)
                .await?
                .bytes()
                .await?;

            Ok(byte_stream)
        });

        let sequence_bytes = match sequence_bytes_result {
            Ok(bytes) => bytes,
            Err(e) => return Some(Err(e)),
        };

        let cursor = std::io::Cursor::new(sequence_bytes);
        let mut fasta_reader = fasta::Reader::new(cursor);

        let mut records = fasta_reader.records();
        let record = records.next().unwrap().unwrap();

        tracing::trace!(name = name, "Inserting record into cache");
        self.internal_cache.insert(name.to_string(), record.clone());

        Some(Ok(record))
    }
}

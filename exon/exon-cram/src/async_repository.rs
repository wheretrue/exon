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

use std::{collections::HashMap, ops::Range, sync::Arc};

use noodles::fasta::{
    self, fai,
    record::{Definition, Sequence},
    repository::Adapter,
};
use object_store::{path::Path, GetOptions, GetRange, ObjectStore};

pub struct ObjectStoreFastaRepositoryAdapter {
    object_store: Arc<dyn ObjectStore>,
    fasta_path: Path,
    index_records: HashMap<String, fai::Record>,
    internal_cache: HashMap<String, fasta::Record>,
}

impl ObjectStoreFastaRepositoryAdapter {
    pub async fn try_new(
        object_store: Arc<dyn ObjectStore>,
        fasta_path: Path,
    ) -> std::io::Result<Self> {
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

        let index_record = match self.index_records.get(name) {
            Some(index_record) => index_record,
            None => return None,
        };

        let start = index_record.offset() as usize;
        let end = start + index_record.length() as usize;

        let get_options = GetOptions {
            range: Some(GetRange::Bounded(Range { start, end })),
            ..Default::default()
        };

        // Use block_on to get the bytes from the object store.
        let sequence_bytes = match futures::executor::block_on(async {
            self.object_store
                .get_opts(&self.fasta_path, get_options)
                .await?
                .bytes()
                .await
        }) {
            Ok(bytes) => bytes,
            Err(e) => return Some(Err(e.into())),
        };

        // https://github.com/zaeleus/noodles/blob/4f7254271f3dad76bae9994a866caa06511aaad7/noodles-fasta/src/reader.rs#L260C9-L262C62
        let record_definition = Definition::new(name, None);
        let sequence = Sequence::from(sequence_bytes);

        let record = fasta::Record::new(record_definition, sequence);

        eprintln!(
            "Inserting record {}, into cache: {:?}",
            name,
            record.sequence().len()
        );

        self.internal_cache.insert(name.to_string(), record.clone());

        Some(Ok(record))
    }
}

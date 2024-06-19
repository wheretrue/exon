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

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use datafusion::common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};

use datafusion::common::config_err;
use datafusion::error::Result as DfResult;
use exon_fasta::SequenceDataType;

use crate::ExonError;

#[derive(Debug, Clone, Default)]
/// Options for the FASTA data source.
pub struct FASTAOptions {
    file_extension: Option<String>,
    fasta_sequence_data_type: Option<String>,
    sequence_buffer_capacity: Option<String>,
}

impl FASTAOptions {
    /// Get the file extension for the FASTA file. If None, return "fasta".
    pub fn file_extension(&self) -> &str {
        self.file_extension.as_deref().unwrap_or("fasta")
    }

    /// Get the sequence data type for the FASTA file. If None, return Utf8.
    pub fn fasta_sequence_data_type(&self) -> crate::Result<SequenceDataType> {
        if let Some(fasta_sequence_data_type) = &self.fasta_sequence_data_type {
            let sdt = SequenceDataType::from_str(fasta_sequence_data_type)?;
            Ok(sdt)
        } else {
            Ok(SequenceDataType::Utf8)
        }
    }

    /// Get the sequence buffer capacity for the FASTA file as an int. If None, return 512. Also raises an error if the value is not a valid integer.
    pub fn sequence_buffer_capacity(&self) -> crate::Result<usize> {
        if let Some(sequence_buffer_capacity) = &self.sequence_buffer_capacity {
            let sbc = sequence_buffer_capacity.parse::<usize>()?;
            Ok(sbc)
        } else {
            Ok(512)
        }
    }
}

impl TryFrom<&HashMap<String, String>> for FASTAOptions {
    type Error = ExonError;

    fn try_from(map: &HashMap<String, String>) -> crate::Result<Self> {
        let mut options = FASTAOptions::default();
        for (key, value) in map {
            if key.starts_with("fasta.") {
                options.set(key, value)?;
            }
        }
        Ok(options)
    }
}

impl ExtensionOptions for FASTAOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DfResult<()> {
        let (_key, key) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "sequence_data_type" => {
                self.fasta_sequence_data_type.set(key, value)?;
            }
            "file_extension" => {
                self.file_extension.set(key, value)?;
            }
            "sequence_buffer_capacity" => {
                self.sequence_buffer_capacity.set(key, value)?;
            }
            _ => {
                return config_err!(
                    "Config value \"{}\" for value \"{}\" not found on FASTAOptions",
                    key,
                    value
                );
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.fasta_sequence_data_type
            .visit(&mut v, "fasta_sequence_data_type", "");
        self.file_extension.visit(&mut v, "file_extension", "");
        self.sequence_buffer_capacity
            .visit(&mut v, "sequence_buffer_capacity", "");

        v.0
    }
}

impl ConfigExtension for FASTAOptions {
    const PREFIX: &'static str = "fasta";
}

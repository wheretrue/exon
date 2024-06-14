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

use std::any::Any;
use std::fmt::Display;

use datafusion::common::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion::common::config_err;
use datafusion::error::Result as DfResult;
use exon_bed::{ExonBEDError, ExonBEDResult};

#[derive(Debug, Clone, Default)]
/// Options for the BED data source.
pub struct BEDOptions {
    n_fields: Option<String>,
    file_extension: Option<String>,
}

impl BEDOptions {
    /// Get the number of fields in the BED file as an int. If None, return 12. Also raises an error if the value is not a valid integer or isn't in the range 3-12.
    pub fn n_fields(&self) -> ExonBEDResult<usize> {
        if let Some(n_fields) = &self.n_fields {
            let n_fields = n_fields.parse::<usize>()?;
            if !(3..=12).contains(&n_fields) {
                return Err(ExonBEDError::InvalidNumberOfFields(n_fields));
            }
            Ok(n_fields)
        } else {
            Ok(12)
        }
    }

    /// Get the file extension for the BED file. If None, return "bed".
    pub fn file_extension(&self) -> &str {
        self.file_extension.as_deref().unwrap_or("bed")
    }
}

impl ExtensionOptions for BEDOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DfResult<()> {
        let (_key, bed_key) = key.split_once('.').unwrap_or((key, ""));
        let (key, rem) = bed_key.split_once('.').unwrap_or((bed_key, ""));
        match key {
            "n_fields" => {
                self.n_fields.set(rem, value)?;
            }
            "file_extension" => {
                self.file_extension.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on BEDOptions", rem);
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
        self.n_fields.visit(&mut v, "n_fields", "");
        self.file_extension.visit(&mut v, "file_extension", "");

        v.0
    }
}

impl ConfigExtension for BEDOptions {
    const PREFIX: &'static str = "bed";
}

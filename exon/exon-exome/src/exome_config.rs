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

use std::str::FromStr;

use datafusion::config::{ConfigExtension, ExtensionOptions};

use crate::{error::ExomeError, OrganizationName};

enum ConfigKeys {
    ExomeOrganization,
    ExomeLibrary,
}

impl FromStr for ConfigKeys {
    type Err = ExomeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "exome_organization" => Ok(Self::ExomeOrganization),
            "exome_library" => Ok(Self::ExomeLibrary),
            _ => Err(ExomeError::Execution(format!("Unknown config key: {}", s))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExomeConfigExtension {
    exome_organization: String,
    exome_library: String,
}

impl Default for ExomeConfigExtension {
    fn default() -> Self {
        Self {
            exome_organization: "public".to_string(),
            exome_library: "example_library".to_string(),
        }
    }
}

impl ExomeConfigExtension {
    pub fn exome_organization(&self) -> OrganizationName {
        OrganizationName(self.exome_organization.clone())
    }
}

impl ConfigExtension for ExomeConfigExtension {
    const PREFIX: &'static str = "exome";
}

impl ExtensionOptions for ExomeConfigExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        match ConfigKeys::from_str(key)? {
            ConfigKeys::ExomeOrganization => self.exome_organization = value.to_string(),
            ConfigKeys::ExomeLibrary => self.exome_library = value.to_string(),
        }

        Ok(())
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        vec![
            datafusion::config::ConfigEntry {
                key: "exome_organization".to_string(),
                value: Some(self.exome_organization.clone()),
                description: "The organization name for the exome catalog",
            },
            datafusion::config::ConfigEntry {
                key: "exome_library".to_string(),
                value: Some(self.exome_library.clone()),
                description: "The library name for the exome catalog",
            },
        ]
    }
}

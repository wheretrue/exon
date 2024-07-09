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

#[derive(Debug, PartialEq)]
pub struct Bond {
    atom1: usize,
    atom2: usize,
    bond_type: u8,
    stereo: Option<u8>,
    topology: Option<u8>,
    reacting_center: Option<u8>,
}

impl Bond {
    pub(super) fn parse(line: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();

        let topology = parts.get(4).map(|s| s.parse()).transpose()?;
        let reacting_center = parts.get(5).map(|s| s.parse()).transpose()?;
        let stereo = parts.get(3).map(|s| s.parse()).transpose()?;

        Ok(Bond {
            atom1: parts[0].parse()?,
            atom2: parts[1].parse()?,
            bond_type: parts[2].parse()?,
            stereo,
            topology,
            reacting_center,
        })
    }

    pub fn atom1(&self) -> usize {
        self.atom1
    }

    pub fn atom2(&self) -> usize {
        self.atom2
    }

    pub fn bond_type(&self) -> u8 {
        self.bond_type
    }
}

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

mod atom;
mod bond;
mod data;

use atom::Atom;
use bond::Bond;
pub(crate) use data::Data;

#[derive(Debug, PartialEq, Default)]
pub(crate) struct Record {
    header: String,
    atom_count: usize,
    bond_count: usize,
    atoms: Vec<Atom>,
    bonds: Vec<Bond>,
    data: Data,
}

impl Record {
    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn atom_count(&self) -> usize {
        self.atom_count
    }

    pub fn bond_count(&self) -> usize {
        self.bond_count
    }

    pub fn atoms(&self) -> &Vec<Atom> {
        &self.atoms
    }

    pub fn bonds(&self) -> &Vec<Bond> {
        &self.bonds
    }

    pub fn data(&self) -> &Data {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut Data {
        &mut self.data
    }

    fn parse_counts_line(line: &str) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let atom_count = parts[0].parse()?;
        let bond_count = parts[1].parse()?;
        Ok((atom_count, bond_count))
    }
}

impl From<Vec<u8>> for Record {
    fn from(bytes: Vec<u8>) -> Self {
        let content = String::from_utf8(bytes).expect("Invalid UTF-8 sequence");
        Record::from(content.as_str())
    }
}

impl From<&Vec<u8>> for Record {
    fn from(bytes: &Vec<u8>) -> Self {
        let content = String::from_utf8(bytes.clone()).expect("Invalid UTF-8 sequence");
        Record::from(content.as_str())
    }
}

impl From<&str> for Record {
    fn from(content: &str) -> Self {
        let mut lines = content.lines();

        // Parse header (first 3 lines)
        let header = lines.by_ref().take(3).collect::<Vec<_>>().join("\n");

        // Parse counts line
        let counts_line = lines.next().expect("Missing counts line");
        let (atom_count, bond_count) =
            Record::parse_counts_line(counts_line).expect("Failed to parse counts line");

        // Parse atom block
        let mut atoms = Vec::with_capacity(atom_count);
        for _ in 0..atom_count {
            let line = lines.next().expect("Unexpected end of atom block");
            atoms.push(Atom::parse(line).expect("Failed to parse atom"));
        }

        // Parse bond block
        let mut bonds = Vec::with_capacity(bond_count);
        for _ in 0..bond_count {
            let line = lines.next().expect("Unexpected end of bond block");
            bonds.push(Bond::parse(line).expect("Failed to parse bond"));
        }

        // Parse properties block
        let mut properties = Vec::new();

        loop {
            let line = lines.next().expect("Unexpected end of properties block");
            if line.ends_with("END") {
                break;
            }
            properties.push(line);
        }

        let mut data = Data::default();

        let mut line = lines.next().expect("Unexpected end of data block");

        loop {
            if line == "$$$$" {
                break;
            }

            let data_line = lines.next().expect("Unexpected end of data block");
            data.push(line.to_string(), data_line.to_string());

            // blank line
            let _ = lines.next().expect("Unexpected end of data block");

            // next line
            line = lines.next().expect("Unexpected end of data block");
        }

        Record {
            header,
            atom_count,
            bond_count,
            atoms,
            bonds,
            data,
        }
    }
}

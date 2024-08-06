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

use crate::ExonSDFError;

#[derive(Debug, PartialEq, Default)]
pub struct Record {
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

    fn parse_counts_line(line: &str) -> crate::Result<(usize, usize)> {
        let line = line.trim_end();

        // Parse the atom and bond counts from their fixed positions
        // let atom_count_str = &line[0..3].trim();
        let atom_count_str = line.get(0..3).ok_or(ExonSDFError::ParseError(
            "Failed to parse atom count".to_string(),
        ))?;
        let bond_count_str = line.get(3..6).ok_or(ExonSDFError::ParseError(
            "Failed to parse bond count".to_string(),
        ))?;

        // Convert the parsed strings to usize
        let atom_count = atom_count_str.trim().parse().map_err(|_| {
            ExonSDFError::ParseError(format!("Failed to parse atom count: {}", atom_count_str))
        })?;

        let bond_count = bond_count_str.trim().parse().map_err(|_| {
            ExonSDFError::ParseError(format!("Failed to parse bond count: {}", bond_count_str))
        })?;

        Ok((atom_count, bond_count))
    }
}

pub fn parse_to_record(content: &str) -> crate::Result<Record> {
    tracing::trace!("Parsing SDF content: {:?}", content);
    let mut lines = content.lines();

    // Parse header (first 3 lines)
    let header = lines
        .by_ref()
        .take(3)
        .filter_map(|l| {
            let line = l.trim();
            if line.is_empty() {
                None
            } else {
                Some(line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Parse counts line
    let counts_line = lines.next().expect("Missing counts line");
    let (atom_count, bond_count) = Record::parse_counts_line(counts_line)?;

    // Parse atom block
    let mut atoms = Vec::with_capacity(atom_count);
    for _ in 0..atom_count {
        let line = lines.next().ok_or(ExonSDFError::UnexpectedEndofAtomBlock)?;
        atoms.push(Atom::parse(line)?);
    }

    // Parse bond block
    let mut bonds = Vec::with_capacity(bond_count);
    for _ in 0..bond_count {
        let line = lines.next().ok_or(ExonSDFError::UnexpectedEndofBondBlock)?;
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

    let re = regex::Regex::new(r"<(.*?)>").unwrap();

    while let Some(line) = lines.next() {
        if line == "$$$$" {
            break;
        }

        let parsed = if let Some(line) = re.captures(line) {
            line
        } else {
            return Err(ExonSDFError::ParseError(format!(
                "Failed to parse data block: {}",
                line
            )));
        };

        let header = parsed.get(1).unwrap().as_str();
        let mut data_string = String::new();
        loop {
            let line = lines.next().expect("Unexpected end of data block");
            if line.trim() == "$$$$" || line.trim() == "" {
                break;
            }
            data_string.push_str(line);
        }

        data.push(header.to_string(), data_string);

        if line.trim() == "$$$$" {
            break;
        }
    }

    Ok(Record {
        header,
        atom_count,
        bond_count,
        atoms,
        bonds,
        data,
    })
}

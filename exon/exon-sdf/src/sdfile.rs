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
use data::{Data, Datum};

#[derive(Debug)]
pub struct Molfile {
    header: String,
    atom_count: usize,
    bond_count: usize,
    atoms: Vec<Atom>,
    bonds: Vec<Bond>,
    data: Data,
}

impl Molfile {
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

    pub fn parse(content: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut lines = content.lines();

        // Parse header (first 3 lines)
        let header = lines.by_ref().take(3).collect::<Vec<_>>().join("\n");

        // Parse counts line
        let counts_line = lines.next().ok_or("Missing counts line")?;
        let (atom_count, bond_count) = Self::parse_counts_line(counts_line)?;

        // Parse atom block
        let mut atoms = Vec::with_capacity(atom_count);
        for _ in 0..atom_count {
            let line = lines.next().ok_or("Unexpected end of atom block")?;
            atoms.push(Atom::parse(line)?);
        }

        // Parse bond block
        let mut bonds = Vec::with_capacity(bond_count);
        for _ in 0..bond_count {
            let line = lines.next().ok_or("Unexpected end of bond block")?;
            eprintln!("line: {:?}", line);
            bonds.push(Bond::parse(line)?);
        }

        // Parse properties block
        let mut properties = Vec::new();

        // while the line starts with "M" parse the property, once we hit "M END" we're done
        // with the properties and can start the data
        loop {
            let line = lines.next().ok_or("Unexpected end of properties block")?;
            eprintln!("line: {:?}", line);
            if line.ends_with("END") {
                break;
            }
            properties.push(line);
        }

        // move forward one line
        // lines.next();

        let mut data = Data::default();

        let mut line = lines.next().ok_or("Unexpected end of data block")?;

        loop {
            if line == "$$$$" {
                break;
            }

            eprintln!("line: {:?}", line);

            let data_line = lines.next().ok_or("Unexpected end of data block")?;
            data.push(line.to_string(), data_line.to_string());

            // blank line
            let _ = lines.next().ok_or("Unexpected end of data block")?;

            // next line
            line = lines.next().ok_or("Unexpected end of data block")?;
        }

        Ok(Molfile {
            header,
            atom_count,
            bond_count,
            atoms,
            bonds,
            data,
        })
    }

    fn parse_counts_line(line: &str) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let atom_count = parts[0].parse()?;
        let bond_count = parts[1].parse()?;
        Ok((atom_count, bond_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_molfile() {
        let molfile_content = r#"
L-Alanine (13C)
GSMACCS-II10169115362D 1   0.00366   0.00000    0

6  5  0  0  1  0            999 V2000
-0.6622    0.5342    0.0000 C   0  0  2  0  0  0
    0.6622   -0.3000    0.0000 C   0  0  0  0  0  0
-0.7207    2.0817    0.0000 C   1  0  0  0  0  0
-1.8622   -0.3695    0.0000 N   0  3  0  0  0  0
    0.6220   -1.8037    0.0000 O   0  0  0  0  0  0
    1.9464    0.4244    0.0000 O   0  5  0  0  0  0
1  2  1  0  0  0
1  3  1  1  0  0
1  4  1  0  0  0
2  5  2  0  0  0
2  6  1  0  0  0
M  CHG  2   4   1   6  -1
M  ISO  1   3  13
M  END
> 25 <MELTING.POINT>
179.0 - 183.0

> 25 <CRC.NUMBER>
C-0710Dat

$$$$
"#
        .trim();

        let molfile = Molfile::parse(molfile_content).unwrap();
        assert!(molfile.atoms.len() == 6);
        assert!(molfile.bonds.len() == 5);

        assert!(molfile.header.starts_with("L-Alanine"));
        assert!(molfile.atom_count == 6);
        assert!(molfile.bond_count == 5);
        assert!(!molfile.data.is_empty());
    }
}

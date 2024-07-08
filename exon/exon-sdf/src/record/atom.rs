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
pub struct Atom {
    x: f64,
    y: f64,
    z: f64,
    element: String,
    mass_difference: i8,
    charge: i8,
    stereochemistry: Option<i8>,
    hydrogen_count: Option<i8>,
    stereo_care: Option<i8>,
    valence: Option<i8>,
}

impl Atom {
    fn new(
        x: f64,
        y: f64,
        z: f64,
        element: String,
        mass_difference: i8,
        charge: i8,
        stereochemistry: Option<i8>,
        hydrogen_count: Option<i8>,
        stereo_care: Option<i8>,
        valence: Option<i8>,
    ) -> Self {
        Atom {
            x,
            y,
            z,
            element,
            mass_difference,
            charge,
            stereochemistry,
            hydrogen_count,
            stereo_care,
            valence,
        }
    }

    fn with_stereochemistry(mut self, stereochemistry: i8) -> Self {
        self.stereochemistry = Some(stereochemistry);
        self
    }

    fn with_hydrogen_count(mut self, hydrogen_count: i8) -> Self {
        self.hydrogen_count = Some(hydrogen_count);
        self
    }

    fn with_stereo_care(mut self, stereo_care: i8) -> Self {
        self.stereo_care = Some(stereo_care);
        self
    }

    fn with_valence(mut self, valence: i8) -> Self {
        self.valence = Some(valence);
        self
    }

    pub(super) fn parse(line: &str) -> crate::Result<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        tracing::debug!("parts: {:?}", parts);

        let x = parts[0].parse()?;
        let y = parts[1].parse()?;
        let z = parts[2].parse()?;
        let element = parts[3].to_string();
        let mass_difference = parts[4].parse()?;
        let charge = parts[5].parse()?;

        let mut atom = Atom::new(
            x,
            y,
            z,
            element,
            mass_difference,
            charge,
            None,
            None,
            None,
            None,
        );

        if parts.len() > 6 {
            atom = atom.with_stereochemistry(parts[6].parse()?);
        }

        if parts.len() > 7 {
            atom = atom.with_hydrogen_count(parts[7].parse()?);
        }

        if parts.len() > 8 {
            atom = atom.with_stereo_care(parts[8].parse()?);
        }

        if parts.len() > 9 {
            atom = atom.with_valence(parts[9].parse()?);
        }

        Ok(atom)
    }
}

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
    element: Option<String>,
    mass_difference: Option<i8>,
    charge: Option<i8>,
    stereochemistry: Option<i8>,
    hydrogen_count: Option<i8>,
    stereo_care: Option<i8>,
    valence: Option<i8>,
}

impl Atom {
    fn new(x: f64, y: f64, z: f64) -> Self {
        Atom {
            x,
            y,
            z,
            element: None,
            mass_difference: None,
            charge: None,
            stereochemistry: None,
            hydrogen_count: None,
            stereo_care: None,
            valence: None,
        }
    }

    fn with_element_opt(mut self, element: Option<String>) -> Self {
        self.element = element;
        self
    }

    fn with_mass_difference_opt(mut self, mass_difference: Option<i8>) -> Self {
        self.mass_difference = mass_difference;
        self
    }

    fn with_charge_opt(mut self, charge: Option<i8>) -> Self {
        self.charge = charge;
        self
    }

    fn with_stereochemistry_opt(mut self, stereochemistry: Option<i8>) -> Self {
        self.stereochemistry = stereochemistry;
        self
    }

    fn with_hydrogen_count_opt(mut self, hydrogen_count: Option<i8>) -> Self {
        self.hydrogen_count = hydrogen_count;
        self
    }

    fn with_stereo_care_opt(mut self, stereo_care: Option<i8>) -> Self {
        self.stereo_care = stereo_care;
        self
    }

    fn with_valence_opt(mut self, valence: Option<i8>) -> Self {
        self.valence = valence;
        self
    }

    pub(super) fn parse(line: &str) -> crate::Result<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        tracing::debug!("parts: {:?}", parts);

        let x = parts[0].parse().map_err(|e| {
            crate::ExonSDFError::ParseError(format!("Failed to parse x coordinate: {} {}", line, e))
        })?;
        let y = parts[1].parse().map_err(|e| {
            crate::ExonSDFError::ParseError(format!("Failed to parse y coordinate: {}", e))
        })?;
        let z = parts[2].parse().map_err(|e| {
            crate::ExonSDFError::ParseError(format!("Failed to parse z coordinate: {}", e))
        })?;

        let element = parts.get(3).map(|s| s.to_string());
        let mass_difference = parts.get(4).and_then(|s| s.parse::<i8>().ok());
        let charge = parts.get(5).and_then(|s| s.parse::<i8>().ok());
        let stereochemistry = parts.get(6).and_then(|s| s.parse::<i8>().ok());
        let hydrogen_count = parts.get(7).and_then(|s| s.parse::<i8>().ok());
        let stereo_care = parts.get(8).and_then(|s| s.parse::<i8>().ok());
        let valence = parts.get(9).and_then(|s| s.parse::<i8>().ok());

        let atom = Atom::new(x, y, z)
            .with_element_opt(element)
            .with_mass_difference_opt(mass_difference)
            .with_charge_opt(charge)
            .with_stereochemistry_opt(stereochemistry)
            .with_hydrogen_count_opt(hydrogen_count)
            .with_stereo_care_opt(stereo_care)
            .with_valence_opt(valence);

        Ok(atom)
    }
}

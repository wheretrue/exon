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
    stereochemistry: i8,
    hydrogen_count: i8,
    stereo_care: i8,
    valence: i8,
}

impl Atom {
    pub(super) fn parse(line: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        Ok(Atom {
            x: parts[0].parse()?,
            y: parts[1].parse()?,
            z: parts[2].parse()?,
            element: parts[3].to_string(),
            mass_difference: parts[4].parse()?,
            charge: parts[5].parse()?,
            stereochemistry: parts[6].parse()?,
            hydrogen_count: parts[7].parse()?,
            stereo_care: parts[8].parse()?,
            valence: parts[9].parse()?,
        })
    }

    pub fn x(&self) -> f64 {
        self.x
    }

    pub fn y(&self) -> f64 {
        self.y
    }

    pub fn z(&self) -> f64 {
        self.z
    }

    pub fn element(&self) -> &str {
        &self.element
    }

    pub fn mass_difference(&self) -> i8 {
        self.mass_difference
    }

    pub fn charge(&self) -> i8 {
        self.charge
    }

    pub fn stereochemistry(&self) -> i8 {
        self.stereochemistry
    }

    pub fn hydrogen_count(&self) -> i8 {
        self.hydrogen_count
    }

    pub fn stereo_care(&self) -> i8 {
        self.stereo_care
    }

    pub fn valence(&self) -> i8 {
        self.valence
    }
}

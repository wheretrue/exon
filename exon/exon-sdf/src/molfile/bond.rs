#[derive(Debug)]
pub struct Bond {
    atom1: usize,
    atom2: usize,
    bond_type: u8,
    stereo: u8,
    topology: u8,
    reacting_center: u8,
}

impl Bond {
    pub(super) fn parse(line: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        Ok(Bond {
            atom1: parts[0].parse()?,
            atom2: parts[1].parse()?,
            bond_type: parts[2].parse()?,
            stereo: parts[3].parse()?,
            topology: parts[4].parse()?,
            reacting_center: parts[5].parse()?,
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

    pub fn stereo(&self) -> u8 {
        self.stereo
    }

    pub fn topology(&self) -> u8 {
        self.topology
    }

    pub fn reacting_center(&self) -> u8 {
        self.reacting_center
    }
}

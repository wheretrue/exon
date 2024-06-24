use std::{error::Error, fmt::Display};

#[derive(Debug, Clone)]
pub enum ExonSDFError {
    InvalidInput(String),
}

impl Display for ExonSDFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExonSDFError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

impl Error for ExonSDFError {}

use std::fmt;
use std::fmt::Formatter;

pub type Result<T> = std::result::Result<T, CheckError>;

#[derive(Debug, Clone)]
pub enum CheckError {
    Simple(String),
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CheckError::Simple(txt) => write!(f, "{}", txt),
        }
    }
}

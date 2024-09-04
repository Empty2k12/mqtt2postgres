use std::fmt::Display;

use serde_json::Value::{Bool, Number};

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum PGDatatype {
    Numeric,
    Boolean,
    Text,
    Null
}

impl TryFrom<&serde_json::value::Value> for PGDatatype {
    type Error = ();

    fn try_from(value: &serde_json::value::Value) -> Result<Self, Self::Error> {
        match value {
            Number(_) => Ok(Self::Numeric),
            Bool(_) => Ok(Self::Boolean),
            serde_json::Value::String(_) => Ok(Self::Text),
            serde_json::Value::Null => Ok(Self::Null),
            _ => Err(())
        }
    }
}

impl Display for PGDatatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric => write!(f, "numeric"),
            Self::Boolean => write!(f, "boolean"),
            Self::Text => write!(f, "text"),
            Self::Null => write!(f, "text")
        }
    }
}

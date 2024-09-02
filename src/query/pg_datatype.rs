use serde_json::Value::{Bool, Number};

#[derive(Debug, Eq, PartialEq)]
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

impl ToString for PGDatatype {
    fn to_string(&self) -> String {
        match self {
            Self::Numeric => "numeric".into(),
            Self::Boolean => "boolean".into(),
            Self::Text => "text".into(),
            Self::Null => "text".into()
        }
    }
}

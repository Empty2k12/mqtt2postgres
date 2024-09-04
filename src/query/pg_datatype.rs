use std::{fmt::Display, str::from_utf8};

use serde_json::Value::{Bool, Number};
use tokio_postgres::types::Type;

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub enum PGDatatype {
    Numeric,
    Boolean,
    Text,
    Null,
    TimestampWithTimezone
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

impl<'a> tokio_postgres::types::FromSql<'a> for PGDatatype {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8]
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let pg_type = from_utf8(raw)?;
        match pg_type {
            "boolean" => Ok(Self::Boolean),
            "numeric" => Ok(Self::Numeric),
            "text" => Ok(Self::Text),
            "timestamp with time zone" => Ok(Self::TimestampWithTimezone),
            pg_type => panic!("should never happen, see 'accepts' below: {:?}", pg_type)
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty == &Type::VARCHAR
    }
}

impl Display for PGDatatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric => write!(f, "numeric"),
            Self::Boolean => write!(f, "boolean"),
            Self::Text => write!(f, "text"),
            Self::Null => write!(f, "text"),
            Self::TimestampWithTimezone => write!(f, "timestamp with time zone")
        }
    }
}

//! Table Insertion Query Builder

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::Value;

use crate::{Error, IsJson, Query, QueryType, ValidQuery};

use super::pg_datatype::PGDatatype;

#[derive(Debug, Clone)]
pub struct InsertRecord<'a> {
    table_name: String,
    payload: &'a Bytes
}

impl<'a> InsertRecord<'a> {
    /// Creates a new [`ReadQuery`]
    #[must_use = "Creating a query is pointless unless you execute it"]
    pub fn new<S1>(table_name: S1, payload: &'a Bytes) -> Self
    where
        S1: Into<String>
    {
        InsertRecord {
            table_name: table_name.into(),
            payload
        }
    }
}

impl<'a> Query for InsertRecord<'a> {
    fn build(&self) -> Result<ValidQuery, Error> {
        if self.payload.is_json() {
            let entries: HashMap<String, Value> = serde_json::from_slice(self.payload)
                .map_err(|err| Error::JSONError {
                    error: format!("{}", err)
                })?;

            let mut keys = Vec::with_capacity(entries.len());
            let mut values = Vec::with_capacity(entries.len());

            for (key, value) in entries {
                if let Ok(datatype) = PGDatatype::try_from(&value) {
                    keys.push(key);
                    if datatype == PGDatatype::Text {
                        values.push(format!(r#"'{}'"#, value.as_str().unwrap()));
                    } else {
                        values.push(value.to_string());
                    }
                }
            }

            return Ok(format!(
                "INSERT INTO {} ({}) VALUES ({});",
                self.table_name,
                keys.join(", "),
                values.join(", ")
            )
            .into());
        } else {
            return Ok(format!(
                "INSERT INTO {} ({}) VALUES ('{}');",
                self.table_name,
                self.table_name,
                self.payload.escape_ascii().to_string()
            )
            .into());
        }
    }

    fn get_type(&self) -> QueryType {
        QueryType::InsertRecord
    }
}

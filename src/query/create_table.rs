//! Table Creation Query Builder
//!
//!

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::Value::{self, Bool, Number};

use crate::{Error, IsJson, Query, QueryType, ValidQuery};

#[derive(Debug, Clone)]
pub struct CreateTable<'a> {
    table_name: String,
    payload: &'a Bytes,
}

impl<'a> CreateTable<'a> {
    /// Creates a new [`ReadQuery`]
    #[must_use = "Creating a query is pointless unless you execute it"]
    pub fn new<S1>(table_name: S1, payload: &'a Bytes) -> Self
    where
        S1: Into<String>,
    {
        CreateTable {
            table_name: table_name.into(),
            payload,
        }
    }
}

impl<'a> Query for CreateTable<'a> {
    fn build(&self) -> Result<ValidQuery, Error> {
        if self.payload.is_json() {
            let m: HashMap<String, Value> =
                serde_json::from_slice(&self.payload).map_err(|err| Error::JSONError {
                    error: format!("{}", err),
                })?;

            let mut fields = Vec::with_capacity(m.len());

            for (k, v) in m {
                let datatype = extract_datatype(&v);

                // TODO: refactor to use Some/None and to support nested objects
                if datatype != "other" {
                    fields.push(format!("{} {}", k, datatype));
                }
            }

            return Ok(format!("CREATE TABLE IF NOT EXISTS {} (timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, {});", self.table_name, fields.join(", ")).into());
        } else {
            return Ok(format!("CREATE TABLE IF NOT EXISTS {} (timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, {} text);", self.table_name, self.table_name).into());
        }
    }

    fn get_type(&self) -> QueryType {
        QueryType::CreateTable
    }
}

fn extract_datatype(value: &Value) -> &str {
    match value {
        Number(_) => "numeric",
        Bool(_) => "boolean",
        serde_json::Value::String(_) => "text",
        // TODO: how to handle this properly?
        serde_json::Value::Null => "text",
        _ => "other",
    }
}

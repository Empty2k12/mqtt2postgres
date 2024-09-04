//! Table Insertion Query Builder

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::Value;

use crate::{get_global_hashmap, Error, IsJson, Query, QueryType, ValidQuery};

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

fn do_vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    matching == a.len() && matching == b.len()
}

impl<'a> Query for InsertRecord<'a> {
    fn build(&self) -> Result<Vec<ValidQuery>, Error> {
        if self.payload.is_json() {
            let entries: HashMap<String, Value> = serde_json::from_slice(self.payload)
                .map_err(|err| Error::JSONError {
                    error: format!("{}", err)
                })?;

            let mut keys = Vec::new();
            let mut values = Vec::new();
            let mut types = Vec::new();

            for (key, value) in entries {
                if let Ok(datatype) = PGDatatype::try_from(&value) {
                    if datatype == PGDatatype::Text {
                        values.push(format!(r#"'{}'"#, value.as_str().unwrap()));
                        keys.push(key);
                        types.push(datatype);
                    } else if datatype != PGDatatype::Null {
                        values.push(value.to_string());
                        keys.push(key);
                        types.push(datatype);
                    }
                }
            }

            let new_schema = keys.iter().cloned().zip(types.into_iter()).collect();
            if let Some((_table_name, previous_schema)) =
                get_global_hashmap().get_key_value(&self.table_name)
            {
                if do_vecs_match(previous_schema, &new_schema) {
                    println!("Type matches previous type");
                } else {
                    get_global_hashmap()
                        .get_mut(&self.table_name)
                        .map(move |val| *val = new_schema.to_vec());
                    println!("Updated type!!");
                }
            } else {
                get_global_hashmap().insert(self.table_name.clone(), new_schema.to_vec());
            }

            return Ok(vec![format!(
                "INSERT INTO {} ({}) VALUES ({});",
                self.table_name,
                keys.join(", "),
                values.join(", ")
            )
            .into()]);
        } else {
            return Ok(vec![format!(
                "INSERT INTO {} ({}) VALUES ('{}');",
                self.table_name,
                self.table_name,
                self.payload.escape_ascii().to_string()
            )
            .into()]);
        }
    }

    fn get_type(&self) -> QueryType {
        QueryType::InsertRecord
    }
}

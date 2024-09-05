//! Table Insertion Query Builder

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use serde_json::Value;
use tracing::info;

use crate::{Error, IsJson, KnownTableSchemata, Query, QueryType, ValidQuery};

use super::pg_datatype::PGDatatype;

#[derive(Debug)]
pub struct InsertRecord<'a> {
    table_name: String,
    payload: &'a Bytes
}

impl<'a> InsertRecord<'a> {
    /// Creates a new [`InsertRecord`]
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
    fn build(
        &self,
        known_schemata: &mut KnownTableSchemata
    ) -> Result<Vec<ValidQuery>, Error> {
        if self.payload.is_json() {
            let mut queries: Vec<ValidQuery> = vec![];

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
                        keys.push(key.to_lowercase());
                        types.push(datatype);
                    } else if datatype != PGDatatype::Null {
                        values.push(value.to_string());
                        keys.push(key.to_lowercase());
                        types.push(datatype);
                    }
                }
            }

            let new_schema: HashSet<(String, PGDatatype)> =
                keys.iter().cloned().zip(types).collect();

            if let Some((_, previous_schema)) =
                known_schemata.get_key_value(&self.table_name)
            {
                let difference = new_schema.difference(previous_schema);

                if difference.clone().count() > 0 {
                    info!("Altering Table '{}', adding fields {:?}", self.table_name, difference);
                    for diff in difference {
                        let alter_query = format!("ALTER TABLE {} ADD {} {})", self.table_name, diff.0, diff.1.to_string()).into();
                        queries.push(alter_query);
                    }
                }

            } else {
                known_schemata.insert(self.table_name.clone(), new_schema);
            }

            queries.push(format!(
                "INSERT INTO {} ({}) VALUES ({});",
                self.table_name,
                keys.join(", "),
                values.join(", ")
            )
            .into());

            return Ok(queries);
        } else {
            return Ok(vec![format!(
                "INSERT INTO {} ({}) VALUES ('{}');",
                self.table_name,
                self.table_name,
                self.payload.escape_ascii()
            )
            .into()]);
        }
    }

    fn get_type(&self) -> QueryType {
        QueryType::InsertRecord
    }
}

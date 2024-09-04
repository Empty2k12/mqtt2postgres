//! Table Creation Query Builder

use std::collections::HashMap;

use bytes::Bytes;
use serde_json::Value;

use crate::{Error, IsJson, KnownTableSchemata, Query, QueryType, ValidQuery};

use super::pg_datatype::PGDatatype;

#[derive(Debug, Clone)]
pub struct CreateTable<'a> {
    table_name: String,
    payload: &'a Bytes,
    init_hypertable: bool
}

impl<'a> CreateTable<'a> {
    /// Creates a new [`ReadQuery`]
    #[must_use = "Creating a query is pointless unless you execute it"]
    pub fn new<S1>(table_name: S1, payload: &'a Bytes, init_hypertable: bool) -> Self
    where
        S1: Into<String>
    {
        CreateTable {
            table_name: table_name.into(),
            payload,
            init_hypertable
        }
    }
}

impl<'a> Query for CreateTable<'a> {
    fn build(
        &self,
        _known_schemata: &mut KnownTableSchemata
    ) -> Result<Vec<ValidQuery>, Error> {
        let mut queries = vec![];
        if self.payload.is_json() {
            let entries: HashMap<String, Value> = serde_json::from_slice(self.payload)
                .map_err(|err| Error::JSONError {
                    error: format!("{}", err)
                })?;

            let mut fields = Vec::with_capacity(entries.len());

            for (keys, value) in entries {
                if let Ok(datatype) = PGDatatype::try_from(&value) {
                    fields.push(format!("{} {}", keys, datatype));
                }
            }

            queries.push(format!("CREATE TABLE IF NOT EXISTS {} (time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, {});", self.table_name, fields.join(", ")).into());
        } else {
            // TODO: use actual type here instead of 'text'
            queries.push(format!("CREATE TABLE IF NOT EXISTS {} (time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, {} text);", self.table_name, self.table_name).into());
        }

        if self.init_hypertable {
            let hypertable_query: ValidQuery = format!(
                "SELECT create_hypertable('{}', by_range('time'), migrate_data => true);",
                self.table_name
            )
            .into();
            queries.push(hypertable_query);
        }

        return Ok(queries);
    }

    fn get_type(&self) -> QueryType {
        QueryType::CreateTable
    }
}

pub mod create_table;
pub mod insert_record;
pub mod pg_datatype;

use crate::Error;

pub trait Query {
    fn build(&self) -> Result<Vec<ValidQuery>, Error>;

    fn get_type(&self) -> QueryType;
}

#[derive(Debug)]
#[doc(hidden)]
pub struct ValidQuery(String);
impl ValidQuery {
    pub fn get(self) -> String {
        self.0
    }
}
impl<T> From<T> for ValidQuery
where
    T: Into<String>
{
    fn from(string: T) -> Self {
        Self(string.into())
    }
}
impl PartialEq<String> for ValidQuery {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}
impl PartialEq<&str> for ValidQuery {
    fn eq(&self, other: &&str) -> bool {
        &self.0 == other
    }
}

/// Internal Enum used to store the type of query
#[derive(PartialEq, Eq, Debug)]
pub enum QueryType {
    CreateTable,
    InsertRecord
}

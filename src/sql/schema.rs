use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use super::types::{DataTypes, Row, Value};

#[derive(Debug,PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // Check if table is valid
    pub fn validate(&self) -> Result<()> {
        // check column
        if self.columns.is_empty() {
            return Err(Error::Internal(format!("table {} has no column", self.name)));
        };

        // check primary key
        match self.columns.iter().filter(|c|c.primary_key).count() {
            1 => {},
            0 => return Err(Error::Internal(format!("No primary key for table {}", self.name))),
            _ => return Err(Error::Internal(format!("Multiple primary key for table {}", self.name))),
        }

        Ok(())
    }

    pub fn get_primary_key(&self, row:&Row) -> Result<Value> {
        let pos = self.columns.iter().position(|c|c.primary_key).expect("No primary key found");
        Ok(row[pos].clone())
    }
    
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataTypes,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}


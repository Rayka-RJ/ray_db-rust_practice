use crate::sql::types::DataTypes;
use std::convert::From;

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {name: String, columns: Vec<Column>},
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>
    },
    Select { table_name: String},
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataTypes,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Consts(Consts),
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}


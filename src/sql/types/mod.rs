use serde::{Deserialize, Serialize};

use super::parser::ast::{Consts, Expression};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DataTypes {
    Boolean,
    String,
    Integer,
    Float,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression_to_value(expr:Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    } 

    pub fn datatype(&self) -> Option<DataTypes> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataTypes::Boolean),
            Value::Integer(_) => Some(DataTypes::Integer),
            Value::Float(_) => Some(DataTypes::Float),
            Value::String(_) => Some(DataTypes::String),
        }
    }
}

pub type Row = Vec<Value>;
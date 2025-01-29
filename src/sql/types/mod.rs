use std::fmt::Display;

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

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
                Value::Null => write!(f, "{}", "NULL"),
                Value::Boolean(b) if *b => write!(f, "{}", "TRUE"),
                Value::Boolean(_) => write!(f, "{}", "FALSE"),
                Value::Integer(v) => write!(f, "{}", v),
                Value::Float(v) => write!(f, "{}", v),
                Value::String(v) => write!(f, "{}", v),
        }
    }
}

pub type Row = Vec<Value>;
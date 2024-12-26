use std::{collections::btree_map::Values, fmt::Display, iter::Peekable, str::Chars};
use crate::error::{Error, Result};


// Pre-define Part
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    Ident(String),
    String(String),
    Number(String),
    OpenParen,
    CloseParen,
    Comma,
    Semicolon,
    Asterisk,
    Plus,
    Minus,
    Slash,
}


impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(v) => v,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
        })
    }
}


#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
}

impl Keyword {
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String, 
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::String,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Bool => "BOOL",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Create => "CREATE",
            Keyword::Default => "DEFAULT",
            Keyword::Double => "DOUBLE",
            Keyword::False => "FALSE",
            Keyword::Float => "FLOAT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Into => "INTO",
            Keyword::Key => "KEY",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Select => "SELECT",
            Keyword::String => "STRING",
            Keyword::Table => "TABLE",
            Keyword::Text => "TEXT",
            Keyword::True => "TRUE",
            Keyword::Values => "VALUES",
            Keyword::Varchar => "VARCHAR",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// Lexer Definition
// SQL for now:
//
// 1. Create Table
// ---------------------------
// CREATE TABLE table_name (
//      [ column_name data_type [ column_constraints [...] ] ]
//      [, ...]
//      );
//
//      where data_type is:
//      - BOOLEAN(BOOL): type | false
//      - FLOAT(DOUBLE)
//      - INTEGER(INI)
//      - STRING(TEXT, VARCHAR)
//
//      where column_constraints is:
//      [ NOT NULL | NULL | DEFAULT expr ]
//
// 2. Insert Into
// ---------------------------
// INSERT INTO table_name
// [ ( column_name [, ...] ) ]
// values ( expr [, ...] );
//
// 3. Select * From
// ---------------------------
// SELECT * FROM table_name;

pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text:&'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    fn next_while<F: Fn(char) -> bool > (&mut self, predicate: F) Option<String> {
        let mut Value = String::new();
        
    }

    fn skip_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }
}
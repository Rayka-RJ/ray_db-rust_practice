use std::{fmt::Display, iter::Peekable, str::Chars};
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
            "SELECT" => Keyword::Select,
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

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.iter.peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpected character {}", c)))),
            Err(err) => Some(Err(err)),    
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text:&'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // Iteration methods

    fn next_if<F: Fn(char) -> bool> (&mut self, predicate: F) -> Option<char> {
        if let Some(&c) = self.iter.peek() {
            if predicate(c) {
                return self.iter.next(); // consuming, delete
            }
        }
        None
    }

    fn next_while<F: Fn(char) -> bool> (&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();

        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }

        Some(value).filter(|v|!v.is_empty())
    }

    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }


    // Token Harvest

    fn scan(&mut self) -> Result<Option<Token>> {
        self.skip_whitespace();

        match self.iter.peek() {
            Some('\'') => self.scan_string(), // insert single quotation mark
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    // Scan and Skip Ancillary

    fn skip_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    fn scan_string(&mut self) -> Result<Option<Token>> {
        
        if self.next_if(|c| c=='\'').is_none() {
            return Ok(None);
        }

        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
            }
        }

        Ok(Some(Token::String(val)))
    }

    fn scan_number(&mut self) -> Option<Token> {
        // integer part
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        
        // float part
        if let Some(d) = self.next_if(|c|c == '.') {
            num.push(d);
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }

        Some(Token::Number(num))
    }

    fn scan_ident(&mut self) -> Option<Token> {
        let mut value = self.next_if(|c|c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c|c.is_alphanumeric() || c == '_') {
            value.push(c);
        }

        Some(Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword))
    }

    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            _ => None,
        })
    }
    
}

#[cfg(test)]

mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::Result,
        sql::parser::lexer::{Token, Keyword},
    };

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
            (
                id1 int primary key,
                id2 string
            );
            "
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::String),
                Token::CloseParen,
                Token::Semicolon
            ]
        );

        Ok(())
    }

} 
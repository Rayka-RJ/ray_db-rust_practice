use std::iter::Peekable;
use ast::Column;
use lexer::{Keyword, Lexer, Token};
use crate::error::{Error, Result};
use super::types::DataTypes;

mod lexer;
pub mod ast;

pub struct Parser<'a> {
    lexer:Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    pub fn parse(&mut self) -> Result<ast::Statement>{
        let stmt = self.parse_statement()?;
        self.next_expect(Token::Semicolon)?;
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        } 
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // Check the first Token
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }


    // DDL type
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parse] Unexcepted token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parse] Unexcepted token {}", token))),
        }
    }

    // Parser: SELECT * FROM TABLE
    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        let table_name = self.next_ident()?;
        Ok(ast::Statement::Select { table_name: table_name })
    }

    // Parser: INSERT value INTO TABLE
    // INSERT INTO tbl(a,b,c) VALUES (1,2,3),(4,5,6);
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        let table_name = self.next_ident()?;

        // Check if column clarified
        let cols = if self.next_if_token(Token::OpenParen).is_some() {
            let mut col = Vec::new();
            loop {
                col.push(self.next_ident()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    t => return Err(Error::Parse(format!("[Parser] Unexcepted token {}", t))),
                }
            }
            Some(col)
        } else {
            None
        };

        self.next_expect(Token::Keyword(Keyword::Values))?;
        let mut vals = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    t => return Err(Error::Parse(format!("[Parser] Unexcepted token {}", t))),
                }
            }
            vals.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(ast::Statement::Insert { table_name: table_name, columns: cols, values: vals })
        }


    // Parser: CREATE TABLE
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // Tablename
        let table_name = self.next_ident()?;
        // Openparen
        self.next_expect(Token::OpenParen)?;

        // Column
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable { name: table_name, columns: columns })
    }

    // Column value
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataTypes::Integer,
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => DataTypes::Boolean,
                Token::Keyword(Keyword::Double) | Token::Keyword(Keyword::Float) => DataTypes::Float,
                Token::Keyword(Keyword::Varchar) | Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::String) => DataTypes::String,
                token => return Err(Error::Parse(format!("[Parse] Unexcepted token {}", token))),
            },
            nullable: None,
            default: None,
        };

        // Nullable or Default
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                },
                Keyword::Default => column.default = Some(self.parse_expression()?),
                k => return Err(Error::Parse(format!("[Parser] Unexcepted keyword {}", k))),
            }
        }

        Ok(column)
    }


    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c|c.is_ascii_digit()) {
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(c) => ast::Consts::String(c).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => return Err(Error::Parse(format!("[Parser] Unexpected expression token {}", t))),
        })
    }

    // -+------------------------+-
    //      Auxilliary Part
    // -+------------------------+- 

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parse] Unexcepted end of input"))))
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token=> Err(Error::Parse(format!("[Parser] Excepted ident, got token {}", token))),
        }
    }

    fn next_expect(&mut self, expect:Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!("[Parser] Excepted token {}, got {}", expect, token)));
        }
        Ok(())
    } 

    fn next_if<F: Fn(&Token) -> bool> (&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|c|predicate(c))?;
        self.next().ok()
    }

    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]

mod tests {
    use crate::error::Result;

    use super::Parser;

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            CREATE table tbl (
                a int default 50,
                b float not null,
                c varchar null,
                d bool default false
            );
        ";

        let stmt1 = Parser::new(sql1).parse()?;
        println!("{:?}",stmt1);

        let sql2 = "
            CREATE table tbl (
                a integer default 50,
                b Double not null,
                c String null,
                d boolean default false
            );
        "; 
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1,stmt2);

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()>{
        let sql1 = "SELECT * FROM tbl;";
        let stmt1 = Parser::new(sql1).parse()?;
        println!("{:?}", stmt1);
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()>{
        let sql1 = "insert into tbl values (1,3,'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        println!("{:?}", stmt1);

        let sql2 = "insert into tbl2 (a, b, c) values (2, '5', false), (6,'a','a');";
        let stmt2 = Parser::new(sql2).parse()?;
        println!("{:?}", stmt2);

        Ok(())
    }
}


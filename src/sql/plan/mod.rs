use planner::Planner;
use crate::error::Result;
use super::engine::Transaction;
use super::executor::{Executor, ResultSet};
use super::schema::Table;
use super::parser::ast::{Expression, Statement};
mod planner;

#[derive(Debug, PartialEq)]
pub enum Node {
    //CREATE
    CreateTable {
        schema: Table,
    },
    // INSERT
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },
    // SELECT/Scan
    Scan {
        table_name: String,
    }
}

#[derive(Debug, PartialEq)]
pub struct Plan(pub Node);

impl Plan {
    pub fn build(stmt: Statement) -> Self {
        Planner::new().build(stmt)
    }

    pub fn execute<T: Transaction>(self, txn:&mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
    }
}

#[cfg(test)]
mod tests {
    use crate::{sql::parser::Parser, error::Result};
    use super::Plan;
    
    #[test]
    fn test_plan_create_table() -> Result<()> {
        let sql1 = "
            CREATE table tbl (
                a int default 50,
                b float not null,
                c varchar null,
                d bool default false
            );
        ";

        let stmt1 = Parser::new(sql1).parse()?;
        let plan1 = Plan::build(stmt1);
        println!("{:?}", plan1);

        let sql2 = "
            CREATE table tbl (
                a integer default 50,
                b Double not null,
                c String null,
                d boolean default false
            );
        "; 
        let stmt2 = Parser::new(sql2).parse()?;
        let plan2 = Plan::build(stmt2);
        assert_eq!(plan1,plan2);


        Ok(())
    }

    #[test]
    fn test_plan_insert_table() -> Result<()> {
        let sql1 = "insert into tbl values (1,3,'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let plan1 = Plan::build(stmt1);        
        println!("{:?}", plan1);
        Ok(())
    } 

    #[test]
    fn test_plan_select_table() -> Result<()> {
        let sql1 = "SELECT * FROM tbl;";
        let stmt1 = Parser::new(sql1).parse()?;
        let plan1 = Plan::build(stmt1);
        println!("{:?}", plan1);
        Ok(())
    }
}
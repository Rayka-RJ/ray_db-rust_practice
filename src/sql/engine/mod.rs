use crate::error::{Result, Error};
use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

pub mod kv;

pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(
            Session {
                engine: self.clone(),
            }
        )
    }
}

// Abstract transaction information: Including DDL and DML
// Both KV and distributed engine can access underlyingly. 
pub trait Transaction {
    // Commit 
    fn commit(&self) -> Result<()>;

    // Rollback
    fn rollback(&self) -> Result<()>;

    // Create row
    fn create_row(&mut self, table: String, row: Row) -> Result<()>;

    // Scan table
    fn scan_table(&mut self, table_name: String) -> Result<Vec<Row>>;

    // DDL related transaction
    fn create_table(&mut self, table: Table) -> Result<()>;

    // Get information
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;

    // Check information
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?
        .ok_or(Error::Internal(format!("table {} does not exist", table_name)))
    }

}

pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // construct the plan
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}

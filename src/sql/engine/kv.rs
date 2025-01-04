use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::{schema::Table, types::{Row, Value}}, storage::{self, engine::Engine as StorageEngine}};
use super::{Engine, Transaction};

pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self { kv: self.kv.clone() }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}


// Package of MvccTransaction
pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn:storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}
    
impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // Check if the row is valid
        for (i,col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                Some(dt) if dt != col.datatype => return Err(Error::Internal(format!("Column {} datatype mismatch", col.name))),
                None if col.nullable => {},
                None => return Err(Error::Internal(format!("Column {} cannot be null", col.name))),
                _ => {},
            }
        }

        // insert the data
        // (Temporarily) (todo) set the first row as the primary key
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&mut self, table_name: String) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // Check if it exists
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("Table {} already exists", table.name)));
        } 

        // Check if table is valid
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("table {} has no column", table.name)));
        }

        let key = Key::Table(table.name.clone());
        let val = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, val)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self.txn.get(bincode::serialize(&key)?)?
        .map(|c|bincode::deserialize(&c)).transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Key {
    Table(String),
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

#[cfg(test)]

mod tests {
    use crate::{sql::engine::Engine, storage::memory::MemoryEngine, error::Result};

    use super::KVEngine;


    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        // (temporarily - todo) The first column (a) is default primary key
        s.execute("CREATE TABLE t1 (
        a integer, 
        b int,
        c varchar default 'apple');")?; 

        s.execute("INSERT INTO t1 (b,c,a) VALUES (3,'哈哈', 4);")?;
        s.execute("INSERT INTO t1 VALUES (3, 3, 'lucky');")?;
        s.execute("INSERT INTO t1 VALUES (9, 6);")?;

        let v = s.execute("SELECT * FROM t1;")?;
        println!("{:?}", v);
        Ok(())
    }
}
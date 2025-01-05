use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex, MutexGuard}};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::engine::Engine;

pub struct Mvcc<E: Engine>{
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }    
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng:E) -> Self {
        Self{ engine: Arc::new(Mutex::new(eng)) }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

pub struct TransactionState {
    // Current transaction version
    pub version: Version,
    // Current active transaction version
    pub active_versions: HashSet<Version>,
}

pub type Version = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    Version(Vec<u8>, Version),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Encode fail")
    }
}

// -+------------------------------------+-
//      NextVersion 0
//      TxnActive 1-100 1-101 1-102
//      Version key1-101 key2-101
// -+------------------------------------+-

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

impl<E: Engine> MvccTransaction<E> {

    // Begin a transaction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // 0. Get the storage engine
        let mut engine = eng.lock()?;

        // 1. Get the newest version
        let next_version = match engine.get(MvccKey::NextVersion.encode())?  {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };

        // 2. Save the next version
        engine.set(MvccKey::NextVersion.encode(), bincode::serialize(&(next_version + 1))?)?;

        // 3. Get the current snapshot
        let active_versions = Self::scan_txnactive(&mut engine)?;

        // 4. Add current transaction into snapshot
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        // 5. Return the MvccTransaction
        Ok(
            Self {
                engine: eng.clone(),
                state: TransactionState {
                    version: next_version,
                    active_versions,
                }
             }
        )
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut eng = self.engine.lock()?;
        eng.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult {key, value });
        }
        Ok(results)
    }

    // -+------------------------+-
    //      Auxilliary Part
    // -+------------------------+- 

    // Update/Delete data
    fn while_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // Obtain the storage engine
        let mut engine = self.engine.lock()?;

        // Check the conflicts
        // 

        Ok(())
    }

    // Scan current active transactions
    fn scan_txnactive(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                },
                _ => {
                    return Err(Error::Internal(format!("Unexcept key: {:?}", String::from_utf8(key))))
                }
            }
        }
        Ok(active_versions)
    }

}

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

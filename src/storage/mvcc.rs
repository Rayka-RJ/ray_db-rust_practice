use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex, MutexGuard}, u64};
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

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false;
        } else {
            return version <= self.version;
        }
    }
}

pub type Version = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(
        Version, 
        #[serde(with = "serde_bytes")]        
        Vec<u8>),
    Version(
        #[serde(with = "serde_bytes")]
        Vec<u8>, 
        Version
    ),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        Vec<u8>
    )
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

    // Txn Commit
    pub fn commit(&self) -> Result<()> {
        // Get the storage engine
        let mut engine = self.engine.lock()?;
        
        let mut delete_keys = Vec::new();

        // Get the current TxnWrite
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()?{
            delete_keys.push(key);
        }
        // Release the RefCall borrow
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // Delete from active_txn
        engine.delete(MvccKey::TxnActive(self.state.version).encode())?;
        Ok(())
    }

    // Txn Rollback
    pub fn rollback(&self) -> Result<()> {
        // Obtain engine
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();

        // Find the current TxnWrite info
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode());
                }
                _ => {
                    return Err(Error::Internal(format!("Unexpected key: {:?}", String::from_utf8(key))))
                }
            }
            delete_keys.push(key);
        }

        drop(iter);

        // Delete from active txn
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        engine.delete(MvccKey::TxnActive(self.state.version).encode())?;

        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // Get the storage engine
        let mut engine = self.engine.lock()?;

        // Version: 9
        // Scan range: 0 - 9
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = engine.scan(from..=to).rev();

        // Start from latest, find the latest visible
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!("Unexpected key: {:?}", String::from_utf8(key))));
                }
            }
        }
        Ok(None)
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
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // Obtain the storage engine
        let mut engine = self.engine.lock()?;

        // Check the conflicts
        // 3 4 5 
        // 6
        // key1-3 key2-4 key3-5
        let from = MvccKey::Version(key.clone(), self.state.active_versions.iter().min().copied().unwrap_or(self.state.version + 1)).encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();

        // Current actice: 3 4 5 
        // Current txn 6
        // Only the last version 
        // 1. Key follows the sequence from small to large
        // 2. If there is a new txn changing the key, such as txn 10, then update by txn 6 is conflict
        // 3. If the current actice txn 4 updated the key, then txn after like txn 5 is unable to update the key
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // Check if the version visable
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!("Unexpected key: {:?}", String::from_utf8(k))))
                }
            }
        } 

        // Record all the key written in by the version, for rollback
        engine.set(MvccKey::TxnWrite(self.state.version, key.clone()).encode(), vec![])?;

        // Write in actual (key, value)
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode(), bincode::serialize(&value)?)?;
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

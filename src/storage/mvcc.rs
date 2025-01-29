use std::{collections::{BTreeMap, HashSet}, sync::{Arc, Mutex, MutexGuard}, u64};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use super::{engine::Engine, keycode::{deserialize_key,serialize_key}};

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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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
    ),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

// -+------------------------------------+-
//      NextVersion 0
//      TxnActive 1-100 1-101 1-102
//      Version key1-101 key2-101
// -+------------------------------------+-

impl MvccKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
    }
}

impl<E: Engine> MvccTransaction<E> {

    // Begin a transaction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // 0. Get the storage engine
        let mut engine = eng.lock()?;

        // 1. Get the newest version
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)?  {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };

        // 2. Save the next version
        engine.set(MvccKey::NextVersion.encode()?, bincode::serialize(&(next_version + 1))?)?;

        // 3. Get the current snapshot
        let active_versions = Self::scan_txnactive(&mut engine)?;

        // 4. Add current transaction into snapshot
        engine.set(MvccKey::TxnActive(next_version).encode()?, vec![])?;

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
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()?{
            delete_keys.push(key);
        }
        // Release the RefCall borrow
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // Delete from active_txn
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)?;
        Ok(())
    }

    // Txn Rollback
    pub fn rollback(&self) -> Result<()> {
        // Obtain engine
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();

        // Find the current TxnWrite info
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
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

        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)?;

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
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
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

        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        
        // Original        Encode
        // 97 98 99     -> 97 98 99 0 0
        // Prefix          Encode
        // 97 98        -> 97 98 0 0    -> 97 98
        // Remove the [0, 0] end

        enc_prefix.truncate(enc_prefix.len() - 2); 

        let mut iter = eng.scan_prefix(enc_prefix);
        let mut results = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::deserialize(&value)? {
                            Some(raw_value) => results.insert(raw_key, raw_value),
                            None => results.remove(&raw_key),
                        };
                    }
                }
                _ => {
                    return Err(Error::Internal(format!("Unexpected key {:?}", String::from_utf8(key))))
                }
            }
        }

        Ok(results
            .into_iter()
            .map(|(key, value)| ScanResult {key, value})
            .collect())
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
        let from = MvccKey::Version(key.clone(), self.state.active_versions.iter().min().copied().unwrap_or(self.state.version + 1)).encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;

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
        engine.set(MvccKey::TxnWrite(self.state.version, key.clone()).encode()?, vec![])?;

        // Write in actual (key, value)
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode()?, bincode::serialize(&value)?)?;
        Ok(())
    }

    // Scan current active transactions
    fn scan_txnactive(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode()?);
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

#[cfg(test)]

mod tests {

    use crate::{
        error::Result, 
        storage::{disk::DiskEngine, engine::Engine, memory::MemoryEngine}
    };

    use super::{Mvcc, MvccKey};

    // 1. Get
    fn get(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"value1".to_vec())?;
        tx.set(b"key2".to_vec(), b"value2".to_vec())?;
        tx.set(b"key3".to_vec(), b"value3".to_vec())?;
        tx.set(b"key4".to_vec(), b"value4".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.commit()?;     

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"value1".to_vec()));
        assert_eq!(tx1.get(b"key2".to_vec())?, Some(b"value2".to_vec()));
        assert_eq!(tx1.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        get(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        get(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }


    // 2. Get Isolation
    fn get_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"a".to_vec(), b"[1,2,3]".to_vec())?;
        tx.set(b"b".to_vec(), b"[2,3,4]".to_vec())?;
        tx.set(b"c".to_vec(), b"[3,4,5]".to_vec())?;        
        tx.set(b"d".to_vec(), b"[4,5,6]".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"a".to_vec(), b"[0,1,2]".to_vec())?;
        // without commit, update fail, lock is held.

        let tx2 = mvcc.begin()?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"b".to_vec(), b"[7,8,9]".to_vec())?;
        tx3.delete(b"c".to_vec())?;
        tx3.commit()?;

        let tx4 = mvcc.begin()?;

        assert_eq!(tx2.get(b"a".to_vec())?, Some(b"[1,2,3]".to_vec()));
        assert_eq!(tx2.get(b"b".to_vec())?, Some(b"[2,3,4]".to_vec()));
        assert_eq!(tx2.get(b"c".to_vec())?, Some(b"[3,4,5]".to_vec()));
        assert_eq!(tx2.get(b"d".to_vec())?, Some(b"[4,5,6]".to_vec()));

        assert_eq!(tx4.get(b"a".to_vec())?, Some(b"[1,2,3]".to_vec()));
        assert_eq!(tx4.get(b"b".to_vec())?, Some(b"[7,8,9]".to_vec()));
        assert_eq!(tx4.get(b"c".to_vec())?, None);
        assert_eq!(tx4.get(b"d".to_vec())?, Some(b"[4,5,6]".to_vec()));
        Ok(())

// 事务隔离机制
// tx1 尝试更新 a，但未提交，因此变更未对其他事务生效，且持有锁。
// 其他事务无法修改 a。

// tx2 和 tx4 展示了隔离级别：
// tx2 读取到事务 tx 提交后的数据。
// tx4 读取到事务 tx3 提交后的数据，展示了最新的已提交状态。

// 通过 MVCC，系统维护多个版本的数据，确保读取和写入互不干扰。

    }

    #[test]
    fn test_get_isolation() -> Result<()> {
        get_isolation(MemoryEngine::new())?;
        
        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        get_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?; 
        Ok(())
    }


    // 3. Scan Prefix
    fn scan_prefix(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![super::ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );
        
        let iter1 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"bcca".to_vec(),
                    value: b"val6".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter3,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                super::ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
                ]
        );

        Ok(())
    }

    #[test]
    fn test_scan_prefix() -> Result<()> {
        scan_prefix(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        scan_prefix(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;

        Ok(())
    }

    // 4. Scan Isolation
    fn scan_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx2.set(b"acca".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"aabb".to_vec(), b"val1-1".to_vec())?;
        // not yet commit

        let tx3 = mvcc.begin()?;
        tx3.set(b"bbaa".to_vec(), b"val3-1".to_vec())?;  
        tx3.delete(b"bcca".to_vec())?;
        tx3.commit()?;

        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key:b"aabb".to_vec(),
                    value: b"val1".to_vec(),
                },
                super::ScanResult {
                    key:b"aaca".to_vec(),
                    value: b"val5".to_vec(),
                },                
            ]
        );

        Ok(())
    }

    #[test]
    fn test_scan_isolation() -> Result<()> {
        scan_isolation(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        scan_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 5. Set
    fn set(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"value1".to_vec())?;
        tx.set(b"key2".to_vec(), b"value2".to_vec())?;
        tx.set(b"key3".to_vec(), b"value3".to_vec())?;
        tx.set(b"key4".to_vec(), b"value4".to_vec())?;
        tx.set(b"key5".to_vec(), b"value5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-1".to_vec())?;

        tx2.set(b"key3".to_vec(), b"haha3".to_vec())?;
        tx2.set(b"key5".to_vec(), b"val5-1".to_vec())?;

        tx1.commit()?;
        tx2.commit()?;

        // visible to previous txn number
        let tx=mvcc.begin()?;
        assert_eq!(tx.get(b"key1".to_vec())?,Some(b"val1-1".to_vec()));
        assert_eq!(tx.get(b"key2".to_vec())?,Some(b"val3-1".to_vec()));
        assert_eq!(tx.get(b"key3".to_vec())?,Some(b"haha3".to_vec()));
        assert_eq!(tx.get(b"key5".to_vec())?,Some(b"val5-1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        set(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 6. Set Conflict
    fn set_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.set(b"key4".to_vec(), b"val4".to_vec())?;
        tx.set(b"key5".to_vec(), b"val5".to_vec())?;
        tx.commit()?;        

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(
            tx2.set(b"key1".to_vec(), b"val1-3".to_vec()),
            Err(super::Error::WriteConflict)
        );

        let tx3 = mvcc.begin()?;
        tx3.set(b"key5".to_vec(), b"val5-2".to_vec())?;
        tx3.commit()?;

        assert_eq!(
            tx1.set(b"key5".to_vec(), b"val5-3".to_vec()),
            Err(super::Error::WriteConflict)
        );

        tx1.commit()?;
        Ok(())
    }


// tx1 的写锁

// 当 tx1 对 key1 执行 set 时，它会获取该键的写锁（或者记录它的意图写操作）。
// 由于 tx1 尚未提交，因此其他事务（如 tx2）对 key1 的任何写操作都会发生冲突（WriteConflict）。

// tx3 在 key5 上设置了新值，并提交后，这些变更对后续事务（如新的 tx4）可见。
// 然而，tx1 是在 tx3 提交之前创建的事务，其事务视图（事务快照）只包含 tx3 提交之前的状态。
// 如果 tx1 尝试修改 key5，它必须确保当前的值（val5-2）仍然与其事务视图一致。

// 当 tx1 试图更新 key5 时，key5 的值已经被 tx3 修改为 val5-2。
// 事务隔离机制会检测到 tx1 的事务视图中 key5 的值和当前存储引擎中的值不一致，从而导致 WriteConflict。

    #[test]
    fn test_set_conflict() -> Result<()> {
        set_conflict(MemoryEngine::new())?;
        
        let p = tempfile::tempdir()?.into_path().join("raydb-log");
        set_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 7. delete
    fn delete(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.delete(b"key2".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key2".to_vec())?, None);

        let iter = tx1.scan_prefix(b"ke".to_vec())?;
        assert_eq!(
            iter,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3-1".to_vec()
                },               
            ]
        );

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        delete(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("ray-db");
        delete(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }


    // 8. delete confilct
    fn delete_conflict(eng:impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx1.delete(b"key1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;

        assert_eq!(
            tx2.delete(b"key1".to_vec()),
            Err(super::Error::WriteConflict)
        );

        assert_eq!(
            tx2.delete(b"key2".to_vec()),
            Err(super::Error::WriteConflict)
        );
        Ok(())
    }

    #[test]
    fn test_delete_conflict() -> Result<()> {
        delete_conflict(MemoryEngine::new())?;
        
        let p = tempfile::tempdir()?.into_path().join("ray-db");
        delete_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }


    // 9. dirty-read
    fn dirty_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?; 
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?; // tx2 修改 key1 -> val1-1，但未提交
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec())); // 验证 tx1 读取到的 key1 的值仍是 val1      
        Ok(())
    }

    #[test]
    fn test_dirty_read() -> Result<()> {
        dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("ray-db");
        dirty_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }


    // 10. Unrepeatable read
    fn unrepeatable_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?; 
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;         
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        tx2.commit()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        
        Ok(())
    }

    #[test]
    fn test_unrepeatable_read() -> Result<()> {
        unrepeatable_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("ray-db");
        unrepeatable_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

// 不可重复读 (Unrepeatable Read)： 不可重复读是指在同一事务中，连续两次读取同一数据的值不一致。这通常发生在一个事务正在读取某条记录，而另一个事务修改并提交了该记录的情况下。
// 验证不可重复读：
// 在 tx1 中第一次读取 key1 得到 val1。
// 在 tx2 提交 key1 的新值 val1-1 后，tx1 再次读取 key1 的值。
// 如果第二次读取到的值不是 val1，说明系统没有阻止不可重复读。

    // 11. Phantom read
    fn phantom_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?; 
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec(),
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec(),
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec(),
                },                
            ]
        );

        tx2.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val4".to_vec())?;
        tx2.commit()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec(),
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec(),
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec(),
                },                
            ]
        );        

        Ok(())
    }
    #[test]
    fn test_phantom_read() -> Result<()> {
        phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("ray-db");
        phantom_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    } 

// 幻读 (Phantom Read)：

// 幻读是一种事务隔离问题，指一个事务两次执行同一查询，但第二次查询返回了第一次未包含的记录（例如新插入的数据）。
// 在这里，tx1 的查询应该保持一致性，不能受到 tx2 插入或修改记录的影响。


    // 12. rollback
    fn rollback(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?; 
        tx.commit()?;

        let tx1 = mvcc.begin()?;   
        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx1.set(b"key3".to_vec(), b"val3-1".to_vec())?;   
        tx1.rollback()?;

        let tx2 = mvcc.begin()?;
        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));        
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val2".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, Some(b"val3".to_vec()));
        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()> {
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("ray-db");
        rollback(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    } 
}
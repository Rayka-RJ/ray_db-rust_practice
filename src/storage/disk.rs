use std::{collections::{btree_map, BTreeMap}, fs::{File, OpenOptions}, io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::PathBuf};
use fs4::fs_std::FileExt;

use crate::error::Result;
use super::engine::{Engine, EngineIterator};


pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
const LOG_HEADER_SIZE: u32 = 8;
pub struct DiskEngine {
    keydir:KeyDir,
    log: Log, 
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
       let mut log =  Log::new(file_path)?;
       let keydir = log.build_keydir()?;
       Ok(Self { keydir, log })
    }

    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    fn compact(&mut self) -> Result<()> {
        // Create a temporary log 
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");
        let mut new_log = Log::new(new_path)?;
        let mut new_keydir = KeyDir::new();

        // Re-Write
        for (key, (offset, val_size)) in self.keydir.iter() {
            // Read value
            let value = self.log.read_value(*offset, *val_size)?;
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;

            new_keydir.insert(key.clone(), (new_offset + new_size as u64 - *val_size as u64, *val_size));
        }

        // Replace with temporary file 
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;

        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;

        Ok(())
    }
}

impl Engine for DiskEngine {

    type EngineIterator<'a> = DiskEngineIterator<'a>;
    
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Write log
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // Renew the memory index
        let val_size = value.len() as u32;
        self.keydir.insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }
    
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, val_size)) => {
                let val = self.log.read_value(*offset, *val_size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }
    
    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }
    
    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        DiskEngineIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }


}

pub struct DiskEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (k, (offset, val_size)) = item;
        let value = self.log.read_value(*offset, *val_size)?;
        Ok((k.clone(), value))
    }
}

impl<'a> EngineIterator for DiskEngineIterator<'a> {}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item|self.map(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

struct Log {
    file_path: PathBuf,
    file: File,
}

impl Log {

    fn new(file_path: PathBuf) -> Result<Self> {
        // If file path not exists
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }

        // Open the file
        let file = OpenOptions::new()
            .create(true).read(true)
            .write(true).open(&file_path)?;

        // Add a file lock; The file is limited to only one transaction 
        file.try_lock_exclusive()?;

        Ok(Self {file, file_path})
    }

    // traverse the data file, construct the memory index
    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();
        let file_size = self.file.metadata()?.len();
        let mut reader = BufReader::new(&self.file);

        let mut offset = 0;
        loop {
            if offset >= file_size {
                break;
            }
            let (key, val_size) = Self::read_entry(&mut reader, offset)?;
            let key_size = key.len() as u32;
            if val_size == -1 {
                keydir.remove(&key);
                offset += key_size as u64 + LOG_HEADER_SIZE as u64;
            } else {
                keydir.insert(key, 
                    (offset + LOG_HEADER_SIZE as u64 + key_size as u64, 
                        val_size as u32));
                offset += key_size as u64 + val_size as u64 + LOG_HEADER_SIZE as u64;
            }
        }

        Ok(keydir)
    }

    // +-------------+-------------+----------------+----------------+
    // | key len(4)    val len(4)     key(varint)       val(varint)  |
    // +-------------+-------------+----------------+----------------+
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // Point to the end of log file
        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        // Write in
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len() as u32 );
        let total_length = key_size + val_size + LOG_HEADER_SIZE;

        // Write in key size, value size, key and value
        let mut writer = BufWriter::with_capacity(total_length as usize, &self.file);

        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v|v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;

        Ok((offset, total_length))
    }  

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    } 

    fn read_entry(reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        reader.seek(SeekFrom::Start(offset))?;
        let mut buf = [0; 4];

        // Read key size
        reader.read_exact(&mut buf)?;
        let key_size = u32::from_be_bytes(buf);

        // Read value size
        reader.read_exact(&mut buf)?;
        let val_size = i32::from_be_bytes(buf);

        // Read key
        let mut key = vec![0; key_size as usize];
        reader.read_exact(&mut key)?;

        Ok((key, val_size))       
    }
}

#[cfg(test)]

mod tests {
    use std::path::PathBuf;
    use crate::{error::Result, storage::engine::Engine};
    use super::DiskEngine;

    #[test]
    fn test_disk_engine_start() -> Result<()> {
        let eng = DiskEngine::new_compact(PathBuf::from("/tmp/raydb-log"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/db/db-log"))?;

        eng.set(b"key1".to_vec(), b"value1".to_vec())?;
        eng.set(b"key2".to_vec(), b"value2".to_vec())?;
        eng.set(b"key3".to_vec(), b"value3".to_vec())?;     
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;   

        // Rewrite
        eng.set(b"key4".to_vec(), b"value4".to_vec())?;  
        eng.set(b"key4".to_vec(), b"value4.5".to_vec())?;          
        eng.set(b"key5".to_vec(), b"value5".to_vec())?;  
        eng.set(b"key5".to_vec(), b"value5.5".to_vec())?;           
        eng.set(b"key4".to_vec(), b"value4.8".to_vec())?;          
        
        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"key3".to_vec(), b"value3".to_vec()),
                (b"key4".to_vec(), b"value4.8".to_vec()),
                (b"key5".to_vec(), b"value5.5".to_vec()),
            ]
        );

        drop(eng); // release the lifetime

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("/tmp/db/db-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"key3".to_vec(), b"value3".to_vec()),
                (b"key4".to_vec(), b"value4.8".to_vec()),
                (b"key5".to_vec(), b"value5.5".to_vec()),                
            ]
        );

        drop(eng2);
        std::fs::remove_dir_all(PathBuf::from("/tmp/db"))?;

        Ok(())
    }
}
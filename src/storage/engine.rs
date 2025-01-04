use std::ops::{Bound, RangeBounds};
use crate::error::Result;

// Abstract engine interface, can be accessed by different engines
// Now it supports memory-based and disk-based

pub trait Engine {

    type EngineIterator<'a>: EngineIterator where Self:'a;

    // Set the Key/Value
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // Get the value by key
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;   

    // Delete the value by key; Ignore if not exists
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    // Scan the value
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    // Scan with the prefix
    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        // start: aaaa
        // end: aaab
        // Only ascii(0-127) 

        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last += 1;
        };
        let end = Bound::Excluded(bound_prefix);

        self.scan((start, end))
    }

}

pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::{error::Result, storage::memory::MemoryEngine};
    use std::{ops::Bound};

    // Point query
    fn test_point_opt(mut eng: impl Engine) -> Result<()> {
        // 1. unknown key
        assert_eq!(eng.get(b"not exist".to_vec())?, None);

        // 2. obtain a known key
        eng.set(b"a".to_vec(), vec![1,2,3,4])?;
        assert_eq!(eng.get(b"a".to_vec())?, Some(vec![1,2,3,4]));

        // 3. Cover an existing key
        eng.set(b"a".to_vec(), vec![5,6,7])?;
        assert_eq!(eng.get(b"a".to_vec())?, Some(vec![5,6,7]));

        // 4. Get after deletion
        eng.delete(b"a".to_vec())?;
        assert_eq!(eng.get(b"a".to_vec())?, None);

        Ok(())
    }

    // Scan 
    fn test_scan(mut eng: impl Engine) -> Result<()> {
        eng.set(b"bb".to_vec(), b"value3".to_vec());
        eng.set(b"ba".to_vec(), b"value4".to_vec());     
        eng.set(b"aa".to_vec(), b"value1".to_vec());
        eng.set(b"ab".to_vec(), b"value2".to_vec());   
        eng.set(b"cc".to_vec(), b"value5".to_vec());  

        let start = Bound::Included(b"a".to_vec());
        let end = Bound::Excluded(b"b".to_vec());

        let mut iter = eng.scan((start, end));
        let (key1, _) = iter.next().expect("no value founded")?;
        assert_eq!(key1, b"aa".to_vec());

        let (key2, _) = iter.next().expect("No value founded")?;
        assert_eq!(key2, b"ab".to_vec());
        drop(iter);

        let start = Bound::Included(b"b".to_vec());
        let end = Bound::Excluded(b"z".to_vec());
        let mut iter2 = eng.scan((start, end));

        let (key3, _) = iter2.next_back().expect("No value founded")?;
        assert_eq!(key3, b"cc".to_vec());

        let (key4, _) = iter2.next_back().expect("No value founded")?;
        assert_eq!(key4, b"bb".to_vec());

        Ok(())
    }

    // Prefix Scan
    fn test_prefix_scan(mut eng: impl Engine) -> Result<()> {
        eng.set(b"aaabbb".to_vec(), b"value1".to_vec())?;
        eng.set(b"cccddd".to_vec(), b"value2".to_vec())?;
        eng.set(b"eeefff".to_vec(), b"value3".to_vec())?;
        eng.set(b"cccfff".to_vec(), b"value4".to_vec())?;
        eng.set(b"bbbccc".to_vec(), b"value5".to_vec())?;
        eng.set(b"aaaccc".to_vec(), b"value6".to_vec())?;
                
        let prefix = b"ccc".to_vec();
        let mut iter = eng.scan_prefix(prefix);
        let (key1, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key1, b"cccddd".to_vec());

        let (key2, _) = iter.next().transpose()?.unwrap();
        assert_eq!(key2, b"cccfff".to_vec());
        Ok(())
    }
    // Memory Engine
    #[test]
    fn test_memory_engine() -> Result<()> {
        test_point_opt(MemoryEngine::new())?;
        test_scan(MemoryEngine::new())?;
        test_prefix_scan(MemoryEngine::new())?;
        Ok(())
    } 
}
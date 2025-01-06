use serde::ser;
use crate::error::{Result, Error};

pub fn serialize<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}

pub struct Serializer {
    output: Vec<u8>,
}

impl <'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleVariant = Self;

    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        todo!()
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        todo!()
    }

    fn serialize_char(self, v: char) -> Result<()> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        todo!()
    }

    // Original value        serialized
    // 97 98 99           -> 97 98 99 0 0
    // 97 98 0 99         -> 97 98 0 255 9 0 0
    // 97 98 0 0 99       -> 97 98 0 255 0 255 99 0 0
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let mut res = Vec::new();
        for e in v.into_iter() {
            match e {
                0 => res.extend([0, 255]),
                b => res.push(*b),
            }
        }
        // End with two 0
        res.extend([0, 0]);

        self.output.extend(res);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        todo!()
    }

    // similar to MvccKey::NextVersion
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.output.extend(u8::try_from(variant_index));
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + ser::Serialize {
        todo!()
    }

    // similar to TxnActive(Version)
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self);
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    // Similar to TxnWrite(Version, Vec<u8>)
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
    
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;
    
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + ser::Serialize, 
        {
            value.serialize(&mut **self)
        }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
        where
            T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]

mod tests {
    use super::serialize;

    use crate::storage::mvcc::{MvccKey, MvccKeyPrefix};

    #[test]
    fn test_encode() {
        let k = MvccKey::NextVersion;
        let v = serialize(&k).unwrap();
        println!("{:?}", v);

        let k = MvccKey::Version(b"abc".to_vec(), 11);
        let v = serialize(&k).unwrap();
        println!("{:?}", v);
    }

    #[test]
    fn test_encode_2() {
        let ser_cmp = |k : MvccKey, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            assert_eq!(res, v)
        };

        // 0,1,2,3 represents the sequence of MvccKey enum variants
        ser_cmp(MvccKey::NextVersion, vec![0]);
        ser_cmp(MvccKey::TxnActive(1), vec![1,0,0,0,0,0,0,0,1]);
        ser_cmp(MvccKey::TxnWrite(1, vec![1,2,3]), vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0]);
        ser_cmp(MvccKey::Version(b"abc".to_vec(), 11), vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11]);
    }

    #[test]
    fn test_encode_prefix() {
        let ser_cmp = |k : MvccKeyPrefix, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            assert_eq!(res, v)
        };
        ser_cmp(MvccKeyPrefix::NextVersion, vec![0]);
        ser_cmp(MvccKeyPrefix::TxnActive, vec![1]);
        ser_cmp(MvccKeyPrefix::TxnWrite(1), vec![2, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(MvccKeyPrefix::Version(b"ab".to_vec()), vec![3, 97, 98, 0, 0]); // without value
    }
}
use serde::{de::{self, IntoDeserializer}, ser};
use crate::error::{Error, Result};

pub fn serialize_key<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
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


// Deserializer

pub fn deserialize_key<'d, T: serde::Deserialize<'d>>(input: &'d [u8]) -> Result<T> {
    let mut der = Deserializer { input };
    T::deserialize(&mut der)
}

pub struct Deserializer<'d> {
    input: &'d [u8],
}


impl<'d> Deserializer<'d>  {
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }

    // - if 255 after0, it is 0 in original string
    // - if 0 after 0, it is end of string
    fn next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let i = loop {
            match iter.next() {
                Some((_, 0)) => match iter.next() {
                    Some((i, 0)) => break i + 1,
                    Some((i, 255)) => res.push(0),
                    _ => return Err(Error::Internal("Unexpected input".into())),
                },
                Some((_, b)) => res.push(*b), // Non zero value
                _ => return Err(Error::Internal("Unexpected input".into())),
            }
        };
        self.input = &self.input[i..];
        Ok(res)
    }
}

impl<'a, 'd> de::Deserializer<'d> for &'a mut Deserializer<'d> {

    type Error = Error;
    
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    // &[u8] -> Vec<u8>
    // From TryFrom
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        let bytes = self.take_bytes(8); // u64 for 8 bytes
        
        let v = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(v)
    }
    
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_bytes(&self.next_bytes()?)
    }
    
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_byte_buf(self.next_bytes()?)
    }
    
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_seq(self)
    }
    
    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_seq(self)
    }
    
    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_enum(self)
    }
    
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
    
}

impl <'d, 'a> de::SeqAccess<'d> for Deserializer<'d> {
    type Error=Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'d> {
        seed.deserialize(self).map(Some)
    }
}

impl<'d, 'a> de::EnumAccess<'d> for &mut Deserializer<'d> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'d> {
        let index = self.take_bytes(1)[0] as u32;
        let variant_index: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((variant_index?, self))
    }
}

impl<'d, 'a> de::VariantAccess<'d> for &mut Deserializer<'d> {
    type Error = Error;
    
    fn unit_variant(self) -> Result<()> {
        Ok(())
    }
    
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'d> {
        seed.deserialize(&mut *self)
    }
    
    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        visitor.visit_seq(self)
    }
    
    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'d> {
        todo!()
    }
}

#[cfg(test)]

mod tests {
    use super::{serialize_key, deserialize_key};

    use crate::storage::mvcc::{MvccKey, MvccKeyPrefix};

    #[test]
    fn test_encode() {
        let k = MvccKey::NextVersion;
        let v = serialize_key(&k).unwrap();
        println!("{:?}", v);

        let k = MvccKey::Version(b"abc".to_vec(), 11);
        let v = serialize_key(&k).unwrap();
        println!("{:?}", v);
    }

    #[test]
    fn test_encode_2() {
        let ser_cmp = |k : MvccKey, v: Vec<u8>| {
            let res = serialize_key(&k).unwrap();
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
            let res = serialize_key(&k).unwrap();
            assert_eq!(res, v)
        };
        ser_cmp(MvccKeyPrefix::NextVersion, vec![0]);
        ser_cmp(MvccKeyPrefix::TxnActive, vec![1]);
        ser_cmp(MvccKeyPrefix::TxnWrite(1), vec![2, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(MvccKeyPrefix::Version(b"ab".to_vec()), vec![3, 97, 98, 0, 0]); // without value
    }

    #[test]
    fn test_u8_convert() {
        let v = [1 as u8, 2, 3];
        let vv = &v;
        let vvv: Vec<u8> = vv.try_into().unwrap();
        println!("{:?}", vvv)
    }

    #[test]
    fn test_decode() {
        let der_cmp = |k: MvccKey, v: Vec<u8>| {
            let res: MvccKey = deserialize_key(&v).unwrap();
            assert_eq!(res, k);
        };

        der_cmp(MvccKey::NextVersion, vec![0]);
        der_cmp(MvccKey::TxnActive(1), vec![1,0,0,0,0,0,0,0,1]);
        der_cmp(MvccKey::TxnWrite(1, vec![1,2,3]), vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0]);
        der_cmp(MvccKey::Version(b"abc".to_vec(), 11), vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11]);        
    }
}
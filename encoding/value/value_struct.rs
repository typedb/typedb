/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::{Formatter, Write},
    ops::{Deref, Range},
    sync::Arc,
};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;
use lending_iterator::LendingIterator;
use primitive::either::Either;
use resource::constants::{
    encoding,
    encoding::StructFieldIDUInt,
    snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE},
};
use serde::{
    de,
    de::{EnumAccess, SeqAccess, Unexpected, VariantAccess, Visitor},
    ser::SerializeSeq,
    Deserialize, Deserializer, Serialize, Serializer,
};
use storage::{
    key_range::KeyRange,
    key_value::StorageKey,
    snapshot::{iterator::SnapshotIteratorError, ReadableSnapshot, WritableSnapshot},
};

use crate::{
    error::EncodingError,
    graph::{
        common::value_hasher::DisambiguatingHashedID,
        definition::{
            definition_key::DefinitionKey,
            r#struct::{StructDefinition, StructDefinitionField},
        },
        thing::{
            vertex_attribute::{AttributeID, AttributeIDLength, AttributeVertex, StructAttributeID},
            vertex_generator::ThingVertexGenerator,
        },
        type_::vertex::{TypeID, TypeVertex},
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    value::{
        boolean_bytes::BooleanBytes,
        date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes,
        decimal_bytes::DecimalBytes,
        decimal_value::Decimal,
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        duration_value::Duration,
        long_bytes::LongBytes,
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        value_type::{ValueType, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue<'a> {
    // Tempting to make it all static
    Boolean(bool),
    Long(i64),
    Double(f64),
    Decimal(Decimal),
    DateTime(NaiveDateTime),
    DateTimeTZ(DateTime<Tz>),
    Duration(Duration),
    String(Cow<'a, str>),
    Struct(Cow<'a, StructValue<'static>>),
}

impl<'a> FieldValue<'a> {
    fn value_type(&self) -> ValueType {
        match self {
            FieldValue::Boolean(_) => ValueType::Boolean,
            FieldValue::Long(_) => ValueType::Long,
            FieldValue::Double(_) => ValueType::Double,
            FieldValue::Decimal(_) => ValueType::Decimal,
            FieldValue::DateTime(_) => ValueType::DateTime,
            FieldValue::DateTimeTZ(_) => ValueType::DateTimeTZ,
            FieldValue::Duration(_) => ValueType::Duration,
            FieldValue::String(_) => ValueType::String,
            FieldValue::Struct(struct_value) => ValueType::Struct(struct_value.definition_key.clone()),
        }
    }
}

// TODO: There's a strong case to handroll encoding of structs and store them as just bytes in memory.
// And throw in some accessor logic so we can efficiently access & deserialise just the fields we need on demand.
#[derive(Debug, Clone, PartialEq)]
pub struct StructValue<'a> {
    definition_key: DefinitionKey<'a>,
    // a map allows empty fields to not be recorded at all
    fields: HashMap<StructFieldIDUInt, FieldValue<'a>>,
}

impl<'a> StructValue<'a> {
    pub fn new(
        definition_key: DefinitionKey<'a>,
        fields: HashMap<StructFieldIDUInt, FieldValue<'a>>,
    ) -> StructValue<'a> {
        StructValue { definition_key, fields }
    }

    pub fn try_translate_fields(
        definition_key: DefinitionKey<'a>,
        struct_definition: StructDefinition,
        value: HashMap<String, FieldValue<'a>>,
    ) -> Result<StructValue<'a>, Vec<EncodingError>> {
        let mut fields: HashMap<StructFieldIDUInt, FieldValue<'a>> = HashMap::new();
        let mut errors: Vec<EncodingError> = Vec::new();
        for (field_name, field_id) in struct_definition.field_names {
            let field_definition: &StructDefinitionField = &struct_definition.fields.get(field_id as usize).unwrap();
            if let Some(value) = value.get(&field_name) {
                if field_definition.value_type == value.value_type() {
                    fields.insert(field_id, value.clone());
                } else {
                    errors.push(EncodingError::StructFieldValueTypeMismatch {
                        field_name,
                        expected: field_definition.value_type.clone(),
                    })
                }
            } else if !field_definition.optional {
                errors.push(EncodingError::StructMissingRequiredField { field_name })
            }
        }

        if errors.is_empty() {
            Ok(StructValue { definition_key, fields })
        } else {
            Err(errors)
        }
    }

    pub fn definition_key(&self) -> &DefinitionKey<'a> {
        &self.definition_key
    }

    pub fn fields(&self) -> &HashMap<StructFieldIDUInt, FieldValue<'a>> {
        &self.fields
    }

    // Deeply nested structs may take up a lot of space with the u16 path.
    pub fn create_index_entries(
        &self,
        snapshot: &impl WritableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        attribute: &AttributeVertex<'_>,
    ) -> Result<Vec<StructIndexEntry>, Arc<SnapshotIteratorError>> {
        let mut acc: Vec<StructIndexEntry> = Vec::new();
        let mut path: Vec<StructFieldIDUInt> = Vec::new();
        Self::create_index_entries_recursively(snapshot, hasher, attribute, &self.fields, &mut path, &mut acc)?;
        Ok(acc)
    }

    fn create_index_entries_recursively<'b>(
        snapshot: &impl WritableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        attribute: &AttributeVertex<'b>,
        fields: &HashMap<StructFieldIDUInt, FieldValue<'a>>,
        path: &mut Vec<StructFieldIDUInt>,
        acc: &mut Vec<StructIndexEntry<'static>>,
    ) -> Result<(), Arc<SnapshotIteratorError>> {
        for (idx, value) in fields.iter() {
            if let FieldValue::Struct(struct_val) = value {
                path.push(*idx);
                Self::create_index_entries_recursively(
                    snapshot,
                    hasher.clone(),
                    attribute,
                    struct_val.fields(),
                    path,
                    acc,
                )?;
                let popped = path.pop().unwrap();
                debug_assert_eq!(*idx, popped);
            } else {
                acc.push(StructIndexEntry::build(snapshot, hasher.clone(), path, value, attribute)?);
            }
        }
        Ok(())
    }
}

impl<'a> Serialize for FieldValue<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldValue::Boolean(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeBoolean.prefix_id().bytes())?;
                seq.serialize_element(&BooleanBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::Long(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeLong.prefix_id().bytes())?;
                seq.serialize_element(&LongBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::Double(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDouble.prefix_id().bytes())?;
                seq.serialize_element(&DoubleBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::Decimal(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDecimal.prefix_id().bytes())?;
                seq.serialize_element(&DecimalBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::DateTime(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDateTime.prefix_id().bytes())?;
                seq.serialize_element(&DateTimeBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::DateTimeTZ(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDateTimeTZ.prefix_id().bytes())?;
                seq.serialize_element(&DateTimeTZBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::Duration(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDuration.prefix_id().bytes())?;
                seq.serialize_element(&DurationBytes::build(*value).bytes())?;
                seq.end()
            }
            FieldValue::String(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeString.prefix_id().bytes())?;
                seq.serialize_element(&StringBytes::<BUFFER_VALUE_INLINE>::build_ref(value).bytes().bytes())?;
                seq.end()
            }
            FieldValue::Struct(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeStruct.prefix_id().bytes()[0])?;
                seq.serialize_element(value)?;
                seq.end()
            }
        }
    }
}

pub struct StructIndexEntry<'a> {
    key_bytes: Bytes<'a, BUFFER_KEY_INLINE>,
    value_bytes: Option<Bytes<'a, BUFFER_VALUE_INLINE>>,
}

impl<'a> StructIndexEntry<'a> {
    pub fn new(key_bytes: Bytes<'a, BUFFER_KEY_INLINE>, value_bytes: Option<Bytes<'a, BUFFER_VALUE_INLINE>>) -> Self {
        Self { key_bytes, value_bytes }
    }

    pub fn attribute_vertex(&self) -> AttributeVertex<'static> {
        let bytes = self.key_bytes.bytes();
        let attribute_type_id = TypeID::new(bytes[StructIndexEntry::ENCODING_TYPEID_RANGE].try_into().unwrap());
        let attribute_id =
            AttributeID::new(ValueTypeCategory::Struct, &bytes[(bytes.len() - StructAttributeID::LENGTH)..bytes.len()]);
        AttributeVertex::build(ValueTypeCategory::Struct, attribute_type_id, attribute_id)
    }

    pub fn value_bytes(&self) -> &Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        &self.value_bytes
    }
}

impl StructIndexEntry<'static> {
    const PREFIX: Prefix = Prefix::IndexValueToStruct;

    const ENCODING_PREFIX_RANGE: Range<usize> = 0..PrefixID::LENGTH;
    const ENCODING_VALUE_TYPE_RANGE: Range<usize> =
        Self::ENCODING_PREFIX_RANGE.end..{ Self::ENCODING_PREFIX_RANGE.end + 1 };
    const ENCODING_TYPEID_RANGE: Range<usize> =
        Self::ENCODING_VALUE_TYPE_RANGE.end..{ Self::ENCODING_VALUE_TYPE_RANGE.end + TypeID::LENGTH };

    const ENCODING_VALUE_RANGE_SHORT: Range<usize> =
        Self::ENCODING_TYPEID_RANGE.end..{ Self::ENCODING_TYPEID_RANGE.end + AttributeIDLength::Short.length() };
    const ENCODING_STRUCT_ATTRIBUTE_ID_RANGE_LONG: Range<usize> =
        Self::ENCODING_VALUE_RANGE_LONG.end..{ Self::ENCODING_VALUE_RANGE_LONG.end + StructAttributeID::LENGTH };

    const ENCODING_VALUE_RANGE_LONG: Range<usize> =
        Self::ENCODING_TYPEID_RANGE.end..{ Self::ENCODING_TYPEID_RANGE.end + AttributeIDLength::Long.length() };
    const ENCODING_STRUCT_ATTRIBUTE_ID_RANGE_SHORT: Range<usize> =
        Self::ENCODING_VALUE_RANGE_SHORT.end..{ Self::ENCODING_VALUE_RANGE_SHORT.end + StructAttributeID::LENGTH };

    pub fn build<'b, 'c>(
        snapshot: &impl ReadableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        path_to_field: &Vec<StructFieldIDUInt>,
        value: &FieldValue<'b>,
        attribute: &AttributeVertex<'c>,
    ) -> Result<StructIndexEntry<'static>, Arc<SnapshotIteratorError>> {
        debug_assert_eq!(Prefix::VertexAttributeStruct, attribute.prefix());
        let mut buf = Self::build_search_key_bytes(snapshot, hasher, path_to_field, value, attribute.type_id_())?;
        buf.extend_from_slice(attribute.attribute_id().bytes());

        let value_bytes = match &value {
            FieldValue::Boolean(_)
            | FieldValue::Long(_)
            | FieldValue::Double(_)
            | FieldValue::Decimal(_)
            | FieldValue::DateTime(_)
            | FieldValue::DateTimeTZ(_)
            | FieldValue::Duration(_) => {
                None
            },
            FieldValue::String(value) => Some(StringBytes::<BUFFER_VALUE_INLINE>::build_owned(value).into_bytes()),
            FieldValue::Struct(_) => unreachable!(),
        };
        Ok(Self { key_bytes: Bytes::copy(buf.as_slice()), value_bytes })
    }

    pub fn build_search_key<'b, 'c>(
        snapshot: &impl ReadableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        path_to_field: &Vec<StructFieldIDUInt>,
        value: &FieldValue<'b>,
        attribute_type: &TypeVertex<'c>,
    ) -> Result<StorageKey<'static, BUFFER_KEY_INLINE>, Arc<SnapshotIteratorError>> {
        let mut buf = Self::build_search_key_bytes(snapshot, hasher, path_to_field, value, attribute_type.type_id_())?;
        Ok(StorageKey::new_owned(Self::KEYSPACE, ByteArray::copy(buf.as_slice())))
    }

    fn build_search_key_bytes<'b>(
        snapshot: &impl ReadableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        path_to_field: &Vec<StructFieldIDUInt>,
        value: &FieldValue<'b>,
        attribute_type_id: TypeID,
    ) -> Result<Vec<u8>, Arc<SnapshotIteratorError>> {
        let mut buf: Vec<u8> = Vec::with_capacity(
            PrefixID::LENGTH +  // Prefix::IndexValueToStruct
                PrefixID::LENGTH +      // ValueTypeCategory of indexed value
                TypeID::LENGTH +        // TypeID of Attribute being indexed
                path_to_field.len() +   // Path to the field
                AttributeIDLength::Long.length() + // Value for the field.
                StructAttributeID::LENGTH, // ID of the attribute being indexed
        );

        buf.extend_from_slice(&Prefix::IndexValueToStruct.prefix_id().bytes);
        buf.extend_from_slice(&value.value_type().category().to_bytes());
        buf.extend_from_slice(&attribute_type_id.bytes());
        for p in path_to_field {
            buf.extend_from_slice(&p.to_be_bytes())
        }
        match &value {
            FieldValue::Boolean(value) => buf.extend_from_slice(&BooleanBytes::build(*value).bytes()),
            FieldValue::Long(value) => buf.extend_from_slice(&LongBytes::build(*value).bytes()),
            FieldValue::Double(value) => buf.extend_from_slice(&DoubleBytes::build(*value).bytes()),
            FieldValue::Decimal(value) => buf.extend_from_slice(&DecimalBytes::build(*value).bytes()),
            FieldValue::DateTime(value) => buf.extend_from_slice(&DateTimeBytes::build(*value).bytes()),
            FieldValue::DateTimeTZ(value) => buf.extend_from_slice(&DateTimeTZBytes::build(*value).bytes()),
            FieldValue::Duration(value) => buf.extend_from_slice(&DurationBytes::build(*value).bytes()),
            FieldValue::String(value) => {
                let string_bytes = StringBytes::<0>::build_ref(value);
                Self::encode_string_into(snapshot, hasher, string_bytes.as_reference(), &mut buf)?;
            }
            FieldValue::Struct(_) => unreachable!(),
        };
        Ok(buf)
    }
}

impl<'a> StructIndexEntry<'a> {
    const STRING_FIELD_LENGTH: usize = 17;
    const STRING_FIELD_DISAMBIGUATED_HASH_LENGTH: usize = 9;
    const STRING_FIELD_HASHED_PREFIX_LENGTH: usize =
        { Self::STRING_FIELD_LENGTH - Self::STRING_FIELD_DISAMBIGUATED_HASH_LENGTH };
    const STRING_FIELD_HASHED_FLAG: u8 = 0b1000_0000;
    const STRING_FIELD_HASHED_FLAG_INDEX: usize = Self::STRING_FIELD_HASHED_HASH_LENGTH;
    const STRING_FIELD_HASHED_HASH_LENGTH: usize = Self::STRING_FIELD_DISAMBIGUATED_HASH_LENGTH - 1;
    const STRING_FIELD_INLINE_LENGTH: usize = { Self::STRING_FIELD_LENGTH - 1 };

    fn encode_string_into<const INLINE_SIZE: usize>(
        snapshot: &impl ReadableSnapshot,
        hasher: &impl Fn(&[u8]) -> u64,
        string_bytes: StringBytes<'_, INLINE_SIZE>,
        buf: &mut Vec<u8>,
    ) -> Result<(), Arc<SnapshotIteratorError>> {
        if Self::is_string_inlineable(string_bytes.as_reference()) {
            let mut inline_bytes: [u8; { StructIndexEntry::STRING_FIELD_INLINE_LENGTH }] =
                [0; { StructIndexEntry::STRING_FIELD_INLINE_LENGTH }];
            inline_bytes[0..string_bytes.bytes().length()].copy_from_slice(string_bytes.bytes().bytes());
            buf.extend_from_slice(&inline_bytes);
            buf.push(string_bytes.length() as u8);
        } else {
            buf.extend_from_slice(&string_bytes.bytes().bytes()[0..Self::STRING_FIELD_HASHED_PREFIX_LENGTH]);
            let prefix_key: Bytes<'_, BUFFER_KEY_INLINE> = Bytes::reference(buf.as_slice());
            let disambiguated_hash_bytes: [u8; StructIndexEntry::STRING_FIELD_DISAMBIGUATED_HASH_LENGTH] =
                match Self::find_existing_or_next_disambiguated_hash(
                    snapshot,
                    hasher,
                    prefix_key,
                    string_bytes.bytes().bytes(),
                )? {
                    Either::First(hash) => hash,
                    Either::Second(hash) => hash,
                };
            buf.extend_from_slice(&disambiguated_hash_bytes);
        }
        Ok(())
    }

    fn is_string_inlineable<'b, const INLINE_SIZE: usize>(string_bytes: StringBytes<'b, INLINE_SIZE>) -> bool {
        string_bytes.length() < Self::STRING_FIELD_HASHED_PREFIX_LENGTH + Self::STRING_FIELD_DISAMBIGUATED_HASH_LENGTH
    }
}

impl<'a> DisambiguatingHashedID<{ StructIndexEntry::STRING_FIELD_DISAMBIGUATED_HASH_LENGTH }> for StructIndexEntry<'a> {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data; // TODO
    const FIXED_WIDTH_KEYS: bool = { Prefix::IndexValueToStruct.fixed_width_keys() };
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for StructIndexEntry<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.key_bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.key_bytes
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for StructIndexEntry<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl<'a, 'de> Deserialize<'de> for FieldValue<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = FieldValue<'static>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("`Value`")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let prefix_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let prefix = Prefix::from_prefix_id(PrefixID::new(prefix_bytes));
                match prefix {
                    Prefix::VertexAttributeBoolean => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Boolean(BooleanBytes::new(value_bytes).as_bool()))
                    }
                    Prefix::VertexAttributeLong => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Long(LongBytes::new(value_bytes).as_i64()))
                    }
                    Prefix::VertexAttributeDouble => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Double(DoubleBytes::new(value_bytes).as_f64()))
                    }
                    Prefix::VertexAttributeDecimal => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Decimal(DecimalBytes::new(value_bytes).as_decimal()))
                    }
                    Prefix::VertexAttributeDateTime => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::DateTime(DateTimeBytes::new(value_bytes).as_naive_date_time()))
                    }
                    Prefix::VertexAttributeDateTimeTZ => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::DateTimeTZ(DateTimeTZBytes::new(value_bytes).as_date_time()))
                    }
                    Prefix::VertexAttributeDuration => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Duration(DurationBytes::new(value_bytes).as_duration()))
                    }
                    Prefix::VertexAttributeString => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::String(Cow::Owned(
                            StringBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(ByteReference::new(value_bytes)))
                                .as_str()
                                .to_owned(),
                        )))
                    }
                    Prefix::VertexAttributeStruct => {
                        let struct_value = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(FieldValue::Struct(Cow::Owned(struct_value)))
                    }
                    other => Err(de::Error::invalid_value(Unexpected::Bytes(&prefix_bytes), &self)),
                }
            }
        }

        deserializer.deserialize_seq(ValueVisitor)
    }
}

impl<'a> Serialize for StructValue<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(2))?;
        s.serialize_element(&self.definition_key)?;
        s.serialize_element(&self.fields)?;
        s.end()
    }
}

impl<'a, 'de> Deserialize<'de> for StructValue<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        pub struct StructValueVisitor;
        impl<'de> Visitor<'de> for StructValueVisitor {
            type Value = StructValue<'static>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("`StructValue`")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut s = seq;
                assert_eq!(2, s.size_hint().unwrap());
                let definition_key = s.next_element::<DefinitionKey<'de>>()?.unwrap().into_owned();
                let fields = s.next_element::<HashMap<StructFieldIDUInt, FieldValue<'static>>>()?.unwrap();
                Ok(StructValue { definition_key, fields })
            }
        }
        deserializer.deserialize_seq(StructValueVisitor)
    }
}

pub mod test {
    use std::{borrow::Cow, collections::HashMap};

    use resource::constants::snapshot::BUFFER_VALUE_INLINE;

    use crate::{
        graph::definition::{
            definition_key::{DefinitionID, DefinitionKey},
            r#struct::StructDefinition,
        },
        value::{
            struct_bytes::StructBytes,
            value_struct::{FieldValue, StructValue},
        },
    };

    #[test]
    fn test_serde() {
        let long_value = FieldValue::Long(5);
        let nested_key = DefinitionKey::build(StructDefinition::PREFIX, DefinitionID::build(0));
        let nested_fields = HashMap::from([(0, long_value)]);
        let nested_struct = StructValue { definition_key: nested_key, fields: nested_fields };

        let struct_key = DefinitionKey::build(StructDefinition::PREFIX, DefinitionID::build(0));
        let struct_fields = HashMap::from([(0, FieldValue::Struct(Cow::Owned(nested_struct.clone())))]);
        let struct_value = StructValue { definition_key: struct_key, fields: struct_fields };

        let struct_bytes: StructBytes<'static, BUFFER_VALUE_INLINE> = struct_value.to_bytes();
        let decoded = StructValue::from_bytes(struct_bytes.as_reference());
        assert_eq!(decoded.fields, struct_value.fields);
    }
}

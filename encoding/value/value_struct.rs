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
    borrow::Cow,
    collections::HashMap,
    fmt::{Formatter, Write},
};
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use crate::{
    error::EncodingError,
    graph::definition::{
        definition_key::DefinitionKey,
        r#struct::{StructDefinition, StructDefinitionField},
    },
    layout::prefix::{Prefix, PrefixID},
    value::{
        boolean_bytes::BooleanBytes,
        date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes,
        decimal_bytes::DecimalBytes,
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        long_bytes::LongBytes,
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        ValueEncodable,
    },
    AsBytes,
};
use resource::constants::{encoding::StructFieldIDUInt, snapshot::BUFFER_VALUE_INLINE};
use serde::{
    de,
    de::{EnumAccess, SeqAccess, Unexpected, VariantAccess, Visitor},
    ser::{SerializeSeq},
    Deserialize, Deserializer, Serialize, Serializer,
};
use crate::value::decimal_value::Decimal;
use crate::value::duration_value::Duration;
use crate::value::value_type::ValueType;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue<'a> { // Tempting to make it all static
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

    //
    // // TODO: Lots of wasted space using a u16 path.
    // pub fn TEMP__create_index_entries(&self, thing_vertex_generator: Arc<ThingVertexGenerator>, attribute_id: StructAttributeID) -> Vec<Vec<u8>> {
    //     let mut acc: Vec<Vec<u8>> = Vec::new();
    //     let mut path: Vec<StructFieldIDUInt> = Vec::new();
    //     Self::create_index_entries_rec(thing_vertex_generator, &self.fields, &mut path, &mut acc);
    //     acc
    // }
    //
    // fn create_index_entries_rec(thing_vertex_generator: Arc<ThingVertexGenerator>, fields: &HashMap<StructFieldIDUInt, Value<'a>>, path: &mut Vec<StructFieldIDUInt>, acc: &mut Vec<Vec<u8>>) {
    //     for (idx, value) in fields.iter() {
    //         if let Value::Struct(struct_val) = value {
    //             path.push(*idx);
    //             Self::create_index_entries_rec(thing_vertex_generator.clone(), struct_val.fields(), path, acc);
    //             let popped = path.pop();
    //             debug_assert_eq!(*idx, popped);
    //         } else {
    //             acc.push(Self::create_index_encode_entry(thing_vertex_generator.clone(), path, value))
    //         }
    //     }
    //
    // }
    //
    // fn create_index_encode_entry(thing_vertex_generator: Arc<ThingVertexGenerator>, path: &Vec<u16>, value: &Value<'a>) -> Vec<u8> {
    //     let encoded : Vec<u8> = Vec::with_capacity(PrefixID::LENGTH + path.len() + AttributeIDLength::LONG_LENGTH + StructAttributeID::ENCODING_LENGTH);
    //     for p in path {
    //         encoded.extend_from_slice(p.as_be_bytes())
    //     }
    //     match value {
    //         Value::Boolean(_) => {}
    //         Value::Long(_) => {}
    //         Value::Double(_) => {}
    //         Value::Decimal(_) => {}
    //         Value::DateTime(_) => {}
    //         Value::DateTimeTZ(_) => {}
    //         Value::Duration(_) => {}
    //         Value::String(_) => {},
    //         Value::Struct(_) => unreachable!()
    //     }
    //
    // }
}

// TODO: implement serialise/deserialise for the StructValue
//       since JSON serialisation seems to be able to handle recursive nesting, it should be able to handle that
impl<'a> FieldValue<'a> {
    pub const ENUM_NAME: &'static str = "Value";
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
    use crate::graph::definition::definition_key::{DefinitionID, DefinitionKey};
    use crate::graph::definition::r#struct::StructDefinition;

    use crate::value::struct_bytes::StructBytes;
    use resource::constants::snapshot::BUFFER_VALUE_INLINE;
    use crate::value::value_struct::{FieldValue, StructValue};

    #[test]
    fn test_serde() {
        let long_value = FieldValue::Long(5);
        let nested_key = DefinitionKey::build(StructDefinition::PREFIX, DefinitionID::build(0));
        let nested_fields = HashMap::from([(0, long_value)]);
        let nested_struct = StructValue { definition_key: nested_key , fields: nested_fields };

        let struct_key = DefinitionKey::build(StructDefinition::PREFIX, DefinitionID::build(0));
        let struct_fields = HashMap::from([(0, FieldValue::Struct(Cow::Owned(nested_struct.clone())))]);
        let struct_value = StructValue { definition_key: struct_key, fields: struct_fields };

        let struct_bytes: StructBytes<'static, BUFFER_VALUE_INLINE> = struct_value.to_bytes();
        let decoded = StructValue::from_bytes(struct_bytes.as_reference());
        assert_eq!(decoded.fields, struct_value.fields);
    }
}

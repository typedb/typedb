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

use std::{borrow::Cow, collections::HashMap, fmt::Formatter};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, Bytes};
use encoding::{
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
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        long_bytes::LongBytes,
        string_bytes::StringBytes,
        struct_bytes::{StructBytes, StructRepresentation},
        ValueEncodable,
    },
    AsBytes,
};
use iterator::Collector;
use resource::constants::{encoding::StructFieldIDUInt, snapshot::BUFFER_VALUE_INLINE};
use serde::{
    de,
    de::{EnumAccess, SeqAccess, Unexpected, VariantAccess, Visitor},
    ser::{SerializeSeq, SerializeTuple, SerializeTupleVariant},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::thing::value::Value;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct StructValue<'a> {
    // a map allows empty fields to not be recorded at all
    fields: HashMap<StructFieldIDUInt, Value<'a>>,
}

impl<'a> StructValue<'a> {
    // TODO: Return vec<ValueTypeMismatch>
    pub fn try_translate_fields(
        struct_definition: StructDefinition,
        value: HashMap<String, Value<'a>>,
    ) -> Result<HashMap<StructFieldIDUInt, Value<'a>>, Vec<EncodingError>> {
        let mut fields: HashMap<StructFieldIDUInt, Value<'a>> = HashMap::new();
        let mut errors: Vec<EncodingError> = Vec::new();
        for (field_name, field_id) in struct_definition.field_names {
            let field_definition: &StructDefinitionField = &struct_definition.fields.get(field_id as usize).unwrap();
            if let Some(value) = value.get(&field_name) {
                if field_definition.value_type == value.value_type() {
                    fields.insert(field_id, value.clone());
                } else {
                    errors.add(EncodingError::StructFieldValueTypeMismatch {
                        field_name,
                        expected: field_definition.value_type.clone(),
                    })
                }
            } else if !field_definition.optional {
                errors.add(EncodingError::StructMissingRequiredField { field_name })
            }
        }

        if errors.is_empty() {
            Ok(fields)
        } else {
            Err(errors)
        }
    }

    pub fn definition_key(&self) -> DefinitionKey<'_> {
        // self.definition.as_reference()
        todo!()
    }
}

impl<'a> StructRepresentation<'a> for StructValue<'a> {
    fn to_bytes<const INLINE_LENGTH: usize>(&self) -> StructBytes<'static, INLINE_LENGTH> {
        StructBytes::new(Bytes::Array(ByteArray::boxed(bincode::serialize(self).unwrap().into_boxed_slice())))
    }

    fn from_bytes<const INLINE_LENGTH: usize>(struct_bytes: &StructBytes<'a, INLINE_LENGTH>) -> Self {
        bincode::deserialize(struct_bytes.bytes().bytes()).unwrap()
    }
}

// TODO: implement serialise/deserialise for the StructValue
//       since JSON serialisation seems to be able to handle recursive nesting, it should be able to handle that
impl<'a> Value<'a> {
    pub(crate) const ENUM_NAME: &'static str = "Value";
}

impl<'a> Serialize for Value<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Boolean(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeBoolean.prefix_id().bytes())?;
                seq.serialize_element(&BooleanBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::Long(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeLong.prefix_id().bytes())?;
                seq.serialize_element(&LongBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::Double(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDouble.prefix_id().bytes())?;
                seq.serialize_element(&DoubleBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::DateTime(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDateTime.prefix_id().bytes())?;
                seq.serialize_element(&DateTimeBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::DateTimeTZ(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDateTimeTZ.prefix_id().bytes())?;
                seq.serialize_element(&DateTimeTZBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::Duration(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeDuration.prefix_id().bytes())?;
                seq.serialize_element(&DurationBytes::build(*value).bytes())?;
                seq.end()
            }
            Value::String(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeString.prefix_id().bytes())?;
                seq.serialize_element(&StringBytes::<BUFFER_VALUE_INLINE>::build_ref(value).bytes().bytes())?;
                seq.end()
            }
            Value::Struct(value) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(&Prefix::VertexAttributeStruct.prefix_id().bytes())?;
                seq.serialize_element(&StructBytes::<BUFFER_VALUE_INLINE>::build(value).bytes().bytes())?;
                seq.end()
            }
        }
    }
}

impl<'a, 'de> Deserialize<'de> for Value<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value<'static>;

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
                        Ok(Value::Boolean(BooleanBytes::new(value_bytes).as_bool()))
                    }
                    Prefix::VertexAttributeLong => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::Long(LongBytes::new(value_bytes).as_i64()))
                    }
                    Prefix::VertexAttributeDouble => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::Double(DoubleBytes::new(value_bytes).as_f64()))
                    }
                    Prefix::VertexAttributeDateTime => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::DateTime(DateTimeBytes::new(value_bytes).as_naive_date_time()))
                    }
                    Prefix::VertexAttributeDateTimeTZ => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::DateTimeTZ(DateTimeTZBytes::new(value_bytes).as_date_time()))
                    }
                    Prefix::VertexAttributeDuration => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::Duration(DurationBytes::new(value_bytes).as_duration()))
                    }
                    Prefix::VertexAttributeString => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        Ok(Value::String(Cow::Owned(
                            StringBytes::new(Bytes::<BUFFER_VALUE_INLINE>::Reference(ByteReference::new(value_bytes)))
                                .as_str()
                                .to_owned(),
                        )))
                    }
                    Prefix::VertexAttributeStruct => {
                        let value_bytes = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                        let struct_bytes = StructBytes::new(Bytes::<BUFFER_VALUE_INLINE>::copy(value_bytes));
                        Ok(Value::Struct(Cow::Owned(StructValue::from_bytes(&struct_bytes))))
                    }
                    other => Err(de::Error::invalid_value(Unexpected::Bytes(&prefix_bytes), &self)),
                }
            }
        }

        deserializer.deserialize_seq(ValueVisitor)
    }
}

pub mod test {
    use std::{borrow::Cow, collections::HashMap};

    use encoding::value::struct_bytes::{StructBytes, StructRepresentation};
    use resource::constants::snapshot::BUFFER_VALUE_INLINE;

    use crate::thing::{value::Value, value_struct::StructValue};
    #[test]
    fn test_serde() {
        let long_value = Value::Long(5);
        let nested_fields = HashMap::from([(0, long_value)]);
        let nested_struct = StructValue { fields: nested_fields };
        let struct_fields = HashMap::from([(0, Value::Struct(Cow::Owned(nested_struct.clone())))]);
        let struct_value = StructValue { fields: struct_fields };
        let struct_bytes: StructBytes<'static, BUFFER_VALUE_INLINE> = struct_value.to_bytes();
        let decoded = StructValue::from_bytes(&struct_bytes);
        assert_eq!(decoded.fields, struct_value.fields);
    }
}

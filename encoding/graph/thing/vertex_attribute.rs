/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt::{Display, Formatter},
    marker::PhantomData,
    ops::Range,
    sync::Arc,
};

use bytes::{byte_array::ByteArray, byte_reference::ByteReference, util::HexBytesFormatter, Bytes};
use chrono::{prelude::DateTime, NaiveDate, NaiveDateTime};
use primitive::either::Either;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use seahash::hash;
use storage::{
    key_value::{StorageKey, StorageKeyReference},
    keyspace::KeyspaceSet,
    snapshot::{iterator::SnapshotIteratorError, ReadableSnapshot},
};

use crate::{
    graph::{
        common::value_hasher::HashedID,
        thing::{ThingVertex, THING_VERTEX_LENGTH_PREFIX_TYPE},
        type_::vertex::TypeID,
        Typed,
    },
    layout::prefix::Prefix,
    value::{
        boolean_bytes::BooleanBytes,
        date_bytes::DateBytes,
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
        value::Value,
        timezone::TimeZone,
        value_type::{ValueType, ValueTypeBytes, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct AttributeVertex<'a> {
    bytes: Bytes<'a, BUFFER_KEY_INLINE>,
}

impl<'a> AttributeVertex<'a> {
    const PREFIX: Prefix = Prefix::VertexAttribute;

    pub fn build(type_id: TypeID, attribute_id: AttributeID) -> Self {
        let mut bytes = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE + attribute_id.length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        bytes.bytes_mut()[Self::range_for_attribute_id(attribute_id.length())].copy_from_slice(attribute_id.bytes());
        Self { bytes: Bytes::Array(bytes) }
    }
    pub fn build_prefix_for_value(
        type_id: TypeID,
        value: Value<'_>,
        large_value_hasher: &impl Fn(&[u8]) -> u64,
    ) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        // preallocate upper bound length and then truncate later
        let mut bytes = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE + AttributeID::max_length());
        bytes.bytes_mut()[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes.bytes_mut()[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        let id_length = AttributeID::write_deterministic_value_or_prefix(
            &mut bytes.bytes_mut()[Self::RANGE_TYPE_ID.end..],
            value,
            large_value_hasher,
        );
        bytes.truncate(Self::RANGE_TYPE_ID.end + id_length);
        StorageKey::new(Self::KEYSPACE, Bytes::Array(bytes))
    }

    pub(crate) fn write_prefix_type_attribute_id(
        bytes: &mut [u8],
        type_id: TypeID,
        attribute_id_part: &[u8],
    ) -> usize {
        bytes[Self::RANGE_PREFIX].copy_from_slice(&Self::PREFIX.prefix_id().bytes());
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.bytes());
        bytes[Self::range_for_attribute_id(attribute_id_part.len())].copy_from_slice(attribute_id_part);
        Self::RANGE_TYPE_ID.end + attribute_id_part.len()
    }

    pub fn is_attribute_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        storage_key.keyspace_id() == Self::KEYSPACE.id()
            && !storage_key.bytes().is_empty()
            && storage_key.bytes()[Self::RANGE_PREFIX] == Prefix::VertexAttribute.prefix_id().bytes()
    }

    pub fn value_type_category(&self) -> ValueTypeCategory {
        self.attribute_id().value_type_category()
    }

    pub fn attribute_id(&self) -> AttributeID {
        AttributeID::new(&self.bytes.bytes()[self.range_of_attribute_id()])
    }

    fn range_of_attribute_id(&self) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..self.length()
    }

    fn range_for_attribute_id(id_length: usize) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + id_length
    }

    pub(crate) fn length(&self) -> usize {
        self.bytes.length()
    }

    pub fn as_reference<'this: 'a>(&'this self) -> AttributeVertex<'this> {
        Self::new(Bytes::Reference(self.bytes.as_reference()))
    }

    pub fn into_owned(self) -> AttributeVertex<'static> {
        AttributeVertex { bytes: self.bytes.into_owned() }
    }
}

impl<'a> AsBytes<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    fn bytes(&'a self) -> ByteReference<'a> {
        self.bytes.as_reference()
    }

    fn into_bytes(self) -> Bytes<'a, BUFFER_KEY_INLINE> {
        self.bytes
    }
}

impl<'a> Prefixed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> Typed<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {}

impl<'a> ThingVertex<'a> for AttributeVertex<'a> {
    fn new(bytes: Bytes<'a, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() > THING_VERTEX_LENGTH_PREFIX_TYPE);
        AttributeVertex { bytes }
    }
}

impl<'a> Keyable<'a, BUFFER_KEY_INLINE> for AttributeVertex<'a> {
    fn keyspace(&self) -> EncodingKeyspace {
        EncodingKeyspace::Data
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ValueEncodingLength {
    Short,
    Long,
}

impl ValueEncodingLength {
    const SHORT_LENGTH: usize = 8;
    const LONG_LENGTH: usize = 17;

    pub(crate) const fn length(&self) -> usize {
        match self {
            ValueEncodingLength::Short => Self::SHORT_LENGTH,
            ValueEncodingLength::Long => Self::LONG_LENGTH,
        }
    }

    const fn max_length() -> usize {
        // TODO: this is brittle - ideally we'd compute this over the max of the enum variants
        Self::LONG_LENGTH
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AttributeID {
    Boolean(BooleanAttributeID),
    Long(LongAttributeID),
    Double(DoubleAttributeID),
    Decimal(DecimalAttributeID),
    Date(DateAttributeID),
    DateTime(DateTimeAttributeID),
    DateTimeTZ(DateTimeTZAttributeID),
    Duration(DurationAttributeID),
    String(StringAttributeID),
    Struct(StructAttributeID),
}

impl AttributeID {
    pub fn new(bytes: &[u8]) -> Self {
        let &[prefix, ..] = bytes else { unreachable!("empty value bytes") };
        match ValueTypeCategory::from_bytes([prefix]) {
            ValueTypeCategory::Boolean => Self::Boolean(BooleanAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Long => Self::Long(LongAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Double => Self::Double(DoubleAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Decimal => Self::Decimal(DecimalAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Date => Self::Date(DateAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::DateTime => Self::DateTime(DateTimeAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::DateTimeTZ => Self::DateTimeTZ(DateTimeTZAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Duration => Self::Duration(DurationAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::String => Self::String(StringAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Struct => Self::Struct(StructAttributeID::new(bytes.try_into().unwrap())),
        }
    }

    pub fn build_inline(value: Value<'_>) -> Self {
        debug_assert!(Self::is_inlineable(value.as_reference()));
        match value.value_type() {
            ValueType::Boolean => Self::Boolean(BooleanAttributeID::build(value.encode_boolean())),
            ValueType::Long => Self::Long(LongAttributeID::build(value.encode_long())),
            ValueType::Double => Self::Double(DoubleAttributeID::build(value.encode_double())),
            ValueType::Decimal => Self::Decimal(DecimalAttributeID::build(value.encode_decimal())),
            ValueType::Date => Self::Date(DateAttributeID::build(value.encode_date())),
            ValueType::DateTime => Self::DateTime(DateTimeAttributeID::build(value.encode_date_time())),
            ValueType::DateTimeTZ => Self::DateTimeTZ(DateTimeTZAttributeID::build(value.encode_date_time_tz())),
            ValueType::Duration => Self::Duration(DurationAttributeID::build(value.encode_duration())),
            ValueType::String => Self::String(StringAttributeID::build_inline_id(value.encode_string::<256>())),
            ValueType::Struct(_) => todo!(),
        }
    }

    pub fn write_deterministic_value_or_prefix(
        bytes: &mut [u8],
        value: Value<'_>,
        large_value_hasher: &impl Fn(&[u8]) -> u64,
    ) -> usize {
        debug_assert!(bytes.len() >= AttributeID::max_length());
        match value.value_type().category() {
            ValueTypeCategory::Boolean => BooleanAttributeID::write(value.encode_boolean(), bytes),
            ValueTypeCategory::Long => LongAttributeID::write(value.encode_long(), bytes),
            ValueTypeCategory::Double => DoubleAttributeID::write(value.encode_double(), bytes),
            ValueTypeCategory::Decimal => DecimalAttributeID::write(value.encode_decimal(), bytes),
            ValueTypeCategory::Date => DateAttributeID::write(value.encode_date(), bytes),
            ValueTypeCategory::DateTime => DateTimeAttributeID::write(value.encode_date_time(), bytes),
            ValueTypeCategory::DateTimeTZ => DateTimeTZAttributeID::write(value.encode_date_time_tz(), bytes),
            ValueTypeCategory::Duration => DurationAttributeID::write(value.encode_duration(), bytes),
            ValueTypeCategory::String => {
                StringAttributeID::write_deterministic_prefix(value.encode_string::<64>(), large_value_hasher, bytes)
            }
            ValueTypeCategory::Struct => StructAttributeID::write_hashed_id_deterministic_prefix(
                value.encode_struct::<64>(),
                large_value_hasher,
                bytes,
            ),
        }
    }

    pub fn value_type_encoding_length(value_type_category: ValueTypeCategory) -> usize {
        match value_type_category {
            ValueTypeCategory::Boolean => BooleanAttributeID::LENGTH,
            ValueTypeCategory::Long => LongAttributeID::LENGTH,
            ValueTypeCategory::Double => DoubleAttributeID::LENGTH,
            ValueTypeCategory::Decimal => DecimalAttributeID::LENGTH,
            ValueTypeCategory::Date => DateAttributeID::LENGTH,
            ValueTypeCategory::DateTime => DateTimeAttributeID::LENGTH,
            ValueTypeCategory::DateTimeTZ => DateTimeTZAttributeID::LENGTH,
            ValueTypeCategory::Duration => DurationAttributeID::LENGTH,
            ValueTypeCategory::String => StringAttributeID::LENGTH,
            ValueTypeCategory::Struct => StructAttributeID::LENGTH,
        }
    }

    ///
    /// Return true if values in the value type are always fully encoded in the ID
    /// Return false if the values are hashed or incomplete and may require a secondary lookup
    ///
    pub fn is_inlineable(value: impl ValueEncodable) -> bool {
        match value.value_type() {
            ValueType::Boolean => BooleanAttributeID::is_inlineable(),
            ValueType::Long => LongAttributeID::is_inlineable(),
            ValueType::Double => DoubleAttributeID::is_inlineable(),
            ValueType::Decimal => DecimalAttributeID::is_inlineable(),
            ValueType::Date => DateAttributeID::is_inlineable(),
            ValueType::DateTime => DateTimeAttributeID::is_inlineable(),
            ValueType::DateTimeTZ => DateTimeTZAttributeID::is_inlineable(),
            ValueType::Duration => DurationAttributeID::is_inlineable(),
            ValueType::String => StringAttributeID::is_inlineable(value.encode_string::<256>()),
            ValueType::Struct(_) => StructAttributeID::is_inlineable(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            AttributeID::Boolean(boolean_id) => boolean_id.bytes_ref(),
            AttributeID::Long(long_id) => long_id.bytes_ref(),
            AttributeID::Double(double_id) => double_id.bytes_ref(),
            AttributeID::Decimal(decimal_id) => decimal_id.bytes_ref(),
            AttributeID::Date(date_id) => date_id.bytes_ref(),
            AttributeID::DateTime(date_time_id) => date_time_id.bytes_ref(),
            AttributeID::DateTimeTZ(date_time_tz_id) => date_time_tz_id.bytes_ref(),
            AttributeID::Duration(duration_id) => duration_id.bytes_ref(),
            AttributeID::String(string_id) => string_id.bytes_ref(),
            AttributeID::Struct(struct_id) => struct_id.bytes_ref(),
        }
    }

    pub(crate) const fn length(&self) -> usize {
        match self {
            AttributeID::Boolean(_) => BooleanAttributeID::LENGTH,
            AttributeID::Long(_) => LongAttributeID::LENGTH,
            AttributeID::Double(_) => DoubleAttributeID::LENGTH,
            AttributeID::Decimal(_) => DecimalAttributeID::LENGTH,
            AttributeID::Date(_) => DateAttributeID::LENGTH,
            AttributeID::DateTime(_) => DateTimeAttributeID::LENGTH,
            AttributeID::DateTimeTZ(_) => DateTimeTZAttributeID::LENGTH,
            AttributeID::Duration(_) => DurationAttributeID::LENGTH,
            AttributeID::String(_) => StringAttributeID::LENGTH,
            AttributeID::Struct(_) => StructAttributeID::LENGTH,
        }
    }

    pub(crate) const fn max_length() -> usize {
        ValueTypeBytes::CATEGORY_LENGTH + ValueEncodingLength::max_length()
    }

    pub fn unwrap_boolean(self) -> BooleanAttributeID {
        match self {
            AttributeID::Boolean(boolean_id) => boolean_id,
            _ => panic!("Cannot unwrap Boolean ID from non-boolean attribute ID."),
        }
    }

    pub fn unwrap_long(self) -> LongAttributeID {
        match self {
            AttributeID::Long(long_id) => long_id,
            _ => panic!("Cannot unwrap Long ID from non-long attribute ID."),
        }
    }

    pub fn unwrap_double(self) -> DoubleAttributeID {
        match self {
            AttributeID::Double(double_id) => double_id,
            _ => panic!("Cannot unwrap Double ID from non-double attribute ID."),
        }
    }

    pub fn unwrap_decimal(self) -> DecimalAttributeID {
        match self {
            AttributeID::Decimal(decimal_id) => decimal_id,
            _ => panic!("Cannot unwrap Decimal ID from non-decimal attribute ID."),
        }
    }

    pub fn unwrap_date(self) -> DateAttributeID {
        match self {
            AttributeID::Date(date_id) => date_id,
            _ => panic!("Cannot unwrap Date ID from non-date attribute ID."),
        }
    }

    pub fn unwrap_date_time(self) -> DateTimeAttributeID {
        match self {
            AttributeID::DateTime(date_time_id) => date_time_id,
            _ => panic!("Cannot unwrap DateTime ID from non-datetime attribute ID."),
        }
    }

    pub fn unwrap_date_time_tz(self) -> DateTimeTZAttributeID {
        match self {
            AttributeID::DateTimeTZ(date_time_tz_id) => date_time_tz_id,
            _ => panic!("Cannot unwrap DateTimeTZ ID from non-datetimeTZ attribute ID."),
        }
    }

    pub fn unwrap_duration(self) -> DurationAttributeID {
        match self {
            AttributeID::Duration(duration_id) => duration_id,
            _ => panic!("Cannot unwrap Duration ID from non-duration attribute ID."),
        }
    }

    pub fn unwrap_string(self) -> StringAttributeID {
        match self {
            AttributeID::String(string_id) => string_id,
            _ => panic!("Cannot unwrap String ID from non-string attribute ID."),
        }
    }

    pub fn unwrap_struct(self) -> StructAttributeID {
        match self {
            AttributeID::Struct(struct_id) => struct_id,
            _ => panic!("Cannot unwrap Struct ID from non-struct attribute ID."),
        }
    }

    pub fn value_type_category(self) -> ValueTypeCategory {
        match self {
            AttributeID::Boolean(_) => ValueTypeCategory::Boolean,
            AttributeID::Long(_) => ValueTypeCategory::Long,
            AttributeID::Double(_) => ValueTypeCategory::Double,
            AttributeID::Decimal(_) => ValueTypeCategory::Decimal,
            AttributeID::Date(_) => ValueTypeCategory::Date,
            AttributeID::DateTime(_) => ValueTypeCategory::DateTime,
            AttributeID::DateTimeTZ(_) => ValueTypeCategory::DateTimeTZ,
            AttributeID::Duration(_) => ValueTypeCategory::Duration,
            AttributeID::String(_) => ValueTypeCategory::String,
            AttributeID::Struct(_) => ValueTypeCategory::Struct,
        }
    }
}

impl Display for AttributeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(self.bytes()))
    }
}

pub trait InlineEncodableAttributeID {
    const ENCODED_LENGTH: usize;

    fn bytes_ref(&self) -> &[u8];
}

pub type BooleanAttributeID = InlinePrimitiveID<{ BooleanBytes::ENCODED_LENGTH }, BooleanBytes>;
pub type LongAttributeID = InlinePrimitiveID<{ LongBytes::ENCODED_LENGTH }, LongBytes>;
pub type DoubleAttributeID = InlinePrimitiveID<{ DoubleBytes::ENCODED_LENGTH }, DoubleBytes>;
pub type DecimalAttributeID = InlinePrimitiveID<{ DecimalBytes::ENCODED_LENGTH }, DecimalBytes>;
pub type DateAttributeID = InlinePrimitiveID<{ DateBytes::ENCODED_LENGTH }, DateBytes>;
pub type DateTimeAttributeID = InlinePrimitiveID<{ DateTimeBytes::ENCODED_LENGTH }, DateTimeBytes>;
pub type DateTimeTZAttributeID = InlinePrimitiveID<{ DateTimeTZBytes::ENCODED_LENGTH }, DateTimeTZBytes>;
pub type DurationAttributeID = InlinePrimitiveID<{ DurationBytes::ENCODED_LENGTH }, DurationBytes>;

// note: SIZE parameter is a workaround for not being able to use EncodedBytesTypeLL:INLINE_BYTES_LENGTH directly yet
//       https://github.com/rust-lang/rust/issues/60551
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InlinePrimitiveID<const ENCODED_BYTES_SIZE: usize, EncodedBytesType: InlineEncodableAttributeID> {
    bytes: [u8; ENCODED_BYTES_SIZE],
    _ph: PhantomData<EncodedBytesType>,
}

impl<const ENCODED_BYTES_SIZE: usize, EncodedBytesType: InlineEncodableAttributeID>
    InlinePrimitiveID<ENCODED_BYTES_SIZE, EncodedBytesType>
{
    // TODO: Update
    // const VALUE_TYPE_LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH;
    // const VALUE_LENGTH: usize = ValueEncodingLength::SHORT_LENGTH;
    // const LENGTH: usize = Self::VALUE_TYPE_LENGTH + Self::VALUE_LENGTH;
    //
    // const VALUE_TYPE: ValueTypeCategory = ValueTypeCategory::Boolean;
    //
    pub const LENGTH: usize = ENCODED_BYTES_SIZE;

    pub fn new(bytes: [u8; ENCODED_BYTES_SIZE]) -> Self {
        debug_assert!(ENCODED_BYTES_SIZE == EncodedBytesType::ENCODED_LENGTH);
        Self { bytes, _ph: PhantomData::default() }
    }

    pub(crate) fn build(value: EncodedBytesType) -> Self {
        debug_assert!(value.bytes_ref().len() == ENCODED_BYTES_SIZE);
        Self::new(value.bytes_ref().try_into().unwrap())
    }

    // write value bytes to the start of the byte slice and return the number of bytes written
    pub(crate) fn write(value: EncodedBytesType, bytes: &mut [u8]) -> usize {
        debug_assert!(bytes.len() >= value.bytes_ref().len());
        // TODO: update
        // bytes[..Self::VALUE_TYPE_LENGTH].copy_from_slice(&Self::VALUE_TYPE.to_bytes());
        // bytes[Self::VALUE_TYPE_LENGTH..].copy_from_slice(&value.bytes());
        bytes[0..value.bytes_ref().len()].copy_from_slice(value.bytes_ref());
        value.bytes_ref().len()
    }

    pub(crate) const fn is_inlineable() -> bool {
        true
    }

    pub fn bytes(&self) -> [u8; ENCODED_BYTES_SIZE] {
        self.bytes
    }

    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// String encoding scheme uses 17 bytes:
///
///   Case 1: string fits in 16 bytes
///     [16: string][1: 0b0[length]]
///
///   Case 2: string does not fit in 16 bytes:
///     [8: prefix][8: hash][1: 0b1[disambiguator]]
///
/// Framing this as the generalised birthday problem, where
///     D: #days in a year, X: #people in the room, p: probability of a collision, N: number of collisions
/// mapped to:
///     D: #buckets = 2^b for b bits of hash, X: #keys inserted
/// Taking a tuple approach reflected here: https://math.stackexchange.com/a/25878
/// We arrive at the p = (1/D^(N-1)) * (X^N)/(N!) ---> X = (p * N! D^(N-1) )^(1/N)
/// With an approximation: X = (D* N/e) * (p * sqrt(2 * pi * N)/D )**(1/N)
/// The dominating (DN) factor is intuitive, because it is the breaking point for an oracle hasher.
///
/// Even for, D = 2**64; N = 3; p=0.01   ;   X = 10^12.
/// If we're concerned about performance, we should be interested in the expected value for the
///     'number of collisions we'll encounter when looking up a given key'.
/// This is just (X/D)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StringAttributeID {
    bytes: [u8; Self::LENGTH],
}

impl StringAttributeID {
    // TODO: update
    // const VALUE_TYPE_LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH;
    // const VALUE_LENGTH: usize = ValueEncodingLength::LONG_LENGTH;
    // const LENGTH: usize = Self::VALUE_TYPE_LENGTH + Self::VALUE_LENGTH;
    //
    // const VALUE_TYPE: ValueTypeCategory = ValueTypeCategory::String;
    //
    // const INLINE_CAPACITY: usize = Self::VALUE_LENGTH - 1;
    //
    // pub const HASHED_PREFIX_LENGTH: usize = Self::INLINE_CAPACITY - Self::HASHID_HASH_LENGTH;
    // pub const HASHED_HASH_LENGTH: usize = 8;
    // pub const HASHED_ENCODING_LENGTH: usize = Self::HASHED_PREFIX_LENGTH + Self::HASHED_HASH_LENGTH;
    //
    // const HASHED_PREFIX_RANGE: Range<usize> =
    //     Self::VALUE_TYPE_LENGTH..Self::VALUE_TYPE_LENGTH + Self::HASHED_PREFIX_LENGTH;
    // const HASHED_HASH_RANGE: Range<usize> =
    //     Self::HASHED_PREFIX_RANGE.end..Self::HASHED_PREFIX_RANGE.end + Self::HASHED_HASH_LENGTH;
    // const HASHED_DISAMBIGUATED_HASH_RANGE: Range<usize> =
    //     Self::HASHED_HASH_RANGE.start..Self::HASHED_HASH_RANGE.end + 1;
    // const HASHED_PREFIX_HASH_RANGE: Range<usize> =
    //     Self::VALUE_TYPE_LENGTH..Self::VALUE_TYPE_LENGTH + Self::HASHED_ENCODING_LENGTH;
    //
    // const TAIL_IS_HASH_MASK: u8 = 0b1000_0000;
    // const TAIL_INDEX: usize = Self::LENGTH - 1;
    //
    //
    //
    const LENGTH: usize = AttributeIDLength::LONG_LENGTH;
    const INLINE_STRING_CAPACITY: usize = Self::LENGTH - 1;
    pub const HASHED_ID_STRING_PREFIX_LENGTH: usize =
        { StringAttributeID::INLINE_STRING_CAPACITY - StringAttributeID::HASH_LENGTH };
    pub const HASHED_ID_HASH_LENGTH: usize = 8;
    pub const HASHED_ID_STRING_PREFIX_HASH_LENGTH: usize =
        Self::HASHED_ID_STRING_PREFIX_LENGTH + Self::HASHED_ID_HASH_LENGTH;
    const ENCODING_STRING_TAIL_IS_HASH_MASK: u8 = 0b10000000;
    const ENCODING_STRING_TAIL_INDEX: usize = { Self::LENGTH - 1 };

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub(crate) fn is_inlineable<const INLINE_LENGTH: usize>(string: StringBytes<'_, INLINE_LENGTH>) -> bool {
        string.length() <= Self::INLINE_STRING_CAPACITY
    }

    pub(crate) fn build_inline_id<const INLINE_LENGTH: usize>(string: StringBytes<'_, INLINE_LENGTH>) -> Self {
        debug_assert!(Self::is_inlineable(string.as_reference()));
        let mut bytes = [0u8; Self::LENGTH];
        Self::write_inline_id(&mut bytes, string);
        Self::new(bytes)
    }

    // write the string bytes to the byte slice, and set the last byte to the string length
    pub(crate) fn write_inline_id<const INLINE_LENGTH: usize>(
        bytes: &mut [u8; Self::LENGTH],
        string: StringBytes<'_, INLINE_LENGTH>,
    ) {
        // TODO: update
        // let [prefix, value_bytes @ ..] = &mut bytes;
        // std::slice::from_mut(prefix).copy_from_slice(&Self::VALUE_TYPE.to_bytes());
        // value_bytes[..string.length()].copy_from_slice(string.bytes().bytes());
        // Self::set_tail_inline_length(&mut bytes, string.length() as u8);

        debug_assert!(Self::is_inlineable(string.as_reference()));
        bytes[0..string.length()].copy_from_slice(string.bytes().bytes());
        Self::set_tail_inline_length(bytes, string.length() as u8);
    }

    // TODO: don't use const 16 here
    pub fn get_inline_string_bytes(&self) -> StringBytes<'static, 16> {
        debug_assert!(self.is_inline());
        // TODO: update
        // let value_bytes = &self.bytes[Self::VALUE_TYPE_LENGTH..];
        // let mut bytes = ByteArray::zeros(Self::INLINE_CAPACITY);
        // let inline_string_length = self.get_inline_length() as usize;
        // bytes.bytes_mut()[..inline_string_length].copy_from_slice(&value_bytes[..inline_string_length]);
        // bytes.truncate(inline_string_length);
        // StringBytes::new(Bytes::Array(bytes))

        let mut bytes = ByteArray::zeros(Self::LENGTH);
        let inline_string_length = self.get_inline_length();
        bytes.bytes_mut()[0..inline_string_length as usize]
            .copy_from_slice(&self.bytes[0..inline_string_length as usize]);
        bytes.truncate(inline_string_length as usize);
        StringBytes::new(Bytes::Array(bytes))
    }

    pub fn get_inline_length(&self) -> u8 {
        debug_assert!(self.is_inline());
        self.bytes[Self::ENCODING_STRING_TAIL_INDEX]
    }

    fn build_or_find_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Self, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        // TODO: update
        //let mut bytes: [u8; Self::LENGTH] = [0; Self::LENGTH];
        //         bytes[..Self::VALUE_TYPE_LENGTH].copy_from_slice(&Self::VALUE_TYPE.to_bytes());
        //         bytes[Self::HASHED_PREFIX_RANGE].copy_from_slice(&string.bytes().bytes()[0..Self::HASHED_PREFIX_LENGTH]);
        //
        //         debug_assert!(!Self::is_inlineable(string.as_reference()));
        //         let key_without_hash = AttributeVertex::build_prefix_type_attribute_id(
        //             type_id,
        //             &bytes[0..Self::VALUE_TYPE_LENGTH + Self::HASHED_PREFIX_LENGTH],
        //         )
        //         .into_bytes();
        //
        //         let disambiguated_hash =
        //             Self::find_existing_or_next_disambiguated_hash(snapshot, hasher, key_without_hash, string.bytes().bytes())?;
        //
        //         let (Either::First(hashed_bytes) | Either::Second(hashed_bytes)) = disambiguated_hash;
        //
        //         debug_assert!(
        //             hashed_bytes[Self::HASHID_DISAMBIGUATOR_BYTE_INDEX] & Self::HASHID_DISAMBIGUATOR_BYTE_IS_HASH_FLAG != 0
        //         );
        //
        //         bytes[Self::HASHED_DISAMBIGUATED_HASH_RANGE].copy_from_slice(&hashed_bytes);
        //
        //         let hashed_id = Self { bytes };
        //
        //         match disambiguated_hash {
        //             Either::First(_) => Ok(Either::First(hashed_id)),
        //             Either::Second(_) => Ok(Either::Second(hashed_id)),
        //         }

        let mut bytes: [u8; Self::LENGTH] = [0; Self::LENGTH];
        bytes[0..{ Self::ENCODING_STRING_HASHED_PREFIX_LENGTH }]
            .copy_from_slice(&string.bytes().bytes()[0..{ Self::ENCODING_STRING_HASHED_PREFIX_LENGTH }]);

        debug_assert!(!Self::is_inlineable(string.as_reference()));
        let key_without_hash = AttributeVertex::build_prefix_type_attribute_id(
            ValueTypeCategory::String,
            type_id,
            &bytes[0..{ Self::ENCODING_STRING_HASHED_PREFIX_LENGTH }],
        )
        .into_bytes();

        let disambiguated_hash =
            Self::find_existing_or_next_disambiguated_hash(snapshot, hasher, key_without_hash, string.bytes().bytes())?;
        let hashed_id = match disambiguated_hash {
            Either::First(hashed_bytes) | Either::Second(hashed_bytes) => {
                debug_assert!(
                    hashed_bytes[Self::HASHID_DISAMBIGUATOR_BYTE_INDEX] & Self::HASHID_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
                        != 0
                );
                bytes[Self::ENCODING_STRING_HASHED_PREFIX_LENGTH..Self::LENGTH].copy_from_slice(&hashed_bytes);
                Self { bytes }
            }
        };
        match disambiguated_hash {
            Either::First(_) => Ok(Either::First(hashed_id)),
            Either::Second(_) => Ok(Either::Second(hashed_id)),
        }
    }

    fn build_or_find_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Either<Self, Self>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        // generate full AttributeVertex that we can use to check for existing hashed values in the same type
        let mut attribute_bytes = [0; AttributeVertex::RANGE_TYPE_ID.end + Self::LENGTH];
        let prefix_length = AttributeVertex::write_prefix_type_attribute_id(
            &mut attribute_bytes,
            ValueTypeCategory::String,
            type_id,
            &string.bytes().bytes()[0..{ Self::HASHED_ID_STRING_PREFIX_LENGTH }],
        );
        let disambiguated_hash = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            &attribute_bytes[0..prefix_length],
            string.bytes().bytes(),
        )?;
        let string_id = match disambiguated_hash {
            Either::First(disambiguated_hash) | Either::Second(disambiguated_hash) => {
                debug_assert!(
                    disambiguated_hash[Self::HASH_DISAMBIGUATOR_BYTE_INDEX]
                        & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
                        != 0
                );
                let mut string_id_bytes = [0; Self::LENGTH];
                string_id_bytes[0..Self::HASHED_ID_STRING_PREFIX_LENGTH]
                    .copy_from_slice(&string.bytes().bytes()[0..{ Self::HASHED_ID_STRING_PREFIX_LENGTH }]);
                string_id_bytes[Self::HASHED_ID_STRING_PREFIX_LENGTH..Self::LENGTH]
                    .copy_from_slice(&disambiguated_hash);
                Self { bytes: string_id_bytes }
            }
        };
        match disambiguated_hash {
            Either::First(_) => Ok(Either::First(string_id)),
            Either::Second(_) => Ok(Either::Second(string_id)),
        }
    }

    pub(crate) fn build_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Self, Arc<SnapshotIteratorError>>
        where
            Snapshot: ReadableSnapshot,
    {
        match Self::build_or_find_hashed_id(type_id, string, snapshot, hasher)? {
            Either::First(hashed_id) | Either::Second(hashed_id) => Ok(hashed_id),
        }
    }

    pub(crate) fn find_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Option<Self>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        debug_assert!(!Self::is_inlineable(string.as_reference()));
        match Self::build_or_find_hashed_id(type_id, string, snapshot, hasher)? {
            Either::First(hashed_id) => Ok(Some(hashed_id)),
            Either::Second(_) => Ok(None),
        }
    }

    // write the deterministic prefix of the hash ID, and return the length of the prefix written
    pub(crate) fn write_deterministic_prefix<const INLINE_LENGTH: usize>(
        string: StringBytes<'_, INLINE_LENGTH>,
        hasher: &impl Fn(&[u8]) -> u64,
        bytes: &mut [u8],
    ) -> usize {
        todo!("update!")
        debug_assert!(bytes.len() >= Self::LENGTH);
        if Self::is_inlineable(string.as_reference()) {
            let bytes_range = &mut bytes[0..Self::LENGTH];
            Self::write_inline_id(bytes_range.try_into().unwrap(), string);
            Self::LENGTH
        } else {
            bytes[0..Self::HASHED_ID_STRING_PREFIX_LENGTH]
                .copy_from_slice(&string.bytes().bytes()[0..{ Self::HASHED_ID_STRING_PREFIX_LENGTH }]);
            let hash_length =
                Self::write_hash(&mut bytes[Self::HASHED_ID_STRING_PREFIX_LENGTH..], hasher, string.bytes().bytes());
            Self::HASHED_ID_STRING_PREFIX_HASH_LENGTH + hash_length
        }
    }

    ///
    /// Encode the last byte by setting 0b0[7 bits representing length of the prefix characters]
    ///
    fn set_tail_inline_length(bytes: &mut [u8; Self::LENGTH], length: u8) {
        assert_eq!(0, length & Self::ENCODING_STRING_TAIL_IS_HASH_MASK); // ie < 128, high bit not set
                                                                         // because the high bit is not set, we already conform to the required mask of high bit = 0
        bytes[Self::ENCODING_STRING_TAIL_INDEX] = length;
    }

    pub fn is_inline(&self) -> bool {
        Self::is_inline_bytes(&self.bytes)
    }

    fn is_inline_bytes(bytes: &[u8]) -> bool {
        bytes[Self::ENCODING_STRING_TAIL_INDEX] & Self::ENCODING_STRING_TAIL_IS_HASH_MASK == 0
    }

    pub fn get_hash_disambiguator(&self) -> u8 {
        self.bytes[Self::ENCODING_STRING_TAIL_INDEX] & !Self::ENCODING_STRING_TAIL_IS_HASH_MASK
    }

    pub fn get_inline_string_bytes(&self) -> StringBytes<'static, 16> {
        debug_assert!(self.is_inline());
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        let inline_string_length = self.get_inline_length();
        bytes.bytes_mut()[0..inline_string_length as usize]
            .copy_from_slice(&self.bytes[0..inline_string_length as usize]);
        bytes.truncate(inline_string_length as usize);
        StringBytes::new(Bytes::Array(bytes))
    }

    pub fn get_inline_length(&self) -> u8 {
        debug_assert!(self.is_inline());
        self.bytes[Self::ENCODING_STRING_TAIL_INDEX]
    }

    pub fn get_hash_prefix(&self) -> [u8; Self::HASHED_ID_STRING_PREFIX_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.bytes[0..Self::HASHED_ID_STRING_PREFIX_LENGTH]).try_into().unwrap()
    }

    pub fn get_hash_hash(&self) -> [u8; Self::HASHED_ID_HASH_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.bytes[Self::HASHED_ID_STRING_PREFIX_LENGTH..Self::HASHED_ID_STRING_PREFIX_HASH_LENGTH])
            .try_into()
            .unwrap()
    }

    pub fn get_hash_prefix_hash(&self) -> [u8; Self::HASHED_ID_STRING_PREFIX_HASH_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.bytes[0..Self::HASHED_ID_STRING_PREFIX_HASH_LENGTH]).try_into().unwrap()
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }

    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl HashedID<{ StringAttributeID::HASHED_ID_HASH_LENGTH + 1 }> for StringAttributeID {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const FIXED_WIDTH_KEYS: bool = true;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct StructAttributeID {
    bytes: [u8; Self::LENGTH],
}

impl StructAttributeID {

    // TODO: update
    // const VALUE_TYPE_LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH;
    // const VALUE_LENGTH: usize = ValueEncodingLength::SHORT_LENGTH;
    // pub(crate) const LENGTH: usize = Self::VALUE_TYPE_LENGTH + Self::VALUE_LENGTH;
    // pub const HASH_LENGTH: usize = Self::VALUE_LENGTH - 1;
    // const TAIL_INDEX: usize = Self::LENGTH - 1;
    //
    // const VALUE_TYPE: ValueTypeCategory = ValueTypeCategory::Struct;
    //
    //
    pub(crate) const LENGTH: usize = AttributeIDLength::SHORT_LENGTH;
    pub const HASH_LENGTH: usize = { Self::LENGTH - 1 };

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }

    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn build_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        struct_bytes: StructBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Self, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        let attribute_prefix = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, type_id);
        let existing_or_new = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            Bytes::Array(ByteArray::<4>::copy_concat([
                attribute_prefix.bytes(),
                &ValueTypeCategory::Struct.to_bytes(),
            ])),
            struct_bytes.bytes().bytes(),
        )?;

        let (Either::First(hashed_id) | Either::Second(hashed_id)) = existing_or_new;

        let mut bytes = [0; Self::LENGTH];
        bytes[..Self::VALUE_TYPE_LENGTH].copy_from_slice(&Self::VALUE_TYPE.to_bytes());
        bytes[Self::VALUE_TYPE_LENGTH..].copy_from_slice(&hashed_id);
        Ok(Self { bytes })
    }

    pub(crate) fn find_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        struct_bytes: StructBytes<'_, INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Option<Self>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        let key_without_hash = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, type_id);
        let existing_or_new = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            key_without_hash.into_bytes().bytes(),
            struct_bytes.bytes().bytes(),
        )?;

        match existing_or_new {
            Either::First(hashed_id) => {
                debug_assert!(
                    hashed_id[Self::HASH_DISAMBIGUATOR_BYTE_INDEX] & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG != 0
                );
                let mut bytes = [0; Self::LENGTH];
                bytes[..Self::VALUE_TYPE_LENGTH].copy_from_slice(&Self::VALUE_TYPE.to_bytes());
                bytes[Self::VALUE_TYPE_LENGTH..].copy_from_slice(&hashed_id);
                Ok(Some(Self { bytes }))
            }
            Either::Second(_) => Ok(None),
        }
    }

    // write the deterministic ID prefix for the provided struct value, and return the length of the prefix written
    pub(crate) fn write_hashed_id_deterministic_prefix<const INLINE_LENGTH: usize>(
        struct_bytes: StructBytes<'_, INLINE_LENGTH>,
        hasher: &impl Fn(&[u8]) -> u64,
        bytes: &mut [u8],
    ) -> usize {
        Self::write_hash(bytes, hasher, struct_bytes.bytes().bytes())
    }

    pub fn get_hash_hash(&self) -> [u8; Self::HASH_LENGTH] {
        self.bytes[0..Self::HASH_LENGTH].try_into().unwrap()
        // TODO: update
        // self.bytes[Self::VALUE_TYPE_LENGTH..][..Self::HASH_LENGTH].try_into().unwrap()
    }

    pub fn get_hash_disambiguator(&self) -> u8 {
        self.bytes[Self::HASH_DISAMBIGUATOR_BYTE_INDEX] & !Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
    }

    pub(crate) const fn is_inlineable() -> bool {
        false
    }
}

impl HashedID<{ StructAttributeID::HASH_LENGTH + 1 }> for StructAttributeID {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::Data;
    const FIXED_WIDTH_KEYS: bool = true;
}

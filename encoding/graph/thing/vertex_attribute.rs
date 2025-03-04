/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, marker::PhantomData, ops::Range, sync::Arc};

use bytes::{byte_array::ByteArray, util::HexBytesFormatter, Bytes};
use error::unimplemented_feature;
use primitive::either::Either;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{
    key_value::{StorageKey, StorageKeyReference},
    keyspace::{KeyspaceId, KeyspaceSet},
    snapshot::{iterator::SnapshotIteratorError, ReadableSnapshot},
};

use crate::{
    graph::{
        common::value_hasher::HashedID,
        thing::{ThingVertex, THING_VERTEX_LENGTH_PREFIX_TYPE},
        type_::vertex::TypeID,
        Typed,
    },
    layout::prefix::{Prefix, PrefixID},
    value::{
        boolean_bytes::BooleanBytes,
        date_bytes::DateBytes,
        date_time_bytes::DateTimeBytes,
        date_time_tz_bytes::DateTimeTZBytes,
        decimal_bytes::DecimalBytes,
        double_bytes::DoubleBytes,
        duration_bytes::DurationBytes,
        integer_bytes::IntegerBytes,
        string_bytes::StringBytes,
        struct_bytes::StructBytes,
        value::Value,
        value_type::{ValueType, ValueTypeBytes, ValueTypeCategory},
        ValueEncodable,
    },
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct AttributeVertex {
    type_id: TypeID,
    attribute_id: AttributeID,
}

impl AttributeVertex {
    pub const PREFIX: Prefix = Prefix::VertexAttribute;
    pub const MAX_LENGTH: usize = PrefixID::LENGTH + TypeID::LENGTH + ValueEncodingLength::LONG_LENGTH;

    pub const fn new(type_id: TypeID, attribute_id: AttributeID) -> Self {
        Self { type_id, attribute_id }
    }

    pub fn decode(bytes: &[u8]) -> Self {
        debug_assert!(bytes[Self::INDEX_PREFIX] == Self::PREFIX.prefix_id().byte);
        debug_assert!(bytes.len() > THING_VERTEX_LENGTH_PREFIX_TYPE);
        let type_id = TypeID::decode(bytes[Self::RANGE_TYPE_ID].try_into().unwrap());
        let attribute_id = AttributeID::new(&bytes[Self::RANGE_TYPE_ID.end..]);
        Self { type_id, attribute_id }
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        if !Self::is_attribute_bytes(bytes) {
            return None;
        }
        Some(Self::decode(bytes))
    }

    pub fn build_or_prefix_for_value(
        type_id: TypeID,
        value: Value<'_>,
        large_value_hasher: &impl Fn(&[u8]) -> u64,
    ) -> Either<Self, StorageKey<'static, BUFFER_KEY_INLINE>> {
        // preallocate upper bound length and then truncate later
        let mut bytes = ByteArray::zeros(THING_VERTEX_LENGTH_PREFIX_TYPE + AttributeID::max_length());
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.to_bytes());
        let keyspace = Self::keyspace_for_category(value.value_type().category());
        let (id_length, is_complete) = AttributeID::write_deterministic_value_or_prefix(
            &mut bytes[Self::RANGE_TYPE_ID.end..],
            value,
            large_value_hasher,
        );
        bytes.truncate(Self::RANGE_TYPE_ID.end + id_length);
        if is_complete {
            Either::First(Self::decode(bytes.as_ref()))
        } else {
            Either::Second(StorageKey::new(keyspace, Bytes::Array(bytes)))
        }
    }

    pub(crate) fn write_prefix_type_attribute_id(bytes: &mut [u8], type_id: TypeID, attribute_id_part: &[u8]) -> usize {
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&type_id.to_bytes());
        bytes[Self::range_for_attribute_id(attribute_id_part.len())].copy_from_slice(attribute_id_part);
        Self::RANGE_TYPE_ID.end + attribute_id_part.len()
    }

    pub fn is_attribute_vertex(storage_key: StorageKeyReference<'_>) -> bool {
        Self::is_valid_keyspace(storage_key.keyspace_id()) && Self::is_attribute_bytes(storage_key.bytes())
    }

    fn is_attribute_bytes(bytes: &[u8]) -> bool {
        !bytes.is_empty() && bytes[Self::INDEX_PREFIX] == Prefix::VertexAttribute.prefix_id().byte
    }

    pub fn value_type_category(&self) -> ValueTypeCategory {
        self.attribute_id().value_type_category()
    }

    pub fn attribute_id(&self) -> AttributeID {
        self.attribute_id
    }

    pub(crate) fn is_category_short_encoding(value_type_category: ValueTypeCategory) -> bool {
        AttributeID::value_type_encoded_value_length(value_type_category).is_short()
    }

    pub(crate) fn is_short_encoding(&self) -> bool {
        Self::is_category_short_encoding(self.value_type_category())
    }

    fn range_of_attribute_id(&self) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..self.len()
    }

    fn range_for_attribute_id(id_length: usize) -> Range<usize> {
        Self::RANGE_TYPE_ID.end..Self::RANGE_TYPE_ID.end + id_length
    }

    pub(crate) fn len(&self) -> usize {
        PrefixID::LENGTH + TypeID::LENGTH + self.attribute_id.length()
    }

    pub fn keyspace_for_is_short(is_short_encoding: bool) -> EncodingKeyspace {
        if is_short_encoding {
            EncodingKeyspace::DefaultOptimisedPrefix11
        } else {
            EncodingKeyspace::OptimisedPrefix17
        }
    }

    pub fn keyspace_for_category(value_type_category: ValueTypeCategory) -> EncodingKeyspace {
        Self::keyspace_for_is_short(AttributeID::value_type_encoded_value_length(value_type_category).is_short())
    }

    pub(crate) fn keyspace(&self) -> EncodingKeyspace {
        Self::keyspace_for_is_short(self.is_short_encoding())
    }

    pub(crate) fn is_valid_keyspace(keyspace_id: KeyspaceId) -> bool {
        keyspace_id == EncodingKeyspace::DefaultOptimisedPrefix11.id()
            || keyspace_id == EncodingKeyspace::OptimisedPrefix17.id()
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for AttributeVertex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut bytes = [0; BUFFER_KEY_INLINE];
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::RANGE_TYPE_ID].copy_from_slice(&self.type_id.to_bytes());
        bytes[Self::range_for_attribute_id(self.attribute_id.length())].copy_from_slice(self.attribute_id.bytes());
        Bytes::Array(ByteArray::inline(bytes, self.len()))
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for AttributeVertex {}

impl Typed<BUFFER_KEY_INLINE> for AttributeVertex {
    fn type_id_(&self) -> TypeID {
        self.type_id
    }
}

impl ThingVertex for AttributeVertex {
    const FIXED_WIDTH_ENCODING: bool = false;

    fn decode(bytes: &[u8]) -> Self {
        AttributeVertex::decode(bytes)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for AttributeVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        self.keyspace()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ValueEncodingLength {
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

    const fn is_short(&self) -> bool {
        matches!(self, Self::Short)
    }

    const fn max_length() -> usize {
        // TODO: this is brittle - ideally we'd compute this over the max of the enum variants
        Self::LONG_LENGTH
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttributeID {
    // WARNING: Changing order of enum will change Ord and `MIN`! This must align with the storage encoding
    Boolean(BooleanAttributeID),
    Integer(IntegerAttributeID),
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
    pub const MIN: AttributeID = Self::Boolean(BooleanAttributeID::MIN);

    pub fn new(bytes: &[u8]) -> Self {
        let &[prefix, ..] = bytes else { unreachable!("empty value bytes") };
        match ValueTypeCategory::from_bytes([prefix]) {
            ValueTypeCategory::Boolean => Self::Boolean(BooleanAttributeID::new(bytes.try_into().unwrap())),
            ValueTypeCategory::Integer => Self::Integer(IntegerAttributeID::new(bytes.try_into().unwrap())),
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
            ValueType::Integer => Self::Integer(IntegerAttributeID::build(value.encode_integer())),
            ValueType::Double => Self::Double(DoubleAttributeID::build(value.encode_double())),
            ValueType::Decimal => Self::Decimal(DecimalAttributeID::build(value.encode_decimal())),
            ValueType::Date => Self::Date(DateAttributeID::build(value.encode_date())),
            ValueType::DateTime => Self::DateTime(DateTimeAttributeID::build(value.encode_date_time())),
            ValueType::DateTimeTZ => Self::DateTimeTZ(DateTimeTZAttributeID::build(value.encode_date_time_tz())),
            ValueType::Duration => Self::Duration(DurationAttributeID::build(value.encode_duration())),
            ValueType::String => Self::String(StringAttributeID::build_inline_id(value.encode_string::<256>())),
            ValueType::Struct(_) => unimplemented_feature!(Structs),
        }
    }

    pub fn write_deterministic_value_or_prefix(
        bytes: &mut [u8],
        value: Value<'_>,
        large_value_hasher: &impl Fn(&[u8]) -> u64,
    ) -> (usize, bool) {
        debug_assert!(bytes.len() >= AttributeID::max_length());
        match value.value_type().category() {
            ValueTypeCategory::Boolean => (BooleanAttributeID::write(value.encode_boolean(), bytes), true),
            ValueTypeCategory::Integer => (IntegerAttributeID::write(value.encode_integer(), bytes), true),
            ValueTypeCategory::Double => (DoubleAttributeID::write(value.encode_double(), bytes), true),
            ValueTypeCategory::Decimal => (DecimalAttributeID::write(value.encode_decimal(), bytes), true),
            ValueTypeCategory::Date => (DateAttributeID::write(value.encode_date(), bytes), true),
            ValueTypeCategory::DateTime => (DateTimeAttributeID::write(value.encode_date_time(), bytes), true),
            ValueTypeCategory::DateTimeTZ => (DateTimeTZAttributeID::write(value.encode_date_time_tz(), bytes), true),
            ValueTypeCategory::Duration => (DurationAttributeID::write(value.encode_duration(), bytes), true),
            ValueTypeCategory::String => (
                StringAttributeID::write_deterministic_prefix(value.encode_string::<64>(), large_value_hasher, bytes),
                false,
            ),
            ValueTypeCategory::Struct => (
                StructAttributeID::write_hashed_id_deterministic_prefix(
                    value.encode_struct::<64>(),
                    large_value_hasher,
                    bytes,
                ),
                false,
            ),
        }
    }

    ///
    /// Return true if values in the value type are always fully encoded in the ID
    /// Return false if the values are hashed or incomplete and may require a secondary lookup
    ///
    pub fn is_inlineable(value: impl ValueEncodable) -> bool {
        match value.value_type() {
            ValueType::Boolean => BooleanAttributeID::is_inlineable(),
            ValueType::Integer => IntegerAttributeID::is_inlineable(),
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
            AttributeID::Integer(integer_id) => integer_id.bytes_ref(),
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

    pub fn value_type_encoding_length(value_type_category: ValueTypeCategory) -> usize {
        match value_type_category {
            ValueTypeCategory::Boolean => BooleanAttributeID::LENGTH,
            ValueTypeCategory::Integer => IntegerAttributeID::LENGTH,
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

    pub(crate) fn length(&self) -> usize {
        Self::value_type_encoding_length(self.value_type_category())
    }

    pub(crate) const fn value_type_encoded_value_length(value_type_category: ValueTypeCategory) -> ValueEncodingLength {
        match value_type_category {
            ValueTypeCategory::Boolean => BooleanAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Integer => IntegerAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Double => DoubleAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Decimal => DecimalAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Date => DateAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::DateTime => DateTimeAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::DateTimeTZ => DateTimeTZAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Duration => DurationAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::String => StringAttributeID::VALUE_LENGTH_ID,
            ValueTypeCategory::Struct => StructAttributeID::VALUE_LENGTH_ID,
        }
    }

    pub(crate) fn encoded_value_length(&self) -> ValueEncodingLength {
        Self::value_type_encoded_value_length(self.value_type_category())
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

    pub fn unwrap_integer(self) -> IntegerAttributeID {
        match self {
            AttributeID::Integer(integer_id) => integer_id,
            _ => panic!("Cannot unwrap Integer ID from non-integer attribute ID."),
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
            AttributeID::Integer(_) => ValueTypeCategory::Integer,
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

impl fmt::Display for AttributeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &HexBytesFormatter::borrowed(self.bytes()))
    }
}

pub trait InlineEncodableAttributeID {
    const ENCODED_LENGTH_ID: ValueEncodingLength;
    const ENCODED_LENGTH: usize = Self::ENCODED_LENGTH_ID.length();
    const VALUE_TYPE: ValueType;

    fn bytes_ref(&self) -> &[u8];

    fn read(bytes: &[u8]) -> Self;
}

pub type BooleanAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + BooleanBytes::ENCODED_LENGTH }, BooleanBytes>;
pub type IntegerAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + IntegerBytes::ENCODED_LENGTH }, IntegerBytes>;
pub type DoubleAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DoubleBytes::ENCODED_LENGTH }, DoubleBytes>;
pub type DecimalAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DecimalBytes::ENCODED_LENGTH }, DecimalBytes>;
pub type DateAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DateBytes::ENCODED_LENGTH }, DateBytes>;
pub type DateTimeAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DateTimeBytes::ENCODED_LENGTH }, DateTimeBytes>;
pub type DateTimeTZAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DateTimeTZBytes::ENCODED_LENGTH }, DateTimeTZBytes>;
pub type DurationAttributeID =
    InlinePrimitiveID<{ ValueTypeBytes::CATEGORY_LENGTH + DurationBytes::ENCODED_LENGTH }, DurationBytes>;

// note: const parameter is a workaround for not being able to use EncodedBytesTypeLL:INLINE_BYTES_LENGTH directly yet
//       https://github.com/rust-lang/rust/issues/60551
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct InlinePrimitiveID<const VALUE_WITH_PREFIX_LENGTH: usize, EncodedBytesType: InlineEncodableAttributeID> {
    bytes: [u8; VALUE_WITH_PREFIX_LENGTH],
    _ph: PhantomData<EncodedBytesType>,
}

impl<const LENGTH_VALUE_WITH_PREFIX: usize, EncodedBytesType: InlineEncodableAttributeID>
    InlinePrimitiveID<LENGTH_VALUE_WITH_PREFIX, EncodedBytesType>
{
    const VALUE_TYPE_LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH;
    const VALUE_LENGTH_ID: ValueEncodingLength = EncodedBytesType::ENCODED_LENGTH_ID;
    const LENGTH: usize = LENGTH_VALUE_WITH_PREFIX;
    const MIN: Self = Self::new([0; LENGTH_VALUE_WITH_PREFIX]);

    pub const fn new(bytes: [u8; LENGTH_VALUE_WITH_PREFIX]) -> Self {
        // assert ensures generics are used correctly (see above note with link to rust-lang issue)
        debug_assert!(LENGTH_VALUE_WITH_PREFIX == EncodedBytesType::ENCODED_LENGTH + Self::VALUE_TYPE_LENGTH);
        Self { bytes, _ph: PhantomData }
    }

    pub(crate) fn build(value: EncodedBytesType) -> Self {
        let mut bytes = [0; LENGTH_VALUE_WITH_PREFIX];
        Self::write(value, &mut bytes);
        Self::new(bytes)
    }

    // write value bytes to the start of the byte slice and return the number of bytes written
    pub(crate) fn write(value: EncodedBytesType, bytes: &mut [u8]) -> usize {
        debug_assert!(bytes.len() >= Self::VALUE_TYPE_LENGTH + value.bytes_ref().len(), "Slice too short");
        bytes[0..Self::VALUE_TYPE_LENGTH].copy_from_slice(&EncodedBytesType::VALUE_TYPE.category().to_bytes());
        bytes[Self::VALUE_TYPE_LENGTH..Self::VALUE_TYPE_LENGTH + value.bytes_ref().len()]
            .copy_from_slice(value.bytes_ref());
        Self::VALUE_TYPE_LENGTH + value.bytes_ref().len()
    }

    pub(crate) const fn is_inlineable() -> bool {
        true
    }

    pub fn bytes(&self) -> [u8; LENGTH_VALUE_WITH_PREFIX] {
        self.bytes
    }

    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }

    pub fn read(&self) -> EncodedBytesType {
        EncodedBytesType::read(&self.bytes_ref()[Self::VALUE_TYPE_LENGTH..])
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
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringAttributeID {
    bytes: [u8; Self::LENGTH],
}

impl StringAttributeID {
    const VALUE_TYPE_LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH;
    const VALUE_LENGTH_ID: ValueEncodingLength = ValueEncodingLength::Long;
    const LENGTH: usize = Self::VALUE_TYPE_LENGTH + Self::VALUE_LENGTH_ID.length();

    const INLINE_OR_PREFIXED_HASH_LENGTH: usize = Self::VALUE_LENGTH_ID.length() - 1;

    pub const HASHED_PREFIX_LENGTH: usize = Self::INLINE_OR_PREFIXED_HASH_LENGTH - Self::HASHED_HASH_LENGTH;
    pub const HASHED_HASH_LENGTH: usize = 8;

    pub const HASHED_PREFIX_RANGE: Range<usize> =
        Self::VALUE_TYPE_LENGTH..Self::VALUE_TYPE_LENGTH + Self::HASHED_PREFIX_LENGTH;
    pub const HASHED_HASH_RANGE: Range<usize> =
        Self::HASHED_PREFIX_RANGE.end..Self::HASHED_PREFIX_RANGE.end + Self::HASHED_HASH_LENGTH;
    pub const HASHED_DISAMBIGUATED_HASH_RANGE: Range<usize> =
        Self::HASHED_HASH_RANGE.start..Self::HASHED_HASH_RANGE.end + 1;
    // const HASHED_PREFIX_HASH_RANGE: Range<usize> =
    //     Self::VALUE_TYPE_LENGTH..Self::VALUE_TYPE_LENGTH + Self::HASHED_ENCODING_LENGTH;

    const TAIL_IS_HASH_MASK: u8 = 0b1000_0000;
    const TAIL_INDEX: usize = Self::LENGTH - 1;

    // pub const HASHED_ID_STRING_PREFIX_LENGTH: usize =
    //     { StringAttributeID::INLINE_OR_PREFIXED_HASH_LENGTH - StringAttributeID::HASH_LENGTH };

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub(crate) fn is_inlineable<const INLINE_LENGTH: usize>(string: StringBytes<INLINE_LENGTH>) -> bool {
        string.len() <= Self::INLINE_OR_PREFIXED_HASH_LENGTH
    }

    pub(crate) fn build_inline_id<const INLINE_LENGTH: usize>(string: StringBytes<INLINE_LENGTH>) -> Self {
        debug_assert!(Self::is_inlineable(string.as_reference()));
        let mut bytes = [0u8; Self::LENGTH];
        Self::write_inline_id(&mut bytes, string);
        Self::new(bytes)
    }

    // write the string bytes to the byte slice, and set the last byte to the string length
    pub(crate) fn write_inline_id<const INLINE_LENGTH: usize>(
        bytes: &mut [u8; Self::LENGTH],
        string: StringBytes<INLINE_LENGTH>,
    ) {
        debug_assert!(Self::is_inlineable(string.as_reference()));
        let [prefix, value_bytes @ ..] = bytes;
        std::slice::from_mut(prefix).copy_from_slice(&ValueTypeCategory::String.to_bytes());
        value_bytes[..string.len()].copy_from_slice(string.bytes());
        Self::set_tail_inline_length(bytes, string.len() as u8);
    }

    pub fn get_inline_id_value(&self) -> StringBytes<{ Self::INLINE_OR_PREFIXED_HASH_LENGTH }> {
        debug_assert!(self.is_inline());
        let value_bytes = &self.bytes[Self::VALUE_TYPE_LENGTH..];
        let mut bytes = ByteArray::zeros(Self::INLINE_OR_PREFIXED_HASH_LENGTH);
        let inline_string_length = self.get_inline_length() as usize;
        bytes[0..inline_string_length].copy_from_slice(&value_bytes[0..inline_string_length]);
        bytes.truncate(inline_string_length);
        StringBytes::new(Bytes::Array(bytes))
    }

    pub(crate) fn build_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<INLINE_LENGTH>,
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
        string: StringBytes<INLINE_LENGTH>,
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

    fn build_or_find_hashed_id<const INLINE_LENGTH: usize, Snapshot>(
        type_id: TypeID,
        string: StringBytes<INLINE_LENGTH>,
        snapshot: &Snapshot,
        hasher: &impl Fn(&[u8]) -> u64,
    ) -> Result<Either<Self, Self>, Arc<SnapshotIteratorError>>
    where
        Snapshot: ReadableSnapshot,
    {
        debug_assert!(!Self::is_inlineable(string.as_reference()));

        let mut id_prefix = [0; Self::VALUE_TYPE_LENGTH + Self::HASHED_PREFIX_LENGTH];
        id_prefix[0..Self::VALUE_TYPE_LENGTH].copy_from_slice(&ValueTypeCategory::String.to_bytes());
        id_prefix[Self::HASHED_PREFIX_RANGE].copy_from_slice(&string.bytes()[0..{ Self::HASHED_PREFIX_LENGTH }]);

        // generate full AttributeVertex that we can use to check for existing hashed values in the same type
        let mut attribute_bytes = [0; AttributeVertex::RANGE_TYPE_ID.end + Self::LENGTH];
        let prefix_length = AttributeVertex::write_prefix_type_attribute_id(&mut attribute_bytes, type_id, &id_prefix);
        let disambiguated_hash = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            AttributeVertex::keyspace_for_category(ValueTypeCategory::String),
            &attribute_bytes[0..prefix_length],
            string.bytes(),
        )?;
        let string_id = match disambiguated_hash {
            Either::First(disambiguated_hash) | Either::Second(disambiguated_hash) => {
                debug_assert!(
                    disambiguated_hash[Self::HASH_DISAMBIGUATOR_BYTE_INDEX]
                        & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
                        != 0
                );
                let mut string_id_bytes = [0; Self::LENGTH];
                string_id_bytes[0..Self::VALUE_TYPE_LENGTH].copy_from_slice(&ValueTypeCategory::String.to_bytes());
                string_id_bytes[Self::HASHED_PREFIX_RANGE]
                    .copy_from_slice(&string.bytes()[0..Self::HASHED_PREFIX_LENGTH]);
                string_id_bytes[Self::HASHED_DISAMBIGUATED_HASH_RANGE].copy_from_slice(&disambiguated_hash);
                Self { bytes: string_id_bytes }
            }
        };
        match disambiguated_hash {
            Either::First(_) => Ok(Either::First(string_id)),
            Either::Second(_) => Ok(Either::Second(string_id)),
        }
    }

    // write the deterministic prefix of the hash ID, and return the length of the prefix written
    pub(crate) fn write_deterministic_prefix<const INLINE_LENGTH: usize>(
        string: StringBytes<INLINE_LENGTH>,
        hasher: &impl Fn(&[u8]) -> u64,
        bytes: &mut [u8],
    ) -> usize {
        debug_assert!(bytes.len() >= Self::LENGTH);
        if Self::is_inlineable(string.as_reference()) {
            let bytes_range = &mut bytes[0..Self::LENGTH];
            Self::write_inline_id(bytes_range.try_into().unwrap(), string);
            Self::LENGTH
        } else {
            bytes[0..Self::VALUE_TYPE_LENGTH].copy_from_slice(&ValueTypeCategory::String.to_bytes());
            bytes[Self::HASHED_PREFIX_RANGE].copy_from_slice(&string.bytes()[0..{ Self::HASHED_PREFIX_LENGTH }]);
            let hash_length = Self::write_hash(&mut bytes[Self::HASHED_HASH_RANGE], hasher, string.bytes());
            Self::VALUE_TYPE_LENGTH + Self::HASHED_PREFIX_LENGTH + hash_length
        }
    }

    ///
    /// Encode the last byte by setting 0b0[7 bits representing length of the prefix characters]
    ///
    fn set_tail_inline_length(bytes: &mut [u8; Self::LENGTH], length: u8) {
        debug_assert!(bytes.len() == Self::LENGTH);
        assert_eq!(0, length & Self::TAIL_IS_HASH_MASK); // ie < 128, high bit not set
                                                         // because the high bit is not set, we already conform to the required mask of high bit = 0
        bytes[Self::TAIL_INDEX] = length;
    }

    pub fn is_inline(&self) -> bool {
        Self::is_inline_bytes(&self.bytes)
    }

    fn is_inline_bytes(bytes: &[u8]) -> bool {
        bytes[Self::TAIL_INDEX] & Self::TAIL_IS_HASH_MASK == 0
    }

    pub fn get_hash_disambiguator(&self) -> u8 {
        self.bytes[Self::TAIL_INDEX] & !Self::TAIL_IS_HASH_MASK
    }

    pub fn get_inline_string_bytes(&self) -> StringBytes<16> {
        debug_assert!(self.is_inline());
        let mut bytes = ByteArray::zeros(Self::LENGTH);
        let inline_string_length = self.get_inline_length();
        bytes[0..inline_string_length as usize].copy_from_slice(&self.bytes[0..inline_string_length as usize]);
        bytes.truncate(inline_string_length as usize);
        StringBytes::new(Bytes::Array(bytes))
    }

    pub fn get_inline_length(&self) -> u8 {
        debug_assert!(self.is_inline());
        self.bytes[Self::TAIL_INDEX]
    }

    pub fn get_hash_prefix(&self) -> [u8; Self::HASHED_PREFIX_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.bytes[Self::HASHED_PREFIX_RANGE]).try_into().unwrap()
    }

    pub fn get_hash_hash(&self) -> [u8; Self::HASHED_HASH_LENGTH] {
        debug_assert!(!self.is_inline());
        (&self.bytes[Self::HASHED_HASH_RANGE]).try_into().unwrap()
    }

    pub fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }

    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl HashedID<{ StringAttributeID::HASHED_HASH_LENGTH + 1 }> for StringAttributeID {
    const FIXED_WIDTH_KEYS: bool = true;
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructAttributeID {
    bytes: [u8; Self::LENGTH],
}

impl StructAttributeID {
    const VALUE_LENGTH_ID: ValueEncodingLength = ValueEncodingLength::Short;
    pub(crate) const LENGTH: usize = ValueTypeBytes::CATEGORY_LENGTH + Self::VALUE_LENGTH_ID.length();
    pub const HASH_LENGTH: usize = Self::VALUE_LENGTH_ID.length() - 1;
    const TAIL_INDEX: usize = Self::LENGTH - 1;

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
        let keyspace = AttributeVertex::keyspace_for_category(ValueTypeCategory::Struct);
        let attribute_prefix = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, type_id, keyspace);
        let existing_or_new = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            keyspace,
            &ByteArray::<{ THING_VERTEX_LENGTH_PREFIX_TYPE + ValueTypeBytes::CATEGORY_LENGTH }>::copy_concat([
                attribute_prefix.bytes(),
                &ValueTypeCategory::Struct.to_bytes(),
            ]),
            struct_bytes.bytes(),
        )?;

        let (Either::First(disambiguated_hash) | Either::Second(disambiguated_hash)) = existing_or_new;

        let mut bytes = [0; Self::LENGTH];
        bytes[0..ValueTypeBytes::CATEGORY_LENGTH].copy_from_slice(&ValueTypeCategory::Struct.to_bytes());
        bytes[ValueTypeBytes::CATEGORY_LENGTH..Self::LENGTH].copy_from_slice(&disambiguated_hash);
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
        let keyspace = AttributeVertex::keyspace_for_category(ValueTypeCategory::Struct);
        let attribute_prefix = AttributeVertex::build_prefix_type(Prefix::VertexAttribute, type_id, keyspace);
        let existing_or_new = Self::find_existing_or_next_disambiguated_hash(
            snapshot,
            hasher,
            keyspace,
            &ByteArray::<{ THING_VERTEX_LENGTH_PREFIX_TYPE + ValueTypeBytes::CATEGORY_LENGTH }>::copy_concat([
                attribute_prefix.bytes(),
                &ValueTypeCategory::Struct.to_bytes(),
            ]),
            struct_bytes.bytes(),
        )?;

        match existing_or_new {
            Either::First(disambiguated_hash) => {
                debug_assert!(
                    disambiguated_hash[Self::HASH_DISAMBIGUATOR_BYTE_INDEX]
                        & Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
                        != 0
                );
                let mut bytes = [0; Self::LENGTH];
                bytes[0..ValueTypeBytes::CATEGORY_LENGTH].copy_from_slice(&ValueTypeCategory::Struct.to_bytes());
                bytes[ValueTypeBytes::CATEGORY_LENGTH..Self::LENGTH].copy_from_slice(&disambiguated_hash);
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
        Self::write_hash(bytes, hasher, struct_bytes.bytes())
    }

    pub fn get_hash_hash(&self) -> [u8; Self::HASH_LENGTH] {
        self.bytes[ValueTypeBytes::CATEGORY_LENGTH..ValueTypeBytes::CATEGORY_LENGTH + Self::HASH_LENGTH]
            .try_into()
            .unwrap()
    }

    pub fn get_hash_disambiguator(&self) -> u8 {
        self.bytes[Self::TAIL_INDEX] & !Self::HASH_DISAMBIGUATOR_BYTE_IS_HASH_FLAG
    }

    pub(crate) const fn is_inlineable() -> bool {
        false
    }
}

impl HashedID<{ StructAttributeID::HASH_LENGTH + 1 }> for StructAttributeID {
    const FIXED_WIDTH_KEYS: bool = true;
}

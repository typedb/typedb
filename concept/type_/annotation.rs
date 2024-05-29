/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use serde::{Deserialize, Serialize};
use bytes::byte_array::ByteArray;
use bytes::byte_reference::ByteReference;
use bytes::Bytes;
use encoding::AsBytes;
use encoding::graph::type_::property::{EncodableTypeEdgeProperty, EncodableTypeVertexProperty};
use encoding::layout::infix::Infix;
use encoding::layout::infix::Infix::{PropertyAnnotationAbstract, PropertyAnnotationDistinct, PropertyAnnotationIndependent, PropertyAnnotationKey};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Annotation {
    Abstract(AnnotationAbstract),
    Distinct(AnnotationDistinct),
    Independent(AnnotationIndependent),
    Unique(AnnotationUnique), TODO: Needs annotation Unique
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
    Regex(AnnotationRegex),
}


#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationAbstract;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationDistinct;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationUnique;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationKey;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationIndependent;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationCardinality {
    // ##########################################################################
    // ###### WARNING: any changes here may break backwards compatibility! ######
    // ##########################################################################
    start_inclusive: u64,
    end_inclusive: Option<u64>,
}

impl AnnotationCardinality {
    pub const fn new(start_inclusive: u64, end_inclusive: Option<u64>) -> Self {
        Self { start_inclusive, end_inclusive }
    }

    pub fn is_valid(&self, count: u64) -> bool {
        self.start_inclusive <= count && (self.end_inclusive.is_none() || count <= self.end_inclusive.unwrap())
    }

    pub fn start(&self) -> u64 {
        self.start_inclusive
    }

    pub fn end(&self) -> Option<u64> {
        self.end_inclusive
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct AnnotationRegex {
    regex: String,
}

impl AnnotationRegex {
    pub const fn new(regex: String) -> Self {
        Self { regex }
    }

    pub fn regex(&self) -> &str {
        &self.regex
    }
}

macro_rules! trivial_type_vertex_annotation_encoder {
    ($annotation:ident, $infix:ident) => {
        impl<'a> EncodableTypeVertexProperty<'a> for $annotation {
            const INFIX: Infix = $infix;

            fn decode_value<'b>(value: ByteReference<'b>) -> $annotation {
                debug_assert!(value.bytes().is_empty());
                $annotation
            }

            fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

trivial_type_vertex_annotation_encoder!(AnnotationAbstract, PropertyAnnotationAbstract);
trivial_type_vertex_annotation_encoder!(AnnotationIndependent, PropertyAnnotationIndependent);
trivial_type_vertex_annotation_encoder!(AnnotationDistinct, PropertyAnnotationDistinct);

impl<'a> EncodableTypeVertexProperty<'a> for AnnotationRegex {
    const INFIX: Infix = Infix::PropertyAnnotationRegex;

    fn decode_value<'b>(value: ByteReference<'b>) -> AnnotationRegex {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        AnnotationRegex::new(std::str::from_utf8(value.bytes()).unwrap().to_owned())
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::copy(self.regex().as_bytes())))
    }
}

impl<'a> EncodableTypeVertexProperty<'a> for AnnotationCardinality {
    const INFIX: Infix = Infix::PropertyAnnotationCardinality;
    fn decode_value<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

macro_rules! trivial_type_edge_annotation_encoder {
    ($annotation:ident, $infix:ident) => {
        impl<'a> EncodableTypeEdgeProperty<'a> for $annotation {
            const INFIX: Infix = $infix;

            fn decode_value<'b>(value: ByteReference<'b>) -> $annotation {
                debug_assert!(value.bytes().is_empty());
                $annotation
            }

            fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
                None
            }
        }
    };
}

trivial_type_edge_annotation_encoder!(AnnotationDistinct, PropertyAnnotationDistinct);
trivial_type_edge_annotation_encoder!(AnnotationKey, PropertyAnnotationKey);

impl<'a> EncodableTypeEdgeProperty<'a> for AnnotationCardinality {
    const INFIX: Infix = Infix::PropertyAnnotationCardinality;
    fn decode_value<'b>(value: ByteReference<'b>) -> Self {
        // TODO this .unwrap() should be handled as an error
        // although it does indicate data corruption
        bincode::deserialize(value.bytes()).unwrap()
    }

    fn build_value(self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::copy(bincode::serialize(&self).unwrap().as_slice()))
    }
}

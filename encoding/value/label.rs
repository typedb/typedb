/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    hash::{Hash, Hasher},
};

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::{
    encoding::{LABEL_NAME_STRING_INLINE, LABEL_SCOPED_NAME_STRING_INLINE, LABEL_SCOPE_STRING_INLINE},
    snapshot::BUFFER_VALUE_INLINE,
};
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    graph::type_::property::TypeVertexPropertyEncoding, layout::infix::Infix, value::string_bytes::StringBytes,
};

#[derive(Clone)]
pub struct Label {
    // TODO dedup
    pub name: StringBytes<LABEL_NAME_STRING_INLINE>,
    pub scope: Option<StringBytes<LABEL_SCOPE_STRING_INLINE>>,
    pub scoped_name: StringBytes<LABEL_SCOPED_NAME_STRING_INLINE>,

    source_span: Option<Span>,
}

impl Label {
    pub fn parse_from_bytes<const INLINE_BYTES: usize>(string_bytes: StringBytes<INLINE_BYTES>) -> Label {
        let as_str = string_bytes.as_str();
        Self::parse_from(as_str, None)
    }

    pub fn parse_from(string: &str, source_span: Option<Span>) -> Label {
        let mut splits = string.split(':');
        let first = splits.next().unwrap();
        if let Some(second) = splits.next() {
            Self::build_scoped(second, first, source_span)
        } else {
            Self::build(first, source_span)
        }
    }

    pub fn build(name: &str, source_span: Option<Span>) -> Label {
        Label {
            name: StringBytes::build_owned(name),
            scope: None,
            scoped_name: StringBytes::build_owned(name),
            source_span,
        }
    }

    pub fn build_scoped(name: &str, scope: &str, source_span: Option<Span>) -> Label {
        let concatenated = format!("{}:{}", scope, name);
        Label {
            name: StringBytes::build_owned(name),
            scope: Some(StringBytes::build_owned(scope)),
            scoped_name: StringBytes::build_owned(concatenated.as_ref()),
            source_span,
        }
    }

    pub const fn new_static(name: &'static str) -> Label {
        Label {
            name: StringBytes::build_static_ref(name),
            scope: None,
            scoped_name: StringBytes::build_static_ref(name),
            source_span: None,
        }
    }

    pub const fn new_static_scoped(name: &'static str, scope: &'static str, scoped_name: &'static str) -> Label {
        if name.len() + scope.len() + 1 != scoped_name.len() {
            panic!("Provided scoped name has a different length to (name+scope+1).");
        }
        Label {
            name: StringBytes::build_static_ref(name),
            scope: Some(StringBytes::build_static_ref(scope)),
            scoped_name: StringBytes::build_static_ref(scoped_name),
            source_span: None,
        }
    }

    pub fn name(&self) -> StringBytes<LABEL_NAME_STRING_INLINE> {
        self.name.as_reference()
    }

    pub fn scope(&self) -> Option<StringBytes<LABEL_SCOPE_STRING_INLINE>> {
        self.scope.as_ref().map(|string_bytes| string_bytes.as_reference())
    }

    pub fn scoped_name(&self) -> StringBytes<LABEL_SCOPED_NAME_STRING_INLINE> {
        self.scoped_name.as_reference()
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl TypeVertexPropertyEncoding for Label {
    const INFIX: Infix = Infix::PropertyLabel;

    fn from_value_bytes(value: &[u8]) -> Self {
        let string_bytes = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(value));
        Label::parse_from_bytes(string_bytes)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'static, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::from(self.scoped_name().bytes())))
    }
}

impl Hash for Label {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.scoped_name.bytes())
    }
}

impl Eq for Label {}

impl PartialEq<Self> for Label {
    fn eq(&self, other: &Self) -> bool {
        self.scoped_name.eq(&other.scoped_name)
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.scoped_name().as_str().cmp(other.scoped_name().as_str())
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl StructuralEquality for Label {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(self.scoped_name.as_str())
    }

    fn equals(&self, other: &Self) -> bool {
        self.scoped_name == other.scoped_name
    }
}

impl fmt::Debug for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Label[{}]", self.scoped_name())
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.scoped_name().as_str())
    }
}

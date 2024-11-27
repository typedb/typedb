/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use bytes::{byte_array::ByteArray, Bytes};
use resource::constants::{
    encoding::{LABEL_NAME_STRING_INLINE, LABEL_SCOPED_NAME_STRING_INLINE, LABEL_SCOPE_STRING_INLINE},
    snapshot::BUFFER_VALUE_INLINE,
};
use structural_equality::StructuralEquality;

use crate::{
    graph::type_::property::TypeVertexPropertyEncoding, layout::infix::Infix, value::string_bytes::StringBytes,
};

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Label<'a> {
    pub name: StringBytes<'a, LABEL_NAME_STRING_INLINE>,
    pub scope: Option<StringBytes<'a, LABEL_SCOPE_STRING_INLINE>>,
    pub scoped_name: StringBytes<'a, LABEL_SCOPED_NAME_STRING_INLINE>,
}

impl<'a> Label<'a> {
    pub fn parse_from_bytes<const INLINE_BYTES: usize>(string_bytes: StringBytes<'a, INLINE_BYTES>) -> Label<'static> {
        let as_str = string_bytes.as_str();
        Self::parse_from(as_str)
    }

    pub fn parse_from(string: &str) -> Label<'static> {
        let mut splits = string.split(':');
        let first = splits.next().unwrap();
        if let Some(second) = splits.next() {
            Self::build_scoped(second, first)
        } else {
            Self::build(first)
        }
    }

    pub fn build(name: &str) -> Label<'static> {
        Label { name: StringBytes::build_owned(name), scope: None, scoped_name: StringBytes::build_owned(name) }
    }

    pub fn build_scoped(name: &str, scope: &str) -> Label<'static> {
        let concatenated = format!("{}:{}", scope, name);
        Label {
            name: StringBytes::build_owned(name),
            scope: Some(StringBytes::build_owned(scope)),
            scoped_name: StringBytes::build_owned(concatenated.as_ref()),
        }
    }

    pub const fn new_static(name: &'static str) -> Label<'static> {
        Label { name: StringBytes::build_ref(name), scope: None, scoped_name: StringBytes::build_ref(name) }
    }

    pub const fn new_static_scoped(
        name: &'static str,
        scope: &'static str,
        scoped_name: &'static str,
    ) -> Label<'static> {
        if name.len() + scope.len() + 1 != scoped_name.len() {
            panic!("Provided scoped name has a different length to (name+scope+1).");
        }
        Label {
            name: StringBytes::build_ref(name),
            scope: Some(StringBytes::build_ref(scope)),
            scoped_name: StringBytes::build_ref(scoped_name),
        }
    }

    pub fn name(&'a self) -> StringBytes<'a, LABEL_NAME_STRING_INLINE> {
        self.name.as_reference()
    }

    pub fn scope(&'a self) -> Option<StringBytes<'a, LABEL_SCOPE_STRING_INLINE>> {
        self.scope.as_ref().map(|string_bytes| string_bytes.as_reference())
    }

    pub fn scoped_name(&'a self) -> StringBytes<'a, LABEL_SCOPED_NAME_STRING_INLINE> {
        self.scoped_name.as_reference()
    }

    // TODO: replace all usages of &Label<'_> with Label<'_>, then update all call sites/usages to use .as_reference() instead of clone()
    pub fn as_reference(&self) -> Label<'_> {
        Label {
            name: self.name.as_reference(),
            scope: self.scope.as_ref().map(|string_bytes| string_bytes.as_reference()),
            scoped_name: self.scoped_name.as_reference(),
        }
    }

    pub fn into_owned(self) -> Label<'static> {
        Label {
            name: self.name.into_owned(),
            scope: self.scope.map(|string_bytes| string_bytes.into_owned()),
            scoped_name: self.scoped_name.into_owned(),
        }
    }
}

impl<'a> TypeVertexPropertyEncoding<'a> for Label<'a> {
    const INFIX: Infix = Infix::PropertyLabel;

    fn from_value_bytes(value: &[u8]) -> Self {
        let string_bytes = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(value));
        Label::parse_from_bytes(string_bytes)
    }

    fn to_value_bytes(&self) -> Option<Bytes<'a, BUFFER_VALUE_INLINE>> {
        Some(Bytes::Array(ByteArray::from(self.scoped_name().bytes())))
    }
}

impl<'a> Ord for Label<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.scoped_name().as_str().cmp(other.scoped_name().as_str())
    }
}

impl<'a> PartialOrd for Label<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> StructuralEquality for Label<'a> {
    fn hash(&self) -> u64 {
        self.scoped_name.as_str().hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.scoped_name == other.scoped_name
    }
}

impl<'a> fmt::Debug for Label<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Label[{}]", self.scoped_name())
    }
}

impl<'a> fmt::Display for Label<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.scoped_name().as_str())
    }
}

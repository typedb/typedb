/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::fmt;

use resource::constants::encoding::{
    LABEL_NAME_STRING_INLINE, LABEL_SCOPED_NAME_STRING_INLINE, LABEL_SCOPE_STRING_INLINE,
};

use crate::value::string::StringBytes;

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct Label<'a> {
    pub name: StringBytes<'a, LABEL_NAME_STRING_INLINE>,
    pub scope: Option<StringBytes<'a, LABEL_SCOPE_STRING_INLINE>>,
    pub scoped_name: StringBytes<'a, { LABEL_SCOPED_NAME_STRING_INLINE }>,
}

impl<'a> Label<'a> {
    pub fn parse_from<const INLINE_BYTES: usize>(string_bytes: StringBytes<'a, INLINE_BYTES>) -> Label<'static> {
        let as_str = string_bytes.decode();
        let mut splits = as_str.split(':');
        let first = splits.next().unwrap();
        if let Some(second) = splits.next() {
            Self::build_scoped(first, second)
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
        self.name.clone_as_ref()
    }

    pub fn scope(&'a self) -> Option<StringBytes<'a, LABEL_SCOPE_STRING_INLINE>> {
        self.scope.as_ref().map(|string_bytes| string_bytes.clone_as_ref())
    }

    pub fn scoped_name(&'a self) -> StringBytes<'a, LABEL_SCOPED_NAME_STRING_INLINE> {
        self.scoped_name.clone_as_ref()
    }

    pub fn into_owned(self) -> Label<'static> {
        Label {
            name: self.name.into_owned(),
            scope: self.scope.map(|string_bytes| string_bytes.into_owned()),
            scoped_name: self.scoped_name.into_owned(),
        }
    }
}

impl<'a> fmt::Display for Label<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Label[name={}, scope={}, scoped_name={}",
            self.name(),
            self.scope().map(|s| format!("{}", s)).unwrap_or_default(),
            self.scoped_name()
        )
    }
}

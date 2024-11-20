/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    hash::{DefaultHasher, Hasher},
    mem,
};

use typeql::{
    schema::definable::function::{Output, Single, SingleSelector, Stream},
    type_::NamedType,
    TypeRef, TypeRefAny, Variable,
};

use crate::{ordered_hash_combine, StructuralEquality};

impl StructuralEquality for Variable {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            ^ match self {
                Variable::Anonymous { optional, .. } => optional.is_none() as u64,
                Variable::Named { ident, optional, .. } => {
                    ordered_hash_combine(ident.as_str().hash(), optional.is_none() as u64)
                }
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Anonymous { optional, .. }, Self::Anonymous { optional: other_optional, .. }) => {
                optional == other_optional
            }
            (Self::Named { ident, optional, .. }, Self::Named { ident: other_ident, optional: other_optional, .. }) => {
                ident == other_ident && optional == other_optional
            }
            // note: this style forces updating the match when the variants change
            (Self::Anonymous { .. }, _) | (Self::Named { .. }, _) => false,
        }
    }
}

impl StructuralEquality for NamedType {
    // note: unordered specialisations not required here as these never contain inner generic T: StructuralEquality
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            ^ match self {
                NamedType::Label(label) => label.ident.as_str().hash(),
                NamedType::Role(scoped_label) => {
                    let mut hasher = DefaultHasher::new();
                    scoped_label.name.ident.as_str().hash_into(&mut hasher);
                    scoped_label.scope.ident.as_str().hash_into(&mut hasher);
                    hasher.finish()
                }
                NamedType::BuiltinValueType(builtin_value_type) => builtin_value_type.token.as_str().hash(),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Label(inner), Self::Label(other_inner)) => inner.ident.as_str().equals(other_inner.ident.as_str()),
            (Self::Role(inner), Self::Role(other_inner)) => {
                inner.name.ident.as_str().equals(other_inner.name.ident.as_str())
                    && inner.scope.ident.as_str().equals(other_inner.scope.ident.as_str())
            }
            (Self::BuiltinValueType(inner), Self::BuiltinValueType(other_inner)) => {
                inner.token.as_str().equals(other_inner.token.as_str())
            }
            // note: this style forces updating the match when the variants change
            (Self::Label { .. }, _) | (Self::BuiltinValueType { .. }, _) | (Self::Role { .. }, _) => false,
        }
    }
}

impl StructuralEquality for TypeRef {
    // note: unordered specialisations not required here as these never contain inner generic T: StructuralEquality
    fn hash(&self) -> u64 {
        ordered_hash_combine(
            mem::discriminant(self).hash(),
            match self {
                TypeRef::Named(named_type) => named_type.hash(),
                TypeRef::Variable(variable) => variable.hash(),
            },
        )
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (TypeRef::Named(inner), TypeRef::Named(other_inner)) => inner.equals(other_inner),
            (TypeRef::Variable(inner), TypeRef::Variable(other_inner)) => inner.equals(other_inner),
            // note: this style forces updating the match when the variants change
            (Self::Named { .. }, _) | (Self::Variable { .. }, _) => false,
        }
    }
}

impl StructuralEquality for TypeRefAny {
    fn hash(&self) -> u64 {
        ordered_hash_combine(
            mem::discriminant(self).hash(),
            match self {
                TypeRefAny::Type(type_ref) => type_ref.hash(),
                TypeRefAny::Optional(optional) => optional.inner.hash(),
                TypeRefAny::List(list) => list.inner.hash(),
            },
        )
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Type(inner), Self::Type(other_inner)) => inner.equals(other_inner),
            (Self::Optional(inner), Self::Optional(other_inner)) => inner.inner.equals(&other_inner.inner),
            (Self::List(inner), Self::List(other_inner)) => inner.inner.equals(&other_inner.inner),
            // note: this style forces updating the match when the variants change
            (Self::Type { .. }, _) | (Self::Optional { .. }, _) | (Self::List { .. }, _) => false,
        }
    }
}

impl StructuralEquality for SingleSelector {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::First, Self::First) | (Self::Last, Self::Last) => true,
            // note: this style forces updating the match when the variants change
            (Self::First { .. }, _) | (Self::Last { .. }, _) => false,
        }
    }
}

impl StructuralEquality for Output {
    fn hash(&self) -> u64 {
        mem::discriminant(self).hash()
            ^ match self {
                Output::Stream(stream) => stream.hash(),
                Output::Single(single) => single.hash(),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stream(stream), Self::Stream(other_stream)) => stream.equals(other_stream),
            (Self::Single(single), Self::Single(other_single)) => single.equals(other_single),
            // Note: this style forces updating the match when the variants change
            (Self::Stream(_), _) | (Self::Single(_), _) => false,
        }
    }
}

impl StructuralEquality for Stream {
    fn hash(&self) -> u64 {
        self.types.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.types.equals(&other.types)
    }
}
impl StructuralEquality for Single {
    fn hash(&self) -> u64 {
        self.types.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.types.equals(&other.types)
    }
}

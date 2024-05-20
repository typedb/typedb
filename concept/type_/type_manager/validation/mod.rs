/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use bytes::byte_reference::ByteReference;
use encoding::graph::thing::edge::ThingEdgeHasReverse;
use encoding::graph::thing::vertex_attribute::AttributeVertex;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::graph::Typed;
use encoding::layout::prefix::Prefix;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use storage::key_range::KeyRange;
use storage::key_value::StorageKeyReference;
use storage::snapshot::iterator::SnapshotIteratorError;
use storage::snapshot::ReadableSnapshot;
use crate::error::ConceptReadError;
use crate::thing::attribute::AttributeIterator;
use crate::thing::entity::EntityIterator;
use crate::thing::relation::RelationIterator;
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::object_type::ObjectType;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::KindAPI;
use crate::type_::{TypeAPI, WrappedTypeForError};

pub mod validation;
mod annotation_compatibility;


#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    ConceptRead(ConceptReadError),
    RootModification,
    LabelUniqueness(Label<'static>),
    CyclicTypeHierarchy(WrappedTypeForError, WrappedTypeForError), // TODO: Add details of what caused it
    RelatesNotInherited(RoleType<'static>),
    OwnsNotInherited(AttributeType<'static>),
    PlaysNotInherited(ObjectType<'static>, RoleType<'static>),
    OverriddenTypeNotSupertype(WrappedTypeForError, WrappedTypeForError),
    PlaysNotDeclared(ObjectType<'static>, RoleType<'static>),
    TypeIsNotAbstract(WrappedTypeForError),
    IncompatibleValueTypes(Option<ValueType>, Option<ValueType>),
    DeletingTypeWithSubtypes(WrappedTypeForError),
    DeletingTypeWithInstances(WrappedTypeForError),
}

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for SchemaValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptRead(source) => Some(source),
            Self::LabelUniqueness(_) => None,
            SchemaValidationError::RootModification => None,
            SchemaValidationError::CyclicTypeHierarchy(_,_) => None,
            SchemaValidationError::RelatesNotInherited(_) => None,
            SchemaValidationError::OwnsNotInherited(_) => None,
            SchemaValidationError::PlaysNotInherited(_, _) => None,
            SchemaValidationError::OverriddenTypeNotSupertype(_, _) => None,
            SchemaValidationError::PlaysNotDeclared(_, _) => None,
            SchemaValidationError::TypeIsNotAbstract(_) => None,
            SchemaValidationError::IncompatibleValueTypes(_, _) => None,
            SchemaValidationError::DeletingTypeWithSubtypes(_) => None,
            SchemaValidationError::DeletingTypeWithInstances(_) => None,
        }
    }
}

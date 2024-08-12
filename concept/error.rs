/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, sync::Arc};

use encoding::{
    error::EncodingError,
    value::{label::Label, value::Value, value_type::ValueType},
};
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

use crate::{
    thing::{object::Object, relation::Relation, thing_manager::validation::DataValidationError},
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationError, AnnotationRegex},
        attribute_type::AttributeType,
        object_type::ObjectType,
        role_type::RoleType,
        type_manager::validation::SchemaValidationError,
    },
};

#[derive(Debug)]
pub struct ConceptError {
    pub kind: ConceptErrorKind,
}

#[derive(Debug)]
pub enum ConceptErrorKind {
    AttributeValueTypeMismatch { attribute_type_value_type: Option<ValueType>, provided_value_type: ValueType },
}

impl fmt::Display for ConceptError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ConceptErrorKind::AttributeValueTypeMismatch { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConceptWriteError {
    RootModification,
    SnapshotGet {
        source: SnapshotGetError,
    },
    SnapshotIterate {
        source: Arc<SnapshotIteratorError>,
    },
    ConceptRead {
        source: ConceptReadError,
    },
    SchemaValidation {
        source: SchemaValidationError,
    },
    DataValidation {
        source: DataValidationError,
    },
    Encoding {
        source: EncodingError,
    },
    ValueTypeMismatch {
        expected: Option<ValueType>,
        provided: ValueType,
    },
    RelationRoleCardinality {
        relation: Relation<'static>,
        role_type: RoleType<'static>,
        cardinality: AnnotationCardinality,
        actual_cardinality: u64,
    },

    MultipleKeys {
        owner: Object<'static>,
        key_type: AttributeType<'static>,
    },
    KeyMissing {
        owner: Object<'static>,
        key_type: AttributeType<'static>,
    },

    SetHasOnDeleted {
        owner: Object<'static>,
    },
    AddPlayerOnDeleted {
        relation: Relation<'static>,
    },

    CardinalityViolation {
        owner: Object<'static>,
        attribute_type: AttributeType<'static>,
        cardinality: AnnotationCardinality,
    },

    SetHasOrderedOwnsUnordered {},
    SetHasUnorderedOwnsOrdered {},
    UnsetHasOrderedOwnsUnordered {},
    UnsetHasUnorderedOwnsOrdered {},

    SetPlayersOrderedRoleUnordered {},

    Annotation {
        source: AnnotationError,
    },
}

impl fmt::Display for ConceptWriteError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotGet { source, .. } => Some(source),
            Self::SnapshotIterate { source, .. } => Some(source),
            Self::ConceptRead { source } => Some(source),
            Self::Encoding { source, .. } => Some(source),
            Self::SchemaValidation { source, .. } => Some(source),
            Self::DataValidation { source, .. } => Some(source),
            Self::ValueTypeMismatch { .. } => None,
            Self::RelationRoleCardinality { .. } => None,
            Self::RootModification { .. } => None,
            Self::KeyMissing { .. } => None,
            Self::SetHasOnDeleted { .. } => None,
            Self::MultipleKeys { .. } => None,
            Self::AddPlayerOnDeleted { .. } => None,
            Self::CardinalityViolation { .. } => None,
            Self::SetHasOrderedOwnsUnordered { .. } => None,
            Self::SetPlayersOrderedRoleUnordered { .. } => None,
            Self::SetHasUnorderedOwnsOrdered { .. } => None,
            Self::UnsetHasOrderedOwnsUnordered { .. } => None,
            Self::UnsetHasUnorderedOwnsOrdered { .. } => None,
            Self::Annotation { .. } => None,
        }
    }
}

impl From<ConceptReadError> for ConceptWriteError {
    fn from(error: ConceptReadError) -> Self {
        match error {
            ConceptReadError::SnapshotGet { source } => Self::SnapshotGet { source },
            ConceptReadError::SnapshotIterate { source } => Self::SnapshotIterate { source },
            ConceptReadError::Encoding { source, .. } => Self::Encoding { source },
            ConceptReadError::CorruptMissingLabelOfType => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryCardinality => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryProperty => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryRelatesForRole => Self::ConceptRead { source: error },
            ConceptReadError::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(_, _, _) => {
                Self::ConceptRead { source: error }
            }
            ConceptReadError::CannotGetOwnsDoesntExist(_, _) => Self::ConceptRead { source: error },
            ConceptReadError::Annotation { .. } => Self::ConceptRead { source: error },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConceptReadError {
    SnapshotGet { source: SnapshotGetError },
    SnapshotIterate { source: Arc<SnapshotIteratorError> },
    Encoding { source: EncodingError },
    CorruptMissingLabelOfType,
    CorruptMissingMandatoryCardinality,
    CorruptMissingMandatoryProperty,
    CorruptMissingMandatoryRelatesForRole,
    CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(Label<'static>, ValueType, Annotation),
    CannotGetOwnsDoesntExist(Label<'static>, Label<'static>),
    Annotation { source: AnnotationError },
}

impl fmt::Display for ConceptReadError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for ConceptReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotGet { source, .. } => Some(source),
            Self::SnapshotIterate { source, .. } => Some(source),
            Self::Encoding { source, .. } => Some(source),
            Self::CorruptMissingLabelOfType => None,
            Self::CorruptMissingMandatoryCardinality => None,
            Self::CorruptMissingMandatoryProperty => None,
            Self::CorruptAttributeValueTypeDoesntMatchAttributeTypeConstraint(_, _, _) => None,
            Self::CorruptMissingMandatoryRelatesForRole => None,
            Self::CannotGetOwnsDoesntExist(_, _) => None,
            Self::Annotation { .. } => None,
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, sync::Arc};

use encoding::{
    error::EncodingError,
    value::{label::Label, value_type::ValueType},
};
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

use crate::{
    thing::thing_manager::validation::DataValidationError,
    type_::{
        annotation::AnnotationError,
        attribute_type::AttributeType,
        constraint::ConstraintError,
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
    SnapshotGet { source: SnapshotGetError },
    SnapshotIterate { source: Arc<SnapshotIteratorError> },
    ConceptRead { source: ConceptReadError },
    SchemaValidation { source: SchemaValidationError },
    DataValidation { source: DataValidationError },
    Encoding { source: EncodingError },
    Annotation { source: AnnotationError },
    // TODO: Might refactor these to "InvalidOperationError", or just use unreachable! instead of it.
    SetHasOrderedOwnsUnordered {},
    SetHasUnorderedOwnsOrdered {},
    UnsetHasOrderedOwnsUnordered {},
    UnsetHasUnorderedOwnsOrdered {},
    SetPlayersOrderedRoleUnordered {},
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
            Self::RootModification { .. } => None,
            Self::Annotation { .. } => None,
            Self::SetHasOrderedOwnsUnordered { .. } => None,
            Self::SetPlayersOrderedRoleUnordered { .. } => None,
            Self::SetHasUnorderedOwnsOrdered { .. } => None,
            Self::UnsetHasOrderedOwnsUnordered { .. } => None,
            Self::UnsetHasUnorderedOwnsOrdered { .. } => None,
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
            ConceptReadError::CorruptMissingCapability => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryOrdering => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryValueType => Self::ConceptRead { source: error },
            ConceptReadError::CorruptMissingMandatoryRelatesForRole => Self::ConceptRead { source: error },
            ConceptReadError::CorruptFoundHasWithoutOwns => Self::ConceptRead { source: error },
            ConceptReadError::CorruptFoundLinksWithoutPlays => Self::ConceptRead { source: error },
            ConceptReadError::CorruptFoundLinksWithoutRelates => Self::ConceptRead { source: error },
            ConceptReadError::CannotGetOwnsDoesntExist(_, _) => Self::ConceptRead { source: error },
            ConceptReadError::CannotGetPlaysDoesntExist(_, _) => Self::ConceptRead { source: error },
            ConceptReadError::CannotGetRelatesDoesntExist(_, _) => Self::ConceptRead { source: error },
            ConceptReadError::Annotation { .. } => Self::ConceptRead { source: error },
            ConceptReadError::Constraint { .. } => Self::ConceptRead { source: error },
            ConceptReadError::ValueTypeMismatchWithAttributeType { .. } => Self::ConceptRead { source: error },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConceptReadError {
    SnapshotGet {
        source: SnapshotGetError,
    },
    SnapshotIterate {
        source: Arc<SnapshotIteratorError>,
    },
    Encoding {
        source: EncodingError,
    },
    CorruptMissingLabelOfType,
    CorruptMissingMandatoryCardinality,
    CorruptMissingCapability,
    CorruptMissingMandatoryOrdering,
    CorruptMissingMandatoryValueType,
    CorruptMissingMandatoryRelatesForRole,
    CorruptFoundHasWithoutOwns,
    CorruptFoundLinksWithoutPlays,
    CorruptFoundLinksWithoutRelates,
    CannotGetOwnsDoesntExist(Label<'static>, Label<'static>),
    CannotGetPlaysDoesntExist(Label<'static>, Label<'static>),
    CannotGetRelatesDoesntExist(Label<'static>, Label<'static>),
    Annotation {
        source: AnnotationError,
    },
    Constraint {
        source: ConstraintError,
    },
    ValueTypeMismatchWithAttributeType {
        attribute_type: AttributeType<'static>,
        expected: Option<ValueType>,
        provided: ValueType,
    },
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
            Self::CorruptMissingCapability => None,
            Self::CorruptMissingMandatoryOrdering => None,
            Self::CorruptMissingMandatoryValueType => None,
            Self::CorruptMissingMandatoryRelatesForRole => None,
            Self::CorruptFoundHasWithoutOwns => None,
            Self::CorruptFoundLinksWithoutPlays => None,
            Self::CorruptFoundLinksWithoutRelates => None,
            Self::CannotGetOwnsDoesntExist(_, _) => None,
            Self::CannotGetPlaysDoesntExist(_, _) => None,
            Self::CannotGetRelatesDoesntExist(_, _) => None,
            Self::Annotation { .. } => None,
            Self::Constraint { .. } => None,
            Self::ValueTypeMismatchWithAttributeType { .. } => None,
        }
    }
}

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
use error::typedb_error;
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

use crate::{
    thing::thing_manager::validation::DataValidationError,
    type_::{
        annotation::AnnotationError, attribute_type::AttributeType, constraint::ConstraintError,
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

typedb_error!(
    pub ConceptWriteError(component = "Concept write", prefix = "COW") {
        SnapshotGet(1, "Concept write failed due to a snapshot read error.", (source: SnapshotGetError)),
        SnapshotIterate(2, "Concept write failed due to a snapshot iteration error.", (source: Arc<SnapshotIteratorError>)),
        ConceptRead(3, "Concept write failed due to a concept read error.", (source: Box<ConceptReadError>)),
        SchemaValidation(4, "Concept write failed due to a schema validation error.", (typedb_source: Box<SchemaValidationError>)),
        DataValidation(5, "Concept write failed due to a data validation error.", (typedb_source: Box<DataValidationError> )),
        Encoding(6, "Concept write failed due to an encoding error.", (source: EncodingError)),
        Annotation(7, "Concept write failed due to an annotation error.", (source: AnnotationError)),

        // TODO: Might refactor these to "InvalidOperationError", or just use unreachable! instead of it.
        SetHasOrderedOwnsUnordered(8, "Concept write failed, due to setting ordered owns as unordered."),
        SetHasUnorderedOwnsOrdered(9, "Concept write failed, due to setting unordered owns as ordered."),
        UnsetHasOrderedOwnsUnordered(10, "Concept write failed, cannot unset an ordered owns when the ownership is unordered."),
        UnsetHasUnorderedOwnsOrdered(11, "Concept write failed, cannot unset an unordered owns when the ownership is ordered"),
        SetPlayersOrderedRoleUnordered(12, "Concept write failed, cannot set relation's ordered role players as unordered."),
    }
);

impl From<Box<ConceptReadError>> for Box<ConceptWriteError> {
    fn from(error: Box<ConceptReadError>) -> Self {
        Box::new(match *error {
            ConceptReadError::SnapshotGet { source } => ConceptWriteError::SnapshotGet { source },
            ConceptReadError::SnapshotIterate { source } => ConceptWriteError::SnapshotIterate { source },
            ConceptReadError::Encoding { source, .. } => ConceptWriteError::Encoding { source },
            _ => ConceptWriteError::ConceptRead { source: error },
        })
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
    OrderingValueMissing,
    CorruptMissingMandatoryValueType,
    CorruptMissingMandatoryAttributeValue,
    CorruptMissingMandatoryRootRelatesForRole,
    CorruptMissingMandatoryScopeForRoleTypeLabel,
    CorruptMissingMandatorySpecialisingRelatesForRole,
    CorruptMissingMandatoryCardinalityForNonSpecialisingCapability,
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
        source: Box<ConstraintError>,
    },
    ValueTypeMismatchWithAttributeType {
        attribute_type: AttributeType,
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
            Self::OrderingValueMissing => None,
            Self::CorruptMissingMandatoryValueType => None,
            Self::CorruptMissingMandatoryAttributeValue => None,
            Self::CorruptMissingMandatoryRootRelatesForRole => None,
            Self::CorruptMissingMandatoryScopeForRoleTypeLabel => None,
            Self::CorruptMissingMandatorySpecialisingRelatesForRole => None,
            Self::CorruptMissingMandatoryCardinalityForNonSpecialisingCapability => None,
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

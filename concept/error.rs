/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, sync::Arc};

use encoding::{error::EncodingError, value::value_type::ValueType};
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

use crate::{
    thing::relation::Relation,
    type_::{annotation::AnnotationCardinality, role_type::RoleType},
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

#[derive(Debug)]
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
    StringAttributeRegex {
        regex: String,
        value: String,
    },
    EntityKeyMissing {},
    SetHasOnDeleted {},
    SetHasMultipleKeys {},
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
            Self::ValueTypeMismatch { .. } => None,
            Self::RelationRoleCardinality { .. } => None,
            Self::RootModification { .. } => None,
            Self::StringAttributeRegex { .. } => None,
            Self::EntityKeyMissing { .. } => None,
            Self::SetHasOnDeleted { .. } => None,
            Self::SetHasMultipleKeys { .. } => None,
        }
    }
}

impl From<ConceptReadError> for ConceptWriteError {
    fn from(error: ConceptReadError) -> Self {
        match error {
            ConceptReadError::SnapshotGet { source } => Self::SnapshotGet { source },
            ConceptReadError::SnapshotIterate { source } => Self::SnapshotIterate { source },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConceptReadError {
    SnapshotGet { source: SnapshotGetError },
    SnapshotIterate { source: Arc<SnapshotIteratorError> },
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
        }
    }
}

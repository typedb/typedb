/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

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

typedb_error!(
    pub ConceptWriteError(component = "Concept write", prefix = "COW") {
        SnapshotGet(1, "Concept write failed due to a snapshot read error.", source: SnapshotGetError),
        SnapshotIterate(2, "Concept write failed due to a snapshot iteration error.", source: Arc<SnapshotIteratorError>),
        ConceptRead(3, "Concept write failed due to a concept read error.", typedb_source: Box<ConceptReadError>),
        SchemaValidation(4, "Concept write failed due to a schema validation error.", typedb_source: Box<SchemaValidationError>),
        DataValidation(5, "Concept write failed due to a data validation error.", typedb_source: Box<DataValidationError>),
        Encoding(6, "Concept write failed due to an encoding error.", source: EncodingError),
        Annotation(7, "Concept write failed due to an annotation error.", typedb_source: AnnotationError),

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
            _ => ConceptWriteError::ConceptRead { typedb_source: error },
        })
    }
}

typedb_error! {
    pub ConceptReadError(component = "Concept read", prefix = "COR") {
        SnapshotGet(1, "Failed to read key from storage snapshot.", source: SnapshotGetError),
        SnapshotIterate(2, "Failed to open iterator on storage snapshot.", source: Arc<SnapshotIteratorError>),
        Encoding(3, "Concept encoding error.", source: EncodingError),
        OrderingValueMissing(4, "Ordering type missing."),
        InternalMissingTypeLabel(5, "Internal error: type is missing a label."),
        InternalMissingCardinality(6, "Internal error: missing cardinality."),
        InternalMissingCapability(7, "Internal error: missing trait."),
        InternalMissingValueType(8, "Internal error: missing value type."),
        InternalMissingAttributeValue(9, "Internal error: missing attribute value."),
        InternalMissingRootRelatesForRole(10, "Internal error: root relates missing for role."),
        InternalMissingScopeForRoleTypeLabel(11, "Internal error: missing scope for role type label."),
        InternalMissingSpecialisingRelatesForRole(12, "Internal error: missing specialising relates for role."),
        InternalMissingCardinalityForNonSpecialisingCapability(13, "Internal error: missing cardinality for non-specialising capability."),
        InternalHasWithoutOwns(14, "Internal error: 'has' operation performed without corresponding schema 'owns'."),
        InternalLinksWithoutPlays(15, "Internal error: 'links' operation without corresponding schema 'plays'."),
        InternalLinksWithoutRelates(16, "Internal error: 'links' operation without corresponding 'relates'."),
        CannotGetOwnsDoesntExist(17, "Cannot get 'owns': relationship does not exist.", type_: Label, owns: Label),
        CannotGetPlaysDoesntExist(18, "Cannot get 'plays': relationship does not exist.", type_: Label, plays: Label),
        CannotGetRelatesDoesntExist(19, "Cannot get 'relates': relationship does not exist.", type_: Label, relates: Label),
        Annotation(20, "Annotation error.", typedb_source: AnnotationError),
        Constraint(21, "Constraint error.", typedb_source: Box<ConstraintError>),
        ValueTypeMismatchWithAttributeType(22, "Attribute type '{attribute_type}' has value type '{expected:?}' and cannot be used with '{provided}'.", attribute_type: AttributeType, expected: Option<ValueType>, provided: ValueType),
        RelationIndexNotAvailable(23, "Relation index not available for relations of type '{relation_label}'.", relation_label: Label),
        UnimplementedFunctionality(24, "Unimplemented functionality encountered: {functionality}.", functionality: error::UnimplementedFeature),
        InternalIncomparableTypes(25, "Incomparable types"),
    }
}

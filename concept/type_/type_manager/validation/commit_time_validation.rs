/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::Kind;
use storage::snapshot::ReadableSnapshot;

use crate::{error::ConceptReadError, type_::type_manager::validation::SchemaValidationError};
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::relation_type::RelationType;
use crate::type_::type_manager::type_reader::TypeReader;

pub struct CommitTimeValidation {}

macro_rules! validate_types {
    ($func_name:ident, $kind:expr, $type_:ident, $func:path) => {
        fn $func_name(snapshot: &impl ReadableSnapshot) -> Result<(), SchemaValidationError> {
            let root_label = $kind.root_label();
            let root = TypeReader::get_labelled_type::<$type_<'static>>(snapshot, &root_label)
                .map_err(|error| SchemaValidationError::ConceptRead(error))?
                .ok_or(SchemaValidationError::RootHasBeenCorrupted(root_label))?;

            $func(snapshot, root.clone())?;

            let subtypes = TypeReader::get_subtypes_transitive(snapshot, root)
                .map_err(|error| SchemaValidationError::ConceptRead(error))?;
            for subtype in subtypes {
                $func(snapshot, subtype)?;
            }
            Ok(())
        }
    };
}

impl CommitTimeValidation {
    pub(crate) fn validate(snapshot: &impl ReadableSnapshot) -> Result<Vec<SchemaValidationError>, ConceptReadError>
    {
        let mut errors = Vec::new();
        // if let Err(error) = Self::validate_entity_types(snapshot) {
        //     errors.push(error);
        // }
        // if let Err(error) = Self::validate_relation_types(snapshot) {
        //     errors.push(error);
        // }
        // if let Err(error) = Self::validate_attribute_types(snapshot) {
        //     errors.push(error);
        // }
        Ok(errors)
    }

    validate_types!(validate_entity_types, Kind::Entity, EntityType, Self::validate_entity_type);
    validate_types!(validate_relation_types, Kind::Relation, RelationType, Self::validate_relation_type);
    validate_types!(validate_attribute_types, Kind::Attribute, AttributeType, Self::validate_attribute_type);

    fn validate_entity_type(snapshot: &impl ReadableSnapshot, type_: EntityType<'static>) -> Result<(), SchemaValidationError> {

        todo!()
    }

    fn validate_relation_type(snapshot: &impl ReadableSnapshot, type_: RelationType<'static>) -> Result<(), SchemaValidationError> {

        todo!()
    }

    fn validate_attribute_type(snapshot: &impl ReadableSnapshot, type_: AttributeType<'static>) -> Result<(), SchemaValidationError> {

        todo!()
    }
}

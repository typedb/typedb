use encoding::value::label::Label;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::{
        thing_manager::{validation::DataValidationError, ThingManager},
    },
    type_::{
        attribute_type::AttributeType, object_type::ObjectType, role_type::RoleType, type_manager::TypeManager,
        ObjectTypeAPI, OwnerAPI, PlayerAPI, TypeAPI,
    },
};

pub struct OperationTimeValidation {}

impl OperationTimeValidation {
    pub(crate) fn get_label_or_concept_read_err<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl TypeAPI<'a>,
    ) -> Result<Label<'static>, ConceptReadError> {
        type_.get_label(snapshot, type_manager).map(|label| label.clone())
    }

    pub(crate) fn get_label_or_schema_err<'a>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        type_: impl TypeAPI<'a>,
    ) -> Result<Label<'static>, DataValidationError> {
        Self::get_label_or_concept_read_err(snapshot, type_manager, type_).map_err(DataValidationError::ConceptRead)
    }

    pub(crate) fn validate_type_instance_is_not_abstract(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        type_: impl TypeAPI<'static>,
    ) -> Result<(), DataValidationError> {
        if type_.is_abstract(snapshot, &thing_manager.type_manager).map_err(DataValidationError::ConceptRead)? {
            Err(DataValidationError::CannotCreateInstanceOfAbstractType(Self::get_label_or_schema_err(
                snapshot,
                &thing_manager.type_manager,
                type_,
            )?))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_object_type_plays_role_type(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: ObjectType<'_>,
        role_type: RoleType<'_>,
    ) -> Result<(), DataValidationError> {
        let has_plays = object_type
            .get_plays(snapshot, &thing_manager.type_manager)
            .map_err(DataValidationError::ConceptRead)?
            .contains_key(&role_type.clone());
        if has_plays {
            Ok(())
        } else {
            Err(DataValidationError::CannotAddPlayerInstanceForNotPlayedRoleType(
                Self::get_label_or_schema_err(snapshot, &thing_manager.type_manager, object_type)?,
                Self::get_label_or_schema_err(snapshot, &thing_manager.type_manager, role_type)?,
            ))
        }
    }

    pub(crate) fn validate_object_type_owns_attribute_type<'a>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object_type: impl ObjectTypeAPI<'a>,
        attribute_type: AttributeType<'_>,
    ) -> Result<(), DataValidationError> {
        let has_owns = object_type
            .get_owns(snapshot, &thing_manager.type_manager)
            .map_err(DataValidationError::ConceptRead)?
            .contains_key(&attribute_type.clone());
        if has_owns {
            Ok(())
        } else {
            Err(DataValidationError::CannotAddOwnerInstanceForNotOwnedAttributeType(
                Self::get_label_or_schema_err(snapshot, &thing_manager.type_manager, object_type)?,
                Self::get_label_or_schema_err(snapshot, &thing_manager.type_manager, attribute_type)?,
            ))
        }
    }
}

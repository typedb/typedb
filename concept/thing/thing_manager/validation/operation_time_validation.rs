use encoding::value::label::Label;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::thing_manager::{validation::DataValidationError, ThingManager},
    type_::{type_manager::TypeManager, TypeAPI},
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
}

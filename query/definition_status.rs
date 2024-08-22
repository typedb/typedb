/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use encoding::graph::definition::r#struct::StructDefinitionField;
use encoding::value::value_type::ValueType;
use storage::snapshot::ReadableSnapshot;

macro_rules! get_some_or_definable_does_not_exist {
    ($res:pat = $opt:ident) => {
        let $res = if let Some(some) = $opt {
            some
        } else {
            return Ok(DefinitionStatus::DoesNotExist);
        };
    };
}

pub(crate) enum DefinitionStatus<T> {
    DoesNotExist,
    ExistsSame,
    ExistsDifferent(T),
}

pub(crate) fn get_struct_status(
    snapshot: &mut impl ReadableSnapshot,
    type_manager: &TypeManager,
    name: &str,
) -> Result<DefinitionStatus<DefinitionKey<'static>>, ConceptReadError> {
    let definition_key_opt = type_manager.get_struct_definition_key(snapshot, name)?;
    get_some_or_definable_does_not_exist!(_ = definition_key_opt);
    Ok(DefinitionStatus::ExistsSame)
}

pub(crate) fn get_struct_field_status(
    snapshot: &mut impl ReadableSnapshot,
    type_manager: &TypeManager,
    definition_key: DefinitionKey<'static>,
    field_key: &str,
    value_type: ValueType,
    optional: bool,
) -> Result<DefinitionStatus<StructDefinitionField>, ConceptReadError> {
    let struct_definition = type_manager.get_struct_definition(snapshot, definition_key)?;
    let field_opt = struct_definition.get_field(field_key);
    get_some_or_definable_does_not_exist!(field = field_opt);

    if field.same(optional, value_type) {
        Ok(DefinitionStatus::ExistsSame)
    } else {
        Ok(DefinitionStatus::ExistsDifferent(field.clone()))
    }
}

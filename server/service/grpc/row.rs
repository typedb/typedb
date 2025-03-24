/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::row::MaybeOwnedRow;
use storage::snapshot::ReadableSnapshot;

use crate::service::grpc::concept::{encode_thing_concept, encode_type_concept, encode_value};

pub(crate) fn encode_row(
    row: MaybeOwnedRow<'_>,
    columns: &[(String, VariablePosition)],
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::ConceptRow, Box<ConceptReadError>> {
    // TODO: multiplicity?
    let mut encoded_row = Vec::with_capacity(columns.len());
    for (_, position) in columns {
        let variable_value = row.get(*position);
        let row_entry =
            encode_row_entry(variable_value, snapshot, type_manager, thing_manager, include_instance_types)?;
        encoded_row.push(typedb_protocol::RowEntry { entry: Some(row_entry) });
    }
    Ok(typedb_protocol::ConceptRow { row: encoded_row })
}

pub(crate) fn encode_row_entry(
    variable_value: &VariableValue<'_>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<typedb_protocol::row_entry::Entry, Box<ConceptReadError>> {
    match variable_value {
        VariableValue::Empty => Ok(typedb_protocol::row_entry::Entry::Empty(typedb_protocol::row_entry::Empty {})),
        VariableValue::Type(type_) => {
            Ok(typedb_protocol::row_entry::Entry::Concept(encode_type_concept(type_, snapshot, type_manager)?))
        }
        VariableValue::Thing(thing) => Ok(typedb_protocol::row_entry::Entry::Concept(encode_thing_concept(
            thing,
            snapshot,
            type_manager,
            thing_manager,
            include_instance_types,
        )?)),
        VariableValue::Value(value) => Ok(typedb_protocol::row_entry::Entry::Value(encode_value(value.as_reference()))),
        VariableValue::ThingList(thing_list) => {
            let mut encoded = Vec::with_capacity(thing_list.len());
            for thing in thing_list.iter() {
                encoded.push(encode_thing_concept(
                    thing,
                    snapshot,
                    type_manager,
                    thing_manager,
                    include_instance_types,
                )?);
            }
            Ok(typedb_protocol::row_entry::Entry::ConceptList(typedb_protocol::row_entry::ConceptList {
                concepts: encoded,
            }))
        }
        VariableValue::ValueList(value_list) => {
            let mut encoded = Vec::with_capacity(value_list.len());
            for value in value_list.iter() {
                encoded.push(encode_value(value.as_reference()))
            }
            Ok(typedb_protocol::row_entry::Entry::ValueList(typedb_protocol::row_entry::ValueList { values: encoded }))
        }
    }
}

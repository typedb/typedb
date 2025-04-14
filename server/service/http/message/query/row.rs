/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::row::MaybeOwnedRow;
use serde_json::json;
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{encode_thing_concept, encode_type_concept, encode_value};

pub fn encode_row(
    row: MaybeOwnedRow<'_>,
    columns: &[(String, VariablePosition)],
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    // TODO: multiplicity?
    let mut encoded_row = HashMap::with_capacity(columns.len());
    for (variable, position) in columns {
        let variable_value = row.get(*position);
        let row_entry =
            encode_row_entry(variable_value, snapshot, type_manager, thing_manager, include_instance_types)?;
        encoded_row.insert(variable, row_entry);
    }
    Ok(json!(encoded_row))
}

pub fn encode_row_entry(
    variable_value: &VariableValue<'_>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    match variable_value {
        VariableValue::Empty => Ok(json!(serde_json::Value::Null)),
        VariableValue::Type(type_) => Ok(json!(encode_type_concept(type_, snapshot, type_manager)?)),
        VariableValue::Thing(thing) => {
            Ok(json!(encode_thing_concept(thing, snapshot, type_manager, thing_manager, include_instance_types)?))
        }
        VariableValue::Value(value) => Ok(json!(encode_value(value.as_reference()))),
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
            Ok(json!(encoded))
        }
        VariableValue::ValueList(value_list) => {
            let mut encoded = Vec::with_capacity(value_list.len());
            for value in value_list.iter() {
                encoded.push(encode_value(value.as_reference()))
            }
            Ok(json!(encoded))
        }
    }
}

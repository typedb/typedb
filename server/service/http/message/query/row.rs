/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable_value::VariableValue;
use compiler::{query_structure::QueryStructureBlockID, VariablePosition};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::row::MaybeOwnedRow;
use hyper::body::HttpBody;
use resource::profile::StorageCounters;
use serde::Serialize;
use serde_json::json;
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{encode_thing_concept, encode_type_concept, encode_value};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EncodedRow<'a> {
    data: HashMap<&'a str, serde_json::Value>,
    involved_blocks: Vec<u16>,
}

pub fn encode_row<'a>(
    row: MaybeOwnedRow<'_>,
    columns: &'a [(String, VariablePosition)],
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
    storage_counters: StorageCounters,
    always_taken_blocks: Option<&Vec<QueryStructureBlockID>>,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    // TODO: multiplicity?
    let mut encoded_row = HashMap::with_capacity(columns.len());
    for (variable, position) in columns {
        let variable_value = row.get(*position);
        let row_entry = encode_row_entry(
            variable_value,
            snapshot,
            type_manager,
            thing_manager,
            include_instance_types,
            storage_counters.clone(),
        )?;
        encoded_row.insert(variable.as_str(), row_entry);
    }
    let involved_blocks = row
        .provenance()
        .branch_ids()
        .map(|b| b.0)
        .chain(always_taken_blocks.unwrap_or(&vec![]).iter().map(|b| b.0))
        .collect();
    Ok(json!(EncodedRow { data: encoded_row, involved_blocks }))
}

pub fn encode_row_entry(
    variable_value: &VariableValue<'_>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    include_instance_types: bool,
    storage_counters: StorageCounters,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    match variable_value {
        VariableValue::Empty => Ok(json!(serde_json::Value::Null)),
        VariableValue::Type(type_) => Ok(json!(encode_type_concept(type_, snapshot, type_manager)?)),
        VariableValue::Thing(thing) => Ok(json!(encode_thing_concept(
            thing,
            snapshot,
            type_manager,
            thing_manager,
            include_instance_types,
            storage_counters
        )?)),
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
                    storage_counters.clone(),
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

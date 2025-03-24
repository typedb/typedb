/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{Concept, Thing, Type};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use executor::document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode};
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::constants::server::DEFAULT_INCLUDE_INSTANCE_TYPES_FETCH;
use serde_json::json;
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{
    encode_attribute, encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_value,
    AttributeTypeDocument, TypeDocument,
};

pub fn encode_document(
    document: ConceptDocument,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    Ok(json!(encode_node(document.root, snapshot, type_manager, thing_manager, parameters)?))
}

fn encode_node(
    node: DocumentNode,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    match node {
        DocumentNode::List(list) => Ok(json!(encode_list(list, snapshot, type_manager, thing_manager, parameters)?)),
        DocumentNode::Map(map) => Ok(json!(encode_map(map, snapshot, type_manager, thing_manager, parameters)?)),
        DocumentNode::Leaf(leaf) => Ok(json!(encode_leaf(leaf, snapshot, type_manager, thing_manager)?)),
    }
}

fn encode_map(
    map: DocumentMap,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    let encoded_map = match map {
        DocumentMap::UserKeys(map) => {
            let mut encoded_map = HashMap::with_capacity(map.len());
            for (key, value) in map.into_iter() {
                let key_name = parameters.fetch_key(key).expect("Expected key in parameters to get its name");
                let encoded_value = encode_node(value, snapshot, type_manager, thing_manager, parameters)?;
                encoded_map.insert(key_name.to_owned(), encoded_value);
            }
            encoded_map
        }
        DocumentMap::GeneratedKeys(map) => {
            let mut encoded_map = HashMap::with_capacity(map.len());
            for (key, value) in map.into_iter() {
                let encoded_value = encode_node(value, snapshot, type_manager, thing_manager, parameters)?;
                encoded_map.insert(key.scoped_name().as_str().to_owned(), encoded_value);
            }
            encoded_map
        }
    };
    Ok(json!(encoded_map))
}

fn encode_list(
    list: DocumentList,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    let encoded_list: Vec<serde_json::Value> = list
        .list
        .into_iter()
        .map(|node| encode_node(node, snapshot, type_manager, thing_manager, parameters))
        .try_collect()
        .expect("Expected json value list conversion");
    Ok(json!(encoded_list))
}

fn encode_leaf(
    leaf: DocumentLeaf,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
) -> Result<serde_json::Value, Box<ConceptReadError>> {
    let include_instance_types = DEFAULT_INCLUDE_INSTANCE_TYPES_FETCH; // TODO: May it be affected by QueryOptions?
    match leaf {
        DocumentLeaf::Empty => Ok(serde_json::Value::Null),
        DocumentLeaf::Concept(concept) => Ok(json!(match concept {
            Concept::Type(Type::Entity(entity_type)) => {
                json!(Into::<TypeDocument>::into(encode_entity_type(&entity_type, snapshot, type_manager)?))
            }
            Concept::Type(Type::Relation(relation_type)) => {
                json!(Into::<TypeDocument>::into(encode_relation_type(&relation_type, snapshot, type_manager)?))
            }
            Concept::Type(Type::Attribute(attribute_type)) => {
                json!(Into::<AttributeTypeDocument>::into(encode_attribute_type(
                    &attribute_type,
                    snapshot,
                    type_manager
                )?))
            }
            Concept::Type(Type::RoleType(role_type)) => {
                json!(Into::<TypeDocument>::into(encode_role_type(&role_type, snapshot, type_manager)?))
            }
            Concept::Thing(Thing::Entity(_)) => {
                unreachable!("Entities are not represented as documents")
            }
            Concept::Thing(Thing::Relation(_)) => {
                unreachable!("Relations are not represented as documents")
            }
            Concept::Thing(Thing::Attribute(attribute)) => {
                Into::<serde_json::Value>::into(encode_attribute(
                    &attribute,
                    snapshot,
                    type_manager,
                    thing_manager,
                    include_instance_types,
                )?)
            }
            Concept::Value(value) => {
                Into::<serde_json::Value>::into(encode_value(value))
            }
        })),
        DocumentLeaf::Kind(kind) => Ok(json!(kind.name())),
    }
}

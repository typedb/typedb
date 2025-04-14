/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{Concept, Thing, Type};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::graph::type_::Kind;
use executor::document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode};
use ir::pipeline::ParameterRegistry;
use itertools::Itertools;
use resource::constants::server::DEFAULT_INCLUDE_INSTANCE_TYPES_FETCH;
use storage::snapshot::ReadableSnapshot;

use crate::service::grpc::concept::{
    encode_attribute, encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_value,
};

pub(crate) fn encode_document(
    document: ConceptDocument,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<typedb_protocol::ConceptDocument, Box<ConceptReadError>> {
    Ok(typedb_protocol::ConceptDocument {
        root: Some(encode_node(document.root, snapshot, type_manager, thing_manager, parameters)?),
    })
}

fn encode_node(
    node: DocumentNode,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<typedb_protocol::concept_document::Node, Box<ConceptReadError>> {
    match node {
        DocumentNode::List(list) => Ok(typedb_protocol::concept_document::Node {
            node: Some(typedb_protocol::concept_document::node::Node::List(encode_list(
                list,
                snapshot,
                type_manager,
                thing_manager,
                parameters,
            )?)),
        }),
        DocumentNode::Map(map) => Ok(typedb_protocol::concept_document::Node {
            node: Some(typedb_protocol::concept_document::node::Node::Map(encode_map(
                map,
                snapshot,
                type_manager,
                thing_manager,
                parameters,
            )?)),
        }),
        DocumentNode::Leaf(leaf) => Ok(typedb_protocol::concept_document::Node {
            node: Some(typedb_protocol::concept_document::node::Node::Leaf(encode_leaf(
                leaf,
                snapshot,
                type_manager,
                thing_manager,
            )?)),
        }),
    }
}

fn encode_map(
    map: DocumentMap,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<typedb_protocol::concept_document::node::Map, Box<ConceptReadError>> {
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

    Ok(typedb_protocol::concept_document::node::Map { map: encoded_map })
}

fn encode_list(
    list: DocumentList,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    parameters: &ParameterRegistry,
) -> Result<typedb_protocol::concept_document::node::List, Box<ConceptReadError>> {
    let encoded_list = list
        .list
        .into_iter()
        .map(|node| encode_node(node, snapshot, type_manager, thing_manager, parameters))
        .try_collect()?;
    Ok(typedb_protocol::concept_document::node::List { list: encoded_list })
}

fn encode_leaf(
    leaf: DocumentLeaf,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
) -> Result<typedb_protocol::concept_document::node::Leaf, Box<ConceptReadError>> {
    match leaf {
        DocumentLeaf::Empty => Ok(typedb_protocol::concept_document::node::Leaf {
            leaf: Some(typedb_protocol::concept_document::node::leaf::Leaf::Empty(
                typedb_protocol::concept_document::node::leaf::Empty {},
            )),
        }),
        DocumentLeaf::Concept(concept) => Ok(typedb_protocol::concept_document::node::Leaf {
            leaf: Some(match concept {
                Concept::Type(Type::Entity(entity_type)) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::EntityType(encode_entity_type(
                        &entity_type,
                        snapshot,
                        type_manager,
                    )?)
                }
                Concept::Type(Type::Relation(relation_type)) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::RelationType(encode_relation_type(
                        &relation_type,
                        snapshot,
                        type_manager,
                    )?)
                }
                Concept::Type(Type::Attribute(attribute_type)) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::AttributeType(encode_attribute_type(
                        &attribute_type,
                        snapshot,
                        type_manager,
                    )?)
                }
                Concept::Type(Type::RoleType(role_type)) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::RoleType(encode_role_type(
                        &role_type,
                        snapshot,
                        type_manager,
                    )?)
                }
                Concept::Thing(Thing::Entity(entity)) => {
                    unreachable!()
                }
                Concept::Thing(Thing::Relation(relation)) => {
                    unreachable!()
                }
                Concept::Thing(Thing::Attribute(attribute)) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::Attribute(encode_attribute(
                        &attribute,
                        snapshot,
                        type_manager,
                        thing_manager,
                        DEFAULT_INCLUDE_INSTANCE_TYPES_FETCH, // TODO: May it be affected by QueryOptions?
                    )?)
                }
                Concept::Value(value) => {
                    typedb_protocol::concept_document::node::leaf::Leaf::Value(encode_value(value))
                }
            }),
        }),
        DocumentLeaf::Kind(kind) => Ok(typedb_protocol::concept_document::node::Leaf {
            leaf: Some(typedb_protocol::concept_document::node::leaf::Leaf::Kind(encode_kind(kind).into())),
        }),
    }
}

fn encode_kind(kind: Kind) -> typedb_protocol::concept_document::node::leaf::Kind {
    match kind {
        Kind::Entity => typedb_protocol::concept_document::node::leaf::Kind::Entity,
        Kind::Attribute => typedb_protocol::concept_document::node::leaf::Kind::Attribute,
        Kind::Relation => typedb_protocol::concept_document::node::leaf::Kind::Relation,
        Kind::Role => typedb_protocol::concept_document::node::leaf::Kind::Role,
    }
}

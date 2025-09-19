/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use answer::{variable_value::VariableValue, Concept, Thing, Type};
use concept::{
    thing::thing_manager::ThingManager,
    type_::{type_manager::TypeManager, TypeAPI},
};
use encoding::{graph::type_::Kind, value::value::Value};
use executor::document::{ConceptDocument, DocumentLeaf, DocumentMap, DocumentNode};
use ir::pipeline::ParameterRegistry;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::json::JSON;

#[derive(Debug, Clone)]
pub enum QueryAnswer {
    ConceptRows(Vec<HashMap<String, VariableValue<'static>>>),
    ConceptDocuments(Vec<ConceptDocument>, Arc<ParameterRegistry>),
}

macro_rules! with_rows_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_rows();
        $expr
    };
}
pub(crate) use with_rows_answer;

#[expect(unused_macros, reason = "added for symmetry")]
macro_rules! with_documents_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_documents();
        $expr
    };
}
#[expect(unused_imports, reason = "added for symmetry")]
pub(crate) use with_documents_answer;

impl QueryAnswer {
    pub fn as_rows(&self) -> &[HashMap<String, VariableValue<'static>>] {
        match self {
            Self::ConceptRows(rows) => rows,
            Self::ConceptDocuments(..) => panic!("Expected ConceptRows, got ConceptDocuments"),
        }
    }

    pub fn as_documents(&self) -> &[ConceptDocument] {
        match self {
            Self::ConceptRows(..) => {
                panic!("Expected ConceptDocuments, got ConceptRows")
            }
            Self::ConceptDocuments(documents, ..) => documents,
        }
    }

    pub fn as_documents_parameters(&self) -> &ParameterRegistry {
        match self {
            Self::ConceptRows(..) => {
                panic!("Expected ConceptDocuments, got ConceptRows")
            }
            Self::ConceptDocuments(_, parameters) => parameters,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            QueryAnswer::ConceptRows(rows) => rows.len(),
            QueryAnswer::ConceptDocuments(documents, ..) => documents.len(),
        }
    }

    pub fn as_documents_json(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
    ) -> Vec<JSON> {
        let documents = self.as_documents();
        let parameters = self.as_documents_parameters();
        let mut result = Vec::with_capacity(documents.len());

        for document in documents {
            result.push(Self::document_node_as_json(snapshot, type_manager, thing_manager, parameters, &document.root))
        }

        result
    }

    fn document_node_as_json(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        parameters: &ParameterRegistry,
        node: &DocumentNode,
    ) -> JSON {
        match &node {
            DocumentNode::List(list) => JSON::Array(
                list.list
                    .iter()
                    .map(|inner_node| {
                        Self::document_node_as_json(snapshot, type_manager, thing_manager, parameters, inner_node)
                    })
                    .collect(),
            ),
            DocumentNode::Map(map) => {
                JSON::Object(Self::document_map_as_json(snapshot, type_manager, thing_manager, parameters, map))
            }
            DocumentNode::Leaf(leaf) => Self::document_leaf_as_json(snapshot, type_manager, thing_manager, leaf),
        }
    }

    fn document_map_as_json(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        parameters: &ParameterRegistry,
        document_map: &DocumentMap,
    ) -> HashMap<Cow<'static, str>, JSON> {
        match document_map {
            DocumentMap::UserKeys(user_keys_map) => user_keys_map
                .iter()
                .map(|(parameter_id, node)| {
                    let key_name = match parameters.fetch_key(*parameter_id) {
                        Some(name) => name,
                        None => panic!("Expected parameter {parameter_id:?} string in {parameters:?}"),
                    };
                    let node_document =
                        Self::document_node_as_json(snapshot, type_manager, thing_manager, parameters, node);

                    (Cow::Owned(key_name.to_string()), node_document)
                })
                .collect(),
            DocumentMap::GeneratedKeys(generated_keys_map) => generated_keys_map
                .iter()
                .map(|(label, node)| {
                    (
                        Cow::Owned(label.scoped_name.as_str().to_string()),
                        Self::document_node_as_json(snapshot, type_manager, thing_manager, parameters, node),
                    )
                })
                .collect(),
        }
    }

    fn document_leaf_as_json(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        document_leaf: &DocumentLeaf,
    ) -> JSON {
        match document_leaf {
            DocumentLeaf::Empty => JSON::Null,
            DocumentLeaf::Concept(concept) => Self::concept_as_json(snapshot, type_manager, thing_manager, concept),
            DocumentLeaf::Kind(kind) => Self::kind_as_json(kind),
        }
    }

    fn concept_as_json(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        concept: &Concept<'_>,
    ) -> JSON {
        match concept {
            Concept::Type(type_) => JSON::String(Cow::Owned(
                match type_ {
                    Type::Entity(entity_type) => entity_type.get_label(snapshot, type_manager).expect("Expected label"),
                    Type::Relation(relation_type) => {
                        relation_type.get_label(snapshot, type_manager).expect("Expected label")
                    }
                    Type::Attribute(attribute_type) => {
                        attribute_type.get_label(snapshot, type_manager).expect("Expected label")
                    }
                    Type::RoleType(role_type) => role_type.get_label(snapshot, type_manager).expect("Expected label"),
                }
                .scoped_name
                .to_string(),
            )),
            Concept::Thing(thing) => match thing {
                Thing::Entity(_) => todo!("Unexpected entity result, requires implementation"),
                Thing::Relation(_) => todo!("Unexpected relation result, requires implementation"),
                Thing::Attribute(attribute) => Self::value_as_json(
                    &attribute
                        .get_value(snapshot, thing_manager, StorageCounters::DISABLED)
                        .expect("Expected attribute's value"),
                ),
            },
            Concept::Value(value) => Self::value_as_json(value),
        }
    }

    fn value_as_json(value: &Value<'_>) -> JSON {
        match value {
            Value::Boolean(bool) => JSON::Boolean(*bool),
            Value::Integer(integer) => JSON::Number(*integer as f64),
            Value::Double(double) => JSON::Number(*double),
            Value::String(cow) => JSON::String(Cow::Owned(match cow {
                Cow::Borrowed(s) => s.to_string(),
                Cow::Owned(s) => s.clone(),
            })),
            Value::Decimal(_) | Value::Date(_) | Value::DateTime(_) | Value::DateTimeTZ(_) | Value::Duration(_) => {
                JSON::String(Cow::Owned(value.to_string()))
            }
            Value::Struct(_) => todo!("Structs are not implemented in fetch tests"),
        }
    }

    fn kind_as_json(kind: &Kind) -> JSON {
        JSON::String(Cow::Borrowed(kind.name()))
    }
}

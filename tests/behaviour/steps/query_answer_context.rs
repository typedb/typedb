/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::{Concept, Thing, Type};

use answer::variable_value::VariableValue;
use concept::type_::TypeAPI;
use encoding::graph::type_::Kind;
use executor::document::{ConceptDocument, DocumentLeaf, DocumentList, DocumentMap, DocumentNode};

#[derive(Debug, Clone)]
pub enum QueryAnswer {
    ConceptRows(Vec<HashMap<String, VariableValue<'static>>>),
    ConceptDocuments(Vec<ConceptDocument>),
}

macro_rules! with_rows_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_rows();
        $expr
    };
}
pub(crate) use with_rows_answer;

macro_rules! with_documents_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_documents();
        $expr
    };
}
pub(crate) use with_documents_answer;

impl QueryAnswer {
    pub fn as_rows(&self) -> &Vec<HashMap<String, VariableValue<'static>>> {
        match self {
            Self::ConceptRows(rows) => rows,
            Self::ConceptDocuments(_) => panic!("Expected ConceptRows, got ConceptDocuments"),
        }
    }

    pub fn as_documents(&self) -> &Vec<ConceptDocument> {
        match self {
            Self::ConceptRows(_) => {
                panic!("Expected ConceptDocuments, got ConceptRows")
            }
            Self::ConceptDocuments(documents) => documents,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            QueryAnswer::ConceptRows(rows) => rows.len(),
            QueryAnswer::ConceptDocuments(documents) => documents.len(),
        }
    }

    pub fn as_documents_json(&self) -> Vec<String> {
        let documents = self.as_documents();
        let mut result = Vec::with_capacity(documents.len());

        for document in documents {
            result.push(Self::document_as_json(&document.root))
        }

        result
    }

    fn document_node_as_json(node: &DocumentNode) -> String {
        let mut result = "".to_owned();
        result += match &node {
            DocumentNode::List(list) => Self::document_list_as_json(list).as_str(),
            DocumentNode::Map(map) => Self::document_map_as_json(map).as_str(),
            DocumentNode::Leaf(leaf) => Self::document_leaf_as_json(leaf),
        };
        result
    }

    fn document_list_as_json(document_list: &DocumentList) -> String {
        let mut result = "[".to_owned();
        for document in document_list {
            result += Self::document_node_as_json(document).as_str();
        }
        result + "]"
    }

    fn document_map_as_json(document_map: &DocumentMap) -> String {
        todo!();
        let mut result = "[".to_owned();
        for document in document_map {
            result += Self::document_node_as_json(document).as_str();
        }
        result + "]"
    }

    fn document_leaf_as_json(document_leaf: &DocumentLeaf) -> &'static str {
        match document_leaf {
            DocumentLeaf::Empty => "none",
            DocumentLeaf::Concept(concept) => Self::concept_as_json(concept),
            DocumentLeaf::Kind(kind) => Self::kind_as_json(kind),
        }
    }

    fn concept_as_json(concept: &Concept) -> &'static str {
        match concept {
            Concept::Type(type_) => {
                match type_ {
                    Type::Entity(entity_type) => entity_type.get_label(snapshot, type_manager).expect("Expected label"),
                    Type::Relation(relation_type) => relation_type.get_label(snapshot, type_manager).expect("Expected label"),
                    Type::Attribute(attribute_type) => attribute_type.get_label(snapshot, type_manager).expect("Expected label"),
                    Type::RoleType(role_type) => role_type.get_label(snapshot, type_manager).expect("Expected label"),
                }
            }
            Concept::Thing(thing) => {
                match thing {
                    Thing::Entity(entity) => entity.type_().get_label(snapshot, type_manager).expect("Expected type label"),
                    Thing::Relation(relation) => relation.type_().get_label(snapshot, type_manager).expect("Expected type label"),
                    Thing::Attribute(attribute) => attribute.type_().get_label(snapshot, type_manager).expect("Expected type label"),
                }
            }
            Concept::Value(value) => value.to_string().as_str(),
        }
    }

    fn kind_as_json(kind: &Kind) -> &'static str {
        kind.name()
    }
}

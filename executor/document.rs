/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use core::fmt;
use std::{collections::HashMap, fmt::Formatter, sync::Arc};

use answer::Concept;
use encoding::{graph::type_::Kind, value::label::Label};
use ir::pattern::ParameterID;

#[derive(Debug, Clone)]
pub struct ConceptDocument {
    pub root: DocumentNode,
}

impl fmt::Display for ConceptDocument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Display::fmt(&self.root, f)
    }
}

#[derive(Debug, Clone)]
pub enum DocumentNode {
    List(DocumentList),
    Map(DocumentMap),
    Leaf(DocumentLeaf),
}

impl DocumentNode {
    pub(crate) fn as_list_mut(&mut self) -> &mut DocumentList {
        if let Self::List(list) = self {
            list
        } else {
            panic!("Node is not a list node.")
        }
    }
}

impl fmt::Display for DocumentNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(list) => fmt::Display::fmt(list, f),
            Self::Map(map) => fmt::Display::fmt(map, f),
            Self::Leaf(leaf) => fmt::Display::fmt(leaf, f),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DocumentMap {
    UserKeys(HashMap<ParameterID, DocumentNode>),
    GeneratedKeys(HashMap<Arc<Label<'static>>, DocumentNode>),
}

impl fmt::Display for DocumentMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        match self {
            Self::UserKeys(map) => {
                for (key, value) in map {
                    write!(f, " \"{}\": {},", key, value)?;
                }
            }
            Self::GeneratedKeys(map) => {
                for (key, value) in map {
                    write!(f, " \"{}\": {},", key, value)?;
                }
            }
        }
        write!(f, " }}")
    }
}

#[derive(Debug, Clone)]
pub struct DocumentList {
    pub list: Vec<DocumentNode>,
}

impl DocumentList {
    pub(crate) fn new() -> DocumentList {
        Self { list: Vec::new() }
    }

    pub(crate) fn new_from(list: Vec<DocumentNode>) -> DocumentList {
        Self { list }
    }
}

impl fmt::Display for DocumentList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for node in &self.list {
            write!(f, " {},", node)?
        }
        write!(f, " ]")
    }
}

#[derive(Debug, Clone)]
pub enum DocumentLeaf {
    Empty,
    Concept(Concept<'static>),
    Kind(Kind),
}

impl fmt::Display for DocumentLeaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DocumentLeaf::Empty => write!(f, "empty"),
            DocumentLeaf::Concept(concept) => write!(f, "{concept}"),
            DocumentLeaf::Kind(kind) => write!(f, "{kind}"),
        }
    }
}

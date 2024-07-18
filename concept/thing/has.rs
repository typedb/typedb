/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use encoding::graph::thing::edge::{ThingEdgeHas, ThingEdgeHasReverse};

use crate::thing::{attribute::Attribute, object::Object};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Has<'a> {
    Edge(ThingEdgeHas<'a>),
    EdgeReverse(ThingEdgeHasReverse<'a>),
}

impl<'a> Has<'a> {
    pub(crate) fn new_from_edge(edge: ThingEdgeHas<'a>) -> Self {
        Self::Edge(edge)
    }

    fn new_from_edge_reverse(edge: ThingEdgeHasReverse<'a>) -> Self {
        Self::EdgeReverse(edge)
    }

    pub fn owner(&'a self) -> Object<'a> {
        match self {
            Has::Edge(edge) => Object::new(edge.from()),
            Has::EdgeReverse(edge_reverse) => Object::new(edge_reverse.to()),
        }
    }

    pub fn attribute(&'a self) -> Attribute<'a> {
        match self {
            Has::Edge(edge) => Attribute::new(edge.to()),
            Has::EdgeReverse(edge_reverse) => Attribute::new(edge_reverse.from()),
        }
    }
}

impl<'a> Display for Has<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} has {}", self.owner(), self.attribute())
    }
}

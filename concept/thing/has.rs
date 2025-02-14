/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use encoding::graph::thing::edge::{ThingEdgeHas, ThingEdgeHasReverse};
use lending_iterator::higher_order::Hkt;

use crate::thing::{attribute::Attribute, object::Object, ThingAPI};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Has {
    Edge(ThingEdgeHas),
    EdgeReverse(ThingEdgeHasReverse),
}

impl<'a> Has {
    pub(crate) fn new_from_edge(edge: ThingEdgeHas) -> Self {
        Self::Edge(edge)
    }

    pub(crate) fn new_from_edge_reverse(edge: ThingEdgeHasReverse) -> Self {
        Self::EdgeReverse(edge)
    }

    pub fn owner(&'a self) -> Object {
        match self {
            Has::Edge(edge) => Object::new(edge.from()),
            Has::EdgeReverse(edge_reverse) => Object::new(edge_reverse.to()),
        }
    }

    pub fn attribute(&'a self) -> Attribute {
        match self {
            Has::Edge(edge) => Attribute::new(edge.to()),
            Has::EdgeReverse(edge_reverse) => Attribute::new(edge_reverse.from()),
        }
    }

    pub fn into_owner_attribute(self) -> (Object, Attribute) {
        match self {
            Has::Edge(edge) => (Object::new(edge.from()), Attribute::new(edge.to())),
            Has::EdgeReverse(edge_reverse) => (Object::new(edge_reverse.to()), Attribute::new(edge_reverse.from())),
        }
    }
}

impl Hkt for Has {
    type HktSelf<'a> = Has;
}

impl fmt::Display for Has {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} has {}", self.owner(), self.attribute())
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use encoding::graph::thing::edge::{ThingEdgeHas, ThingEdgeHasReverse};
use lending_iterator::higher_order::Hkt;

use crate::thing::{attribute::Attribute, object::Object, ThingAPI};

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

    pub fn into_owner_attribute(self) -> (Object<'a>, Attribute<'a>) {
        match self {
            Has::Edge(edge) => (Object::new(edge.clone().into_from()), Attribute::new(edge.clone().into_to())),
            Has::EdgeReverse(edge_reverse) => {
                (Object::new(edge_reverse.clone().into_to()), Attribute::new(edge_reverse.into_from()))
            }
        }
    }
}

impl Hkt for Has<'static> {
    type HktSelf<'a> = Has<'a>;
}

impl<'a> Display for Has<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} has {}", self.owner(), self.attribute())
    }
}

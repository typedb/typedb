/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use encoding::graph::thing::vertex_attribute::AttributeVertex;
use encoding::property::value_type::ValueType;

use crate::ConceptAPI;
use crate::thing::{AttributeAPI, ThingAPI};
use crate::thing::value::Value;

#[derive(Clone, Debug)]
pub struct Attribute<'a> {
    vertex: AttributeVertex<'a>,
    value: Option<Value>, // TODO: if we end up doing traversals over Vertex instead of Concept, we could embed the Value cache into the AttributeVertex
}

impl<'a> Attribute<'a> {
    fn new(vertex: AttributeVertex<'a>) -> Self {
        Attribute { vertex: vertex, value: None }
    }
}

impl<'a> ThingAPI<'a> for Attribute<'a> {}

impl<'a> ConceptAPI<'a> for Attribute<'a> {}

impl<'a> AttributeAPI<'a> for Attribute<'a> {
    fn vertex(&self) -> &AttributeVertex<'a> {
        &self.vertex
    }

    fn into_owned(self) -> Attribute<'static> {
        Attribute::new(self.vertex.into_owned())
    }

    fn value_type(&self) -> ValueType {
        self.vertex.value_type()
    }

    fn value(&self) -> Value {
        todo!()
    }
}

impl<'a> PartialEq<Self> for Attribute<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.vertex().eq(other.vertex())
    }
}

impl<'a> Eq for Attribute<'a> {}

/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use encoding::graph::thing::vertex::{AttributeVertex, ObjectVertex};
use encoding::graph::type_::vertex::TypeVertex;

pub mod type_;
pub mod thing;
pub mod error;
pub mod iterator;

trait Concept<'a>: Eq + PartialEq { }

trait Type<'a>: Concept<'a> {
    fn vertex(&'a self) -> &TypeVertex<'a>;
}

trait Thing<'a>: Concept<'a> {}

trait Object<'a>: Thing<'a> {
    fn vertex(&'a self) -> &ObjectVertex<'a>;
}

trait Attribute<'a>: Thing<'a> {
    fn vertex(&'a self) -> &AttributeVertex<'a>;
}

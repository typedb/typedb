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

use encoding::graph::thing::vertex_object::ObjectVertex;
use crate::thing::object::Object;

pub mod attribute;
pub mod entity;
mod relation;
pub mod thing_manager;
pub mod value;
pub mod object;

pub trait ObjectAPI<'a> {
    fn as_reference<'this>(&'this self) -> impl ObjectAPI<'this>;

    fn vertex<'this>(&'this self) -> ObjectVertex<'this>;

    fn into_vertex(self) -> ObjectVertex<'a>;
}
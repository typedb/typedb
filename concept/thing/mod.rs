/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::thing::vertex_object::ObjectVertex;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use crate::{ConceptStatus, GetStatus};
use crate::error::ConceptWriteError;
use crate::thing::thing_manager::ThingManager;

pub mod attribute;
pub mod entity;
mod relation;
pub mod thing_manager;
pub mod value;
pub mod object;

pub trait ThingAPI<'a> {
    fn get_status<'m>(&self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>) -> ConceptStatus;

    fn delete<'m>(self, thing_manager: &'m ThingManager<'_, impl WritableSnapshot>) -> Result<(), ConceptWriteError>;
}

pub trait ObjectAPI<'a> {
    fn vertex<'this>(&'this self) -> ObjectVertex<'this>;

    fn into_vertex(self) -> ObjectVertex<'a>;
}

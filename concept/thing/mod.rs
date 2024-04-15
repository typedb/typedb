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

///
/// TODO: one thing to consider is that we have a general issue with update-delete race conditions
///       For example, if within a single transaction, a thread adds an ownership to a concept, and another deletes it
/// Note: Some operations (such as relation player add + index update) are already locked to be atomic
///

pub trait ThingAPI<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<'_, impl WritableSnapshot>);

    // TODO: implementers could cache the status in a OnceCell if we do many operations on the same Thing at once
    fn get_status<'m>(&self, thing_manager: &'m ThingManager<'_, impl ReadableSnapshot>) -> ConceptStatus;

    fn delete<'m>(self, thing_manager: &'m ThingManager<'_, impl WritableSnapshot>) -> Result<(), ConceptWriteError>;
}

pub trait ObjectAPI<'a> {
    fn vertex<'this>(&'this self) -> ObjectVertex<'this>;

    fn into_vertex(self) -> ObjectVertex<'a>;
}

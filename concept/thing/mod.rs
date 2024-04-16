/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::byte_reference::ByteReference;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::value::decode_value_u64;
use storage::key_value::StorageKeyReference;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{ConceptStatus, edge_iterator, error::ConceptWriteError, thing::thing_manager::ThingManager};
use crate::thing::object::Object;

pub mod attribute;
pub mod entity;
pub mod object;
mod relation;
pub mod thing_manager;
pub mod value;

///
/// TODO: one thing to consider is that we have a general issue with update-delete race conditions
///       For example, if within a single transaction, a thread adds an ownership to a concept, and another deletes it
/// Note: Some operations (such as relation player add + index update) are already locked to be atomic
///

pub trait ThingAPI<'a> {
    fn set_modified(&self, thing_manager: &ThingManager<impl WritableSnapshot>);

    // TODO: implementers could cache the status in a OnceCell if we do many operations on the same Thing at once
    fn get_status<'m>(&self, thing_manager: &'m ThingManager<impl ReadableSnapshot>) -> ConceptStatus;

    fn delete(self, thing_manager: &ThingManager<impl WritableSnapshot>) -> Result<(), ConceptWriteError>;
}

pub trait ObjectAPI<'a> {
    fn vertex(&self) -> ObjectVertex<'_>;

    fn into_vertex(self) -> ObjectVertex<'a>;
}

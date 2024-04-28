/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::io::Read;
use bytes::byte_array::ByteArray;
use encoding::graph::thing::vertex_attribute::AttributeID;
use encoding::graph::thing::vertex_object::ObjectVertex;
use encoding::value::value_type::ValueType;
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{ConceptStatus, error::ConceptWriteError, thing::thing_manager::ThingManager};
use crate::error::ConceptReadError;

pub mod attribute;
pub mod entity;
pub mod object;
pub mod relation;
pub mod thing_manager;
pub mod value;

pub trait ThingAPI<'a> {
    fn set_modified<Snapshot: WritableSnapshot>(&self, thing_manager: &ThingManager<Snapshot>);

    // TODO: implementers could cache the status in a OnceCell if we do many operations on the same Thing at once
    fn get_status<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &'m ThingManager<Snapshot>
    ) -> ConceptStatus;

    fn errors<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>
    )  -> Result<Vec<ConceptWriteError>, ConceptReadError>;

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>
    ) -> Result<(), ConceptWriteError>;
}

pub trait ObjectAPI<'a>: ThingAPI<'a> {
    fn vertex(&self) -> ObjectVertex<'_>;

    fn into_vertex(self) -> ObjectVertex<'a>;
}

// TODO: where do these belong? They're encodings of values we store for keys
pub(crate) fn decode_attribute_ids(value_type: ValueType, bytes: &[u8]) -> impl Iterator<Item=AttributeID> + '_ {
    let chunk_size = AttributeID::value_type_encoding_length(value_type);
    let chunks_iter = bytes.chunks_exact(chunk_size);
    debug_assert!(chunks_iter.remainder().is_empty());
    chunks_iter.map(move |chunk| AttributeID::new(value_type, chunk))
}

pub(crate) fn encode_attribute_ids(attribute_ids: impl Iterator<Item=AttributeID>) -> ByteArray<{ BUFFER_VALUE_INLINE }> {
    todo!()
}

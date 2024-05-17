/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::byte_array::ByteArray;
use encoding::{
    graph::thing::{edge::ThingEdgeHas, vertex_attribute::AttributeID, vertex_object::ObjectVertex},
    value::value_type::ValueType,
};
use resource::constants::snapshot::BUFFER_VALUE_INLINE;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    thing::{attribute::Attribute, object::HasAttributeIterator, thing_manager::ThingManager, value::Value},
    type_::{
        attribute_type::AttributeType,
        owns::{Owns, OwnsAnnotation},
        type_manager::TypeManager,
        ObjectTypeAPI, Ordering, OwnerAPI,
    },
    ConceptStatus,
};

pub mod attribute;
pub mod entity;
pub mod object;
pub mod relation;
pub mod statistics;
pub mod thing_manager;
pub mod value;

pub trait ThingAPI<'a> {
    fn set_modified<Snapshot: WritableSnapshot>(&self, snapshot: &mut Snapshot, thing_manager: &ThingManager<Snapshot>);

    // TODO: implementers could cache the status in a OnceCell if we do many operations on the same Thing at once
    fn get_status<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &'m ThingManager<Snapshot>,
    ) -> ConceptStatus;

    fn errors<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<Vec<ConceptWriteError>, ConceptReadError>;

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
    ) -> Result<(), ConceptWriteError>;
}

pub trait ObjectAPI<'a>: ThingAPI<'a> + Clone {
    fn vertex(&self) -> ObjectVertex<'_>;

    fn into_vertex(self) -> ObjectVertex<'a>;

    fn type_(&self) -> impl ObjectTypeAPI<'static>;

    fn has_attribute<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        attribute_type: AttributeType<'static>,
        value: Value<'_>,
    ) -> Result<bool, ConceptReadError> {
        thing_manager.has_attribute(snapshot, self, attribute_type, value)
    }

    fn get_has<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &'m Snapshot,
        thing_manager: &'m ThingManager<Snapshot>,
    ) -> HasAttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT }> {
        thing_manager.get_has_unordered(snapshot, self)
    }

    fn get_has_type<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &'m Snapshot,
        thing_manager: &'m ThingManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<HasAttributeIterator<'m, { ThingEdgeHas::LENGTH_PREFIX_FROM_OBJECT_TO_TYPE }>, ConceptReadError> {
        thing_manager.get_has_type_unordered(snapshot, self, attribute_type)
    }

    fn set_has_unordered<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        attribute: Attribute<'_>,
    ) -> Result<(), ConceptWriteError> {
        if !thing_manager.object_exists(snapshot, self)? {
            return Err(ConceptWriteError::SetHasOnDeleted {});
        }

        let owns = self
            .get_type_owns(snapshot, thing_manager.type_manager(), attribute.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        match owns
            .get_ordering(snapshot, thing_manager.type_manager())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
            Ordering::Unordered => (),
            Ordering::Ordered => todo!("throw a good error"),
        }

        for annotation in &owns.get_annotations(snapshot, thing_manager.type_manager())? {
            match annotation {
                OwnsAnnotation::Distinct(_) => todo!(),
                OwnsAnnotation::Unique(_) => todo!(),
                OwnsAnnotation::Key(_) => {
                    if self
                        .get_has_type(snapshot, thing_manager, owns.attribute())
                        .map_err(|error| ConceptWriteError::ConceptRead { source: error })?
                        .next()
                        .is_some()
                    {
                        return Err(ConceptWriteError::SetHasMultipleKeys {});
                    }
                }
                OwnsAnnotation::Cardinality(_) => todo!(),
            }
        }

        thing_manager.set_has_unordered(snapshot, self, attribute.as_reference());
        Ok(())
    }

    fn unset_has_unordered<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        attribute: Attribute<'_>,
    ) -> Result<(), ConceptWriteError> {
        let owns = self
            .get_type_owns(snapshot, thing_manager.type_manager(), attribute.type_())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        match owns
            .get_ordering(snapshot, thing_manager.type_manager())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
            Ordering::Unordered => (),
            Ordering::Ordered => todo!("throw good error"),
        }
        thing_manager.unset_has(snapshot, self, attribute);
        Ok(())
    }

    fn set_has_ordered<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        attribute_type: AttributeType<'static>,
        attributes: Vec<Attribute<'_>>,
    ) -> Result<(), ConceptWriteError> {
        if !thing_manager.object_exists(snapshot, self)? {
            return Err(ConceptWriteError::SetHasOnDeleted {});
        }

        let owns = self
            .get_type_owns(snapshot, thing_manager.type_manager(), attribute_type.clone())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        match owns
            .get_ordering(snapshot, thing_manager.type_manager())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?
        {
            Ordering::Unordered => todo!("throw good error"),
            Ordering::Ordered => (),
        }

        // 1. get owned list
        let attributes = thing_manager
            .get_has_type_ordered(snapshot, self, attribute_type.clone())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;

        // 2. Delete existing but no-longer necessary has, and add new ones, with the correct counts (!)
        todo!()

        // // 3. Overwrite owned list
        // thing_manager.set_has_ordered(self.as_reference(), attribute_type, attributes);
        // Ok(())
    }

    fn unset_has_ordered<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        let owns = self
            .get_type_owns(snapshot, thing_manager.type_manager(), attribute_type)
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        let ordering = owns
            .get_ordering(snapshot, thing_manager.type_manager())
            .map_err(|err| ConceptWriteError::ConceptRead { source: err })?;
        match ordering {
            Ordering::Unordered => {
                todo!("throw good error")
            }
            Ordering::Ordered => {
                // TODO: 1. get owned list 2. Delete each ownership has 3. delete owned list
                todo!()
            }
        }
    }

    fn get_type_owns<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Owns<'m>, ConceptReadError> {
        let owns = self.type_().get_owns_attribute(snapshot, type_manager, attribute_type)?;
        match owns {
            None => {
                todo!("throw useful schema error")
            }
            Some(owns) => Ok(owns),
        }
    }
}

// TODO: where do these belong? They're encodings of values we store for keys
pub(crate) fn decode_attribute_ids(value_type: ValueType, bytes: &[u8]) -> impl Iterator<Item = AttributeID> + '_ {
    let chunk_size = AttributeID::value_type_encoding_length(value_type);
    let chunks_iter = bytes.chunks_exact(chunk_size);
    debug_assert!(chunks_iter.remainder().is_empty());
    chunks_iter.map(move |chunk| AttributeID::new(value_type, chunk))
}

pub(crate) fn encode_attribute_ids(attribute_ids: impl Iterator<Item = AttributeID>) -> ByteArray<BUFFER_VALUE_INLINE> {
    todo!()
}

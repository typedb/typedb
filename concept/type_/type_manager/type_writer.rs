/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use bytes::byte_array::ByteArray;
use encoding::{
    graph::{
        definition::{definition_key::DefinitionKey, r#struct::StructDefinition, DefinitionValueEncoding},
        type_::{
            edge::TypeEdgeEncoding,
            index::{LabelToTypeVertexIndex, NameToStructDefinitionIndex},
            property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
            vertex::TypeVertexEncoding,
        },
    },
    value::{label::Label, value_type::ValueType},
    AsBytes, Keyable,
};
use resource::profile::StorageCounters;
use storage::snapshot::WritableSnapshot;

use crate::{
    error::ConceptWriteError,
    type_::{
        attribute_type::AttributeType, owns::Owns, sub::Sub, type_manager::type_reader::TypeReader, Ordering, TypeAPI,
    },
};

pub struct TypeWriter<Snapshot: WritableSnapshot> {
    snapshot: PhantomData<Snapshot>,
}

// TODO: Make everything pub(super) and make this submodule of type_manager.
impl<Snapshot: WritableSnapshot> TypeWriter<Snapshot> {
    pub(crate) fn storage_insert_struct(
        snapshot: &mut Snapshot,
        definition_key: DefinitionKey,
        struct_definition: StructDefinition,
    ) {
        let index_key = NameToStructDefinitionIndex::build(struct_definition.name.as_str());
        snapshot.put_val(index_key.into_storage_key().into_owned_array(), ByteArray::copy(definition_key.bytes()));
        snapshot.insert_val(
            definition_key.into_storage_key().into_owned_array(),
            struct_definition.into_bytes().unwrap().into_array(),
        );
    }

    pub(crate) fn storage_delete_struct(snapshot: &mut Snapshot, definition_key: &DefinitionKey) {
        let existing_struct = TypeReader::get_struct_definition(snapshot, definition_key.clone());
        if let Ok(struct_definition) = existing_struct {
            let index_key = NameToStructDefinitionIndex::build(struct_definition.name.as_str());
            snapshot.delete(definition_key.clone().into_storage_key().into_owned_array());
            snapshot.delete(index_key.into_storage_key().into_owned_array());
        }
    }

    // Basic vertex type operations
    pub(crate) fn storage_put_label<T: TypeAPI>(snapshot: &mut Snapshot, type_: T, label: &Label) {
        debug_assert!(TypeReader::get_label(snapshot, type_).unwrap().is_none());
        Self::storage_put_type_vertex_property(snapshot, type_, Some(label.clone()));

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(&*type_.into_vertex().to_bytes());
        snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    pub(crate) fn storage_delete_vertex(snapshot: &mut Snapshot, type_: impl TypeAPI) {
        snapshot.delete(type_.vertex().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_vertex(snapshot: &mut Snapshot, type_: impl TypeAPI) {
        debug_assert!(snapshot
            .contains(type_.vertex().into_storage_key().as_reference(), StorageCounters::DISABLED)
            .unwrap_or(false));
        snapshot.unput(type_.vertex().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding + Clone,
    {
        let canonical_key = capability.to_canonical_type_edge().into_storage_key();
        let reverse_key = capability.to_reverse_type_edge().into_storage_key();
        debug_assert!(snapshot.contains(canonical_key.as_reference(), StorageCounters::DISABLED).unwrap_or(false));
        debug_assert!(snapshot.contains(reverse_key.as_reference(), StorageCounters::DISABLED).unwrap_or(false));
        snapshot.unput(canonical_key.into_owned_array());
        snapshot.unput(reverse_key.into_owned_array());
    }

    pub(crate) fn storage_delete_label(snapshot: &mut Snapshot, type_: impl TypeAPI) {
        let existing_label = TypeReader::get_label(snapshot, type_).unwrap();
        if let Some(label) = existing_label {
            Self::storage_delete_type_vertex_property::<Label>(snapshot, type_);
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_unput_label<T: TypeAPI>(snapshot: &mut Snapshot, type_: T, label: &Label) {
        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        Self::storage_unput_type_vertex_property(snapshot, type_, Some(label.clone()));
        let vertex_value = ByteArray::from(&*type_.into_vertex().to_bytes());
        snapshot.unput_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    pub(crate) fn storage_put_supertype<T>(snapshot: &mut Snapshot, subtype: T, supertype: T)
    where
        T: TypeAPI,
    {
        let sub_edge = Sub::from_vertices(subtype, supertype);
        snapshot.put(sub_edge.to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(sub_edge.to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_may_delete_supertype<T>(
        snapshot: &mut Snapshot,
        subtype: T,
    ) -> Result<(), Box<ConceptWriteError>>
    where
        T: TypeAPI,
    {
        let supertype = TypeReader::get_supertype(snapshot, subtype)?;
        if let Some(supertype) = supertype {
            let sub_edge = Sub::from_vertices(subtype, supertype);
            snapshot.delete(sub_edge.to_canonical_type_edge().into_storage_key().into_owned_array());
            snapshot.delete(sub_edge.to_reverse_type_edge().into_storage_key().into_owned_array());
        }
        Ok(())
    }

    pub(crate) fn storage_set_value_type(snapshot: &mut Snapshot, attribute: AttributeType, value_type: ValueType) {
        TypeWriter::storage_put_type_vertex_property(snapshot, attribute, Some(value_type));
    }

    pub(crate) fn storage_unset_value_type(snapshot: &mut Snapshot, attribute: AttributeType) {
        TypeWriter::storage_delete_type_vertex_property::<ValueType>(snapshot, attribute);
    }

    // Type edges
    pub(crate) fn storage_put_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding + Clone,
    {
        snapshot.put(capability.to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(capability.to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding + Clone,
    {
        snapshot.delete(capability.to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.delete(capability.to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_insert_type_vertex_property<P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.insert_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.insert(key.into_owned_array())
        }
    }

    pub(crate) fn storage_put_type_vertex_property<P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.put_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.put(key.into_owned_array())
        }
    }

    pub(crate) fn storage_delete_type_vertex_property<P>(snapshot: &mut Snapshot, vertex: impl TypeVertexEncoding)
    where
        P: TypeVertexPropertyEncoding,
    {
        snapshot.delete(P::build_key(vertex).into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_type_vertex_property<P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.unput_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.unput(key.into_owned_array())
        }
    }

    pub(crate) fn storage_set_owns_ordering(snapshot: &mut Snapshot, owns: Owns, ordering: Ordering) {
        Self::storage_put_type_edge_property(snapshot, owns, Some(ordering))
    }

    pub(crate) fn storage_insert_type_edge_property<P>(
        snapshot: &mut Snapshot,
        edge: impl TypeEdgeEncoding,
        property_opt: Option<P>,
    ) where
        P: TypeEdgePropertyEncoding,
    {
        let key = P::build_key(edge).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.insert_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.insert(key.into_owned_array())
        }
    }

    pub(crate) fn storage_put_type_edge_property<P>(
        snapshot: &mut Snapshot,
        edge: impl TypeEdgeEncoding,
        property_opt: Option<P>,
    ) where
        P: TypeEdgePropertyEncoding,
    {
        let key = P::build_key(edge).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.put_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.put(key.into_owned_array())
        }
    }

    pub(crate) fn storage_delete_type_edge_property<P>(snapshot: &mut Snapshot, edge: impl TypeEdgeEncoding)
    where
        P: TypeEdgePropertyEncoding,
    {
        snapshot.delete(P::build_key(edge).into_storage_key().into_owned_array());
    }
}

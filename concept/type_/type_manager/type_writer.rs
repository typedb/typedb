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
            edge::{TypeEdge, TypeEdgeEncoding},
            index::{LabelToTypeVertexIndex, NameToStructDefinitionIndex},
            property::{TypeEdgePropertyEncoding, TypeVertexPropertyEncoding},
            vertex::TypeVertexEncoding,
        },
    },
    value::{label::Label, string_bytes::StringBytes, value_type::ValueType},
    AsBytes, Keyable,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_range::KeyRange, snapshot::WritableSnapshot};

use crate::{
    error::ConceptWriteError,
    type_::{
        attribute_type::AttributeType, owns::Owns, relates::Relates, relation_type::RelationType, role_type::RoleType,
        sub::Sub, type_manager::type_reader::TypeReader, EdgeOverride, Ordering, TypeAPI,
    },
};

pub struct TypeWriter<Snapshot: WritableSnapshot> {
    snapshot: PhantomData<Snapshot>,
}

// TODO: Make everything pub(super) and make this submodule of type_manager.
impl<Snapshot: WritableSnapshot> TypeWriter<Snapshot> {
    pub(crate) fn storage_insert_struct(
        snapshot: &mut Snapshot,
        definition_key: DefinitionKey<'static>,
        struct_definition: StructDefinition,
    ) {
        let index_key = NameToStructDefinitionIndex::build(struct_definition.name.as_str());
        snapshot
            .put_val(index_key.into_storage_key().into_owned_array(), ByteArray::copy(definition_key.bytes().bytes()));
        snapshot.insert_val(
            definition_key.into_storage_key().into_owned_array(),
            struct_definition.into_bytes().unwrap().into_array(),
        );
    }

    pub(crate) fn storage_delete_struct(snapshot: &mut Snapshot, definition_key: &DefinitionKey<'static>) {
        let existing_struct = TypeReader::get_struct_definition(snapshot, definition_key.clone());
        if let Ok(struct_definition) = existing_struct {
            let index_key = NameToStructDefinitionIndex::build(struct_definition.name.as_str());
            snapshot.delete(definition_key.clone().into_storage_key().into_owned_array());
            snapshot.delete(index_key.into_storage_key().into_owned_array());
        }
    }

    // Basic vertex type operations
    pub(crate) fn storage_put_label<T: TypeAPI<'static>>(snapshot: &mut Snapshot, type_: T, label: &Label<'_>) {
        debug_assert!(TypeReader::get_label(snapshot, type_.clone()).unwrap().is_none());
        Self::storage_put_type_vertex_property(snapshot, type_.clone(), Some(label.clone().into_owned()));

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(type_.into_vertex().bytes());
        snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    pub(crate) fn storage_delete_vertex(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        snapshot.delete(type_.vertex().as_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_vertex(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        debug_assert!(snapshot.contains(type_.vertex().as_storage_key().as_reference()).unwrap_or(false));
        snapshot.unput(type_.vertex().as_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding<'static> + Clone,
    {
        let canonical_key = capability.clone().to_canonical_type_edge().into_storage_key();
        let reverse_key = capability.to_reverse_type_edge().into_storage_key();
        debug_assert!(snapshot.contains(canonical_key.as_reference()).unwrap_or(false));
        debug_assert!(snapshot.contains(reverse_key.as_reference()).unwrap_or(false));
        snapshot.unput(canonical_key.into_owned_array());
        snapshot.unput(reverse_key.into_owned_array());
    }

    pub(crate) fn storage_delete_label(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
        let existing_label = TypeReader::get_label(snapshot, type_.clone()).unwrap();
        if let Some(label) = existing_label {
            Self::storage_delete_type_vertex_property::<Label<'_>>(snapshot, type_);
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_unput_label<T: TypeAPI<'static>>(snapshot: &mut Snapshot, type_: T, label: &Label<'_>) {
        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        Self::storage_unput_type_vertex_property(snapshot, type_.clone(), Some(label.clone().into_owned()));
        let vertex_value = ByteArray::from(type_.into_vertex().bytes());
        snapshot.unput_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    pub(crate) fn storage_put_supertype<T>(snapshot: &mut Snapshot, subtype: T, supertype: T)
    where
        T: TypeAPI<'static>,
    {
        let sub_edge = Sub::from_vertices(subtype.clone(), supertype.clone());
        snapshot.put(sub_edge.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(sub_edge.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_may_delete_supertype<T>(snapshot: &mut Snapshot, subtype: T) -> Result<(), ConceptWriteError>
    where
        T: TypeAPI<'static>,
    {
        let supertype = TypeReader::get_supertype(snapshot, subtype.clone())?;
        if let Some(supertype) = supertype {
            let sub_edge = Sub::from_vertices(subtype.clone(), supertype.clone());
            snapshot.delete(sub_edge.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
            snapshot.delete(sub_edge.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
        }
        Ok(())
    }

    pub(crate) fn storage_set_value_type(
        snapshot: &mut Snapshot,
        attribute: AttributeType<'static>,
        value_type: ValueType,
    ) {
        TypeWriter::storage_put_type_vertex_property(snapshot, attribute, Some(value_type));
    }

    pub(crate) fn storage_unset_value_type(snapshot: &mut Snapshot, attribute: AttributeType<'static>) {
        TypeWriter::storage_delete_type_vertex_property::<ValueType>(snapshot, attribute);
    }

    // Type edges
    pub(crate) fn storage_put_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding<'static> + Clone,
    {
        snapshot.put(capability.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(capability.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_edge<EDGE>(snapshot: &mut Snapshot, capability: EDGE)
    where
        EDGE: TypeEdgeEncoding<'static> + Clone,
    {
        snapshot.delete(capability.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.delete(capability.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_insert_type_vertex_property<'a, P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding<'a>,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding<'a>,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.insert_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.insert(key.into_owned_array())
        }
    }

    pub(crate) fn storage_put_type_vertex_property<'a, P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding<'a>,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding<'a>,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.put_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.put(key.into_owned_array())
        }
    }

    pub(crate) fn storage_delete_type_vertex_property<'a, P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding<'a>,
    ) where
        P: TypeVertexPropertyEncoding<'a>,
    {
        snapshot.delete(P::build_key(vertex).into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_unput_type_vertex_property<'a, P>(
        snapshot: &mut Snapshot,
        vertex: impl TypeVertexEncoding<'a>,
        property_opt: Option<P>,
    ) where
        P: TypeVertexPropertyEncoding<'a>,
    {
        let key = P::build_key(vertex).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.unput_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.unput(key.into_owned_array())
        }
    }

    pub(crate) fn storage_set_type_edge_overridden<E>(snapshot: &mut Snapshot, edge: E, overridden: E)
    where
        E: TypeEdgeEncoding<'static>,
    {
        let overridden_to = EdgeOverride::<E> { overrides: overridden };
        Self::storage_put_type_edge_property(snapshot, edge, Some(overridden_to))
    }

    pub(crate) fn storage_delete_type_edge_overridden<E>(snapshot: &mut Snapshot, edge: E)
    where
        E: TypeEdgeEncoding<'static>,
    {
        Self::storage_delete_type_edge_property::<EdgeOverride<E>>(snapshot, edge)
    }

    pub(crate) fn storage_set_owns_ordering(snapshot: &mut Snapshot, owns: Owns<'_>, ordering: Ordering) {
        Self::storage_put_type_edge_property(snapshot, owns, Some(ordering))
    }

    pub(crate) fn storage_insert_type_edge_property<'a, P>(
        snapshot: &mut Snapshot,
        edge: impl TypeEdgeEncoding<'a>,
        property_opt: Option<P>,
    ) where
        P: TypeEdgePropertyEncoding<'a>,
    {
        let key = P::build_key(edge).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.insert_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.insert(key.into_owned_array())
        }
    }

    pub(crate) fn storage_put_type_edge_property<'a, P>(
        snapshot: &mut Snapshot,
        edge: impl TypeEdgeEncoding<'a>,
        property_opt: Option<P>,
    ) where
        P: TypeEdgePropertyEncoding<'a>,
    {
        let key = P::build_key(edge).into_storage_key();
        if let Some(property) = property_opt {
            let value = property.to_value_bytes().unwrap();
            snapshot.put_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.put(key.into_owned_array())
        }
    }

    pub(crate) fn storage_delete_type_edge_property<'a, P>(snapshot: &mut Snapshot, edge: impl TypeEdgeEncoding<'a>)
    where
        P: TypeEdgePropertyEncoding<'a>,
    {
        snapshot.delete(P::build_key(edge).into_storage_key().into_owned_array());
    }
}

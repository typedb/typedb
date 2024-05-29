/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use bytes::byte_array::ByteArray;
use encoding::graph::type_::index::LabelToTypeVertexIndex;
use encoding::graph::type_::property::{build_property_type_label, build_property_type_value_type, EncodableTypeEdgeProperty};
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::edge::EncodableParametrisedTypeEdge;
use encoding::graph::type_::vertex::EncodableTypeVertex;
use encoding::value::label::Label;
use encoding::value::value_type::{ValueType, ValueTypeBytes};
use storage::snapshot::WritableSnapshot;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::KindAPI;
use crate::type_::type_manager::type_reader::TypeReader;
use crate::type_::{EdgeOverride, Ordering, TypeAPI};
use crate::type_::attribute_type::AttributeType;
use crate::type_::owns::Owns;
use crate::type_::relates::Relates;
use crate::type_::type_manager::encoding_helper::EdgeSub;

pub struct TypeWriter<Snapshot: WritableSnapshot> {
    snapshot: PhantomData<Snapshot>,
}

// TODO: Make everything pub(super) and make this submodule of type_manager.
impl<Snapshot: WritableSnapshot> TypeWriter<Snapshot> {

    // Basic vertex type operations
    pub(crate) fn storage_put_label<T: KindAPI<'static>>(snapshot: &mut Snapshot, type_: T, label: &Label<'_>) {
        debug_assert!(TypeReader::get_label(snapshot, type_.clone()).unwrap().is_none());
        let vertex_to_label_key = build_property_type_label(type_.clone().into_vertex());
        let label_value = ByteArray::from(label.scoped_name().bytes());
        snapshot.put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(type_.into_vertex().bytes());
        snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
    }

    // TODO: why is this "may delete label"?
    pub(crate) fn storage_delete_label(snapshot: &mut Snapshot, type_: impl KindAPI<'static>) {
        let existing_label = TypeReader::get_label(snapshot, type_.clone()).unwrap();
        if let Some(label) = existing_label {
            let vertex_to_label_key = build_property_type_label(type_.into_vertex());
            snapshot.delete(vertex_to_label_key.into_storage_key().into_owned_array());
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
    }

    pub(crate) fn storage_put_supertype<K>(snapshot: &mut Snapshot, subtype: K, supertype: K)
        where K: KindAPI<'static>
    {
        let sub_edge = EdgeSub::from_vertices(subtype.clone(), supertype.clone());
        snapshot.put(sub_edge.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(sub_edge.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_supertype<T>(snapshot: &mut Snapshot, subtype: T)
    where T: KindAPI<'static>
    {
        let supertype = TypeReader::get_supertype(snapshot, subtype.clone()).unwrap().unwrap();
        let sub_edge = EdgeSub::from_vertices(subtype.clone(), supertype.clone());
        snapshot.delete(sub_edge.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.delete(sub_edge.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_set_value_type(
        snapshot: &mut Snapshot,
        attribute: AttributeType<'static>,
        value_type: ValueType,
    ) {
        let property_key =
            build_property_type_value_type(attribute.into_vertex()).into_storage_key().into_owned_array();
        let property_value = ByteArray::copy(ValueTypeBytes::build(&value_type).into_bytes());
        snapshot.put_val(property_key, property_value);
    }

    // Type edges
    pub(crate) fn storage_put_relates(
        snapshot: &mut Snapshot,
        relation: RelationType<'static>,
        role: RoleType<'static>,
    ) {
        let relates = Relates::from_vertices(relation, role);
        snapshot.put(relates.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(relates.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_relates(
        snapshot: &mut Snapshot,
        relation: RelationType<'static>,
        role: RoleType<'static>,
    ) {
        let relates = Relates::from_vertices(relation, role);
        snapshot.delete(relates.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.delete(relates.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_put_interface_impl<IMPL>(
        snapshot: &mut Snapshot,
        implementation: IMPL,
    )
    where
        IMPL: EncodableParametrisedTypeEdge<'static> + Clone,
    {
        snapshot.put(implementation.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.put(implementation.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_interface_impl<IMPL>(
        snapshot: &mut Snapshot,
        implementation: IMPL,
    )
        where IMPL: EncodableParametrisedTypeEdge<'static> + Clone
    {
        snapshot.delete(implementation.clone().to_canonical_type_edge().into_storage_key().into_owned_array());
        snapshot.delete(implementation.clone().to_reverse_type_edge().into_storage_key().into_owned_array());
    }

    // TODO: Store just the overridden.to vertex as value
    pub(crate) fn storage_set_type_edge_overridden<E>(
        snapshot: &mut Snapshot,
        edge: E,
        overridden: E
    )
    where E: EncodableParametrisedTypeEdge<'static>
    {
        let overridden_to = EdgeOverride::<E> { overridden : overridden.canonical_to() };
        Self::storage_put_type_edge_property(snapshot, edge, Some(overridden_to))
    }

    pub(crate) fn storage_delete_type_edge_overridden<E>(
        snapshot: &mut Snapshot,
        edge: E,
    )
        where E: EncodableParametrisedTypeEdge<'static>
    {
        Self::storage_delete_type_edge_property::<EdgeOverride<'_, E>>(snapshot, edge)
    }

    // Modifiers
    // TODO: Should this just accept owns: Owns<'_> ?
    pub(crate) fn storage_set_owns_ordering(
        snapshot: &mut Snapshot,
        owns: Owns<'_>,
        ordering: Ordering,
    ) {
        Self::storage_put_type_edge_property(snapshot, owns, Some(ordering))
    }

    pub(crate) fn storage_delete_owns_ordering(snapshot: &mut Snapshot, owns: Owns<'_>) {
        Self::storage_delete_type_edge_property::<Ordering>(snapshot, owns)
    }

    pub(crate) fn storage_put_type_edge_property<'a, P>(snapshot: &mut Snapshot, edge: impl EncodableParametrisedTypeEdge<'a>, property_opt: Option<P>)
    where
        P: EncodableTypeEdgeProperty<'a>,
    {
        let key = P::build_key(edge).into_storage_key();
        if let(Some(property)) = property_opt {
            let value =  property.build_value().unwrap();
            snapshot.put_val(key.into_owned_array(), value.into_array())
        } else {
            snapshot.put(key.into_owned_array())
        }
    }

    pub(crate) fn storage_delete_type_edge_property<'a, P>(snapshot: &mut Snapshot, edge: impl EncodableParametrisedTypeEdge<'a>)
        where P: EncodableTypeEdgeProperty<'a>,
    {
        snapshot.delete(P::build_key(edge).into_storage_key().into_owned_array());
    }
}

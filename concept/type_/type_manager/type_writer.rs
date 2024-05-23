/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use bytes::byte_array::ByteArray;
use encoding::graph::type_::index::LabelToTypeVertexIndex;
use encoding::graph::type_::property::{build_property_type_edge_ordering, build_property_type_edge_override, build_property_type_label, build_property_type_value_type};
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::edge::{EdgeRelatesEncoder, EdgeRelatesReverseEncoder, EdgeSubEncoder, EdgeSubReverseEncoder, TypeEdge, TypeEdgeEncoder};
use encoding::layout::prefix::Prefix;
use encoding::value::label::Label;
use encoding::value::value_type::{ValueType, ValueTypeBytes};
use storage::snapshot::WritableSnapshot;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::KindAPI;
use crate::type_::type_manager::type_reader::TypeReader;
use crate::type_::{InterfaceImplementation, IntoCanonicalTypeEdge, Ordering, serialise_ordering, TypeAPI};
use crate::type_::attribute_type::AttributeType;
use crate::type_::relates::Relates;
use crate::type_::type_manager::encoding_helper::EdgeEncoder;

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
        let sub = EdgeSubEncoder::build_edge(subtype.clone().into_vertex(), supertype.clone().into_vertex());
        snapshot.put(sub.into_storage_key().into_owned_array());
        let sub_reverse = EdgeSubReverseEncoder::build_edge(supertype.into_vertex(), subtype.into_vertex());
        snapshot.put(sub_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_supertype<T>(snapshot: &mut Snapshot, subtype: T)
    where T: KindAPI<'static>
    {
        let supertype = TypeReader::get_supertype(snapshot, subtype.clone()).unwrap().unwrap();
        let sub = EdgeSubEncoder::build_edge(subtype.clone().into_vertex(), supertype.clone().into_vertex());
        snapshot.delete(sub.into_storage_key().into_owned_array());
        let sub_reverse = EdgeSubReverseEncoder::build_edge(supertype.clone().into_vertex(), subtype.into_vertex());
        snapshot.delete(sub_reverse.into_storage_key().into_owned_array());
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
        let relates = EdgeRelatesEncoder::build_edge(relation.clone().into_vertex(), role.clone().into_vertex());
        snapshot.put(relates.into_storage_key().into_owned_array());
        let relates_reverse = EdgeRelatesReverseEncoder::build_edge(role.into_vertex(), relation.into_vertex());
        snapshot.put(relates_reverse.into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_relates(
        snapshot: &mut Snapshot,
        relation: RelationType<'static>,
        role: RoleType<'static>,
    ) {
        let relates = EdgeRelatesEncoder::build_edge(relation.clone().into_vertex(), role.clone().into_vertex());
        snapshot.delete(relates.into_storage_key().into_owned_array());
        let relates_reverse = EdgeRelatesReverseEncoder::build_edge(role.into_vertex(), relation.into_vertex());
        snapshot.delete(relates_reverse.into_storage_key().into_owned_array());
    }
    pub(crate) fn storage_put_interface_impl<IMPL>(
        snapshot: &mut Snapshot,
        implementation: IMPL,
    )
    where
        IMPL: InterfaceImplementation<'static>,
    {
        snapshot.put(IMPL::Encoder::forward_edge(implementation.clone()).into_storage_key().into_owned_array());
        snapshot.put(IMPL::Encoder::reverse_edge(implementation.clone()).into_storage_key().into_owned_array());
    }

    pub(crate) fn storage_delete_interface_impl<IMPL>(
        snapshot: &mut Snapshot,
        implementation: IMPL,
    )
        where IMPL: InterfaceImplementation<'static>
    {
        snapshot.delete(IMPL::Encoder::forward_edge(implementation.clone()).into_storage_key().into_owned_array());
        snapshot.delete(IMPL::Encoder::reverse_edge(implementation.clone()).into_storage_key().into_owned_array());
    }

    // TODO: Store just the overridden.to vertex as value
    pub(crate) fn storage_set_type_edge_overridden<E>(
        snapshot: &mut Snapshot,
        edge: E,
        overridden: E
        // canonical_overridden_to: impl TypeAPI<'static>,
    )
    where E: IntoCanonicalTypeEdge<'static>
    {
        let property_key =
            build_property_type_edge_override(edge.into_type_edge()).into_storage_key().into_owned_array();
        let overridden_to_vertex = ByteArray::copy(overridden.into_type_edge().into_bytes().bytes());
        snapshot.put_val(property_key, overridden_to_vertex);
    }

    // Modifiers
    // TODO: Should this just accept owns: Owns<'_> ?
    pub(crate) fn storage_set_owns_ordering(
        snapshot: &mut Snapshot,
        owns_edge: TypeEdge<'_>,
        ordering: Ordering,
    ) {
        debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
        snapshot.put_val(
            build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_ordering(ordering)),
        )
    }

    pub(crate) fn storage_delete_owns_ordering(snapshot: &mut Snapshot, owns_edge: TypeEdge<'_>) {
        debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
        snapshot.delete(build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array())
    }

}

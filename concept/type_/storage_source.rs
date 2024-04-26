/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use bytes::Bytes;
use encoding::graph::type_::edge::{build_edge_owns_prefix_from, build_edge_plays_prefix_from, build_edge_relates_prefix_from, build_edge_relates_reverse_prefix_from, build_edge_sub_prefix_from, new_edge_owns, new_edge_plays, new_edge_relates, new_edge_relates_reverse, new_edge_sub, TypeEdge};
use encoding::graph::type_::index::LabelToTypeVertexIndex;
use encoding::graph::type_::property::{build_property_type_edge_ordering, build_property_type_label, build_property_type_ordering, build_property_type_value_type, TypeEdgeProperty, TypeVertexProperty};
use encoding::graph::type_::vertex::TypeVertex;
use encoding::value::label::Label;
use encoding::value::string::StringBytes;
use resource::constants::encoding::LABEL_SCOPED_NAME_STRING_INLINE;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use std::collections::HashSet;
use bytes::byte_reference::ByteReference;
use encoding::{AsBytes, Keyable};
use encoding::layout::infix::Infix;
use encoding::value::value_type::{ValueType, ValueTypeID};
use storage::key_range::KeyRange;
use storage::snapshot::ReadableSnapshot;
use crate::error::ConceptReadError;
use crate::type_::type_manager::ReadableType;
use crate::type_::{deserialise_annotation_cardinality, deserialise_ordering, IntoCanonicalTypeEdge, Ordering, OwnerAPI, PlayerAPI, TypeAPI};
use crate::type_::annotation::{Annotation, AnnotationAbstract, AnnotationDistinct, AnnotationIndependent};
use crate::type_::attribute_type::AttributeType;
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;

pub struct StorageTypeManagerSource { }

// TODO: The '_s is only here for the enforcement of pass-by-value of types. If we drop that, we can move it to the function signatures
impl<'_s> StorageTypeManagerSource
    where '_s : 'static {

    // TODO: Return vertex for consistency with other methods
    pub(crate) fn storage_get_labelled_type<'b, U: ReadableType<'_s, 'b>>(snapshot: &impl ReadableSnapshot, label: &Label<'_>) -> Result<Option<U::SelfWithLifetime>, ConceptReadError>
        where U:
    {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => Ok(Some(U::read_from(Bytes::Array(value)))),
            Err(error) => Err(ConceptReadError::SnapshotGet { source: error })
        }
    }

    pub(crate) fn storage_get_supertype<'b, U: ReadableType<'_s, 'b>>(snapshot: &impl ReadableSnapshot, subtype: U) -> Result<Option<U::SelfWithLifetime>, ConceptReadError> {
        Ok(Self::storage_get_supertype_vertex(snapshot, subtype.into_vertex())?.map(|supertype_vertex| U::read_from(supertype_vertex.into_bytes())))
    }

    pub(crate) fn storage_get_supertype_vertex(snapshot: &impl ReadableSnapshot, subtype: TypeVertex<'_s>) -> Result<Option<TypeVertex<'static>>, ConceptReadError>
    {
        // TODO: handle possible errors
        Ok(snapshot
            .iterate_range(KeyRange::new_within(build_edge_sub_prefix_from(subtype), TypeEdge::FIXED_WIDTH_ENCODING))
            .first_cloned()
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
            .map(|(key, _)| new_edge_sub(key.into_byte_array_or_ref()).to().into_owned()))
    }

    pub(crate) fn storage_get_label(snapshot: &impl ReadableSnapshot, type_: impl TypeAPI<'_s>) -> Result<Option<Label<'static>>, ConceptReadError> {
        let key = build_property_type_label(type_.into_vertex());
        snapshot
            .get_mapped(key.into_storage_key().as_reference(), |reference| {
                let value = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
                Label::parse_from(value)
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn storage_get_owns(
        snapshot: &impl ReadableSnapshot,
        owner: impl OwnerAPI<'_s>
    ) -> Result<HashSet<Owns<'static>>, ConceptReadError>
    {
        let owns_prefix = build_edge_owns_prefix_from(owner.into_vertex());
        // TODO: handle possible errors
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let owns_edge = new_edge_owns(Bytes::Reference(key.byte_ref()));
                Owns::new(ObjectType::new(owns_edge.from().into_owned()), AttributeType::new(owns_edge.to().into_owned())) // TODO: Should we make this more type safe.
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn storage_get_plays(
        snapshot: &impl ReadableSnapshot,
        player: impl PlayerAPI<'_s>
    ) -> Result<HashSet<Plays<'static>>, ConceptReadError>
    {
        let plays_prefix = build_edge_plays_prefix_from(player.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(plays_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let plays_edge = new_edge_plays(Bytes::Reference(key.byte_ref()));
                Plays::new(ObjectType::new(plays_edge.from().into_owned()), RoleType::new(plays_edge.to().into_owned()))
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn storage_get_relates(
        snapshot: &impl ReadableSnapshot,
        relation: RelationType<'_s>,
    ) -> Result<HashSet<Relates<'static>>, ConceptReadError>
    {
        let relates_prefix = build_edge_relates_prefix_from(relation.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(relates_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let relates_edge = new_edge_relates(Bytes::Reference(key.byte_ref()));
                Relates::new(RelationType::new(relates_edge.from().into_owned()), RoleType::new(relates_edge.to().into_owned()))
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn storage_get_relations(
        snapshot: &impl ReadableSnapshot,
        role: RoleType<'_s>,
    ) -> Result<Relates<'static>, ConceptReadError>
    {
        let relates_prefix = build_edge_relates_reverse_prefix_from(role.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(relates_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|key, _| {
                let relates_edge = new_edge_relates_reverse(Bytes::Reference(key.byte_ref()));
                Relates::new(RelationType::new(relates_edge.to().into_owned()), RoleType::new(relates_edge.from().into_owned()))
            }).map_err(|error| ConceptReadError::SnapshotIterate { source: error })
            .map(|v| { v.first().unwrap().clone() })
    }

    pub(crate) fn storage_get_value_type(snapshot: &impl ReadableSnapshot, type_: AttributeType<'_s>) -> Result<Option<ValueType>, ConceptReadError> {
        snapshot
            .get_mapped(
                build_property_type_value_type(type_.into_vertex()).into_storage_key().as_reference(),
                |bytes| {
                    ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap()))
                },
            )
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn storage_get_type_annotations(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'_s>,
    ) -> Result<HashSet<Annotation>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_inclusive(
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeVertexProperty::new(Bytes::Reference(key.byte_ref()));
                match annotation_key.infix() {
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract::new()),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct::new()),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent::new()),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::_PropertyAnnotationLast
                    | Infix::PropertyLabel
                    | Infix::PropertyValueType
                    | Infix::PropertyOrdering
                    | Infix::PropertyHasOrder
                    | Infix::PropertyRolePlayerOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                }
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    // TODO: this is currently breaking our architectural pattern that none of the Manager methods should operate graphs
    pub(crate) fn storage_get_type_edge_annotations<'a>(
        snapshot: &impl ReadableSnapshot,
        into_type_edge: impl IntoCanonicalTypeEdge<'a>,
    ) -> Result<HashSet<Annotation>, ConceptReadError> {
        let type_edge = into_type_edge.into_type_edge();
        snapshot
            .iterate_range(KeyRange::new_inclusive(
                TypeEdgeProperty::build(type_edge.clone(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeEdgeProperty::build(type_edge, Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeEdgeProperty::new(Bytes::Reference(key.byte_ref()));
                match annotation_key.infix() {
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract::new()),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct::new()),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent::new()),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::_PropertyAnnotationLast
                    | Infix::PropertyLabel
                    | Infix::PropertyValueType
                    | Infix::PropertyOrdering
                    | Infix::PropertyHasOrder
                    | Infix::PropertyRolePlayerOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                }
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    pub(crate) fn storage_get_type_ordering<'a>(snapshot: &impl ReadableSnapshot, role_type: RoleType<'_s>) -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(
                build_property_type_ordering(role_type.vertex()).into_storage_key().as_reference(),
                |bytes| deserialise_ordering(bytes),
            )
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }
    pub(crate) fn storage_get_type_edge_ordering<'a>(snapshot: &impl ReadableSnapshot, owns: Owns<'_s>)  -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(
                build_property_type_edge_ordering(owns.into_type_edge()).into_storage_key().as_reference(),
                |bytes| deserialise_ordering(bytes),
            )
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }

}

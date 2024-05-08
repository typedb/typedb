/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use encoding::{
    graph::type_::{
        edge::{
            build_edge_owns_prefix_from, build_edge_plays_prefix_from, build_edge_relates_prefix_from,
            build_edge_relates_reverse_prefix_from, build_edge_sub_prefix_from, build_edge_sub_reverse_prefix_from,
            new_edge_owns, new_edge_plays, new_edge_relates, new_edge_relates_reverse, new_edge_sub,
            new_edge_sub_reverse, TypeEdge,
        },
        index::LabelToTypeVertexIndex,
        property::{
            build_property_type_edge_ordering, build_property_type_edge_override, build_property_type_label,
            build_property_type_ordering, build_property_type_value_type,
            TypeEdgeProperty, TypeVertexProperty,
        },
        vertex::TypeVertex,
    },
    layout::infix::Infix,
    value::{
        label::Label,
        string_bytes::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
    AsBytes, Keyable,
};
use iterator::Collector;
use resource::constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_KEY_INLINE};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationDistinct, AnnotationIndependent},
        attribute_type::AttributeType,
        deserialise_annotation_cardinality, deserialise_ordering,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::ReadableType,
        IntoCanonicalTypeEdge, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use crate::type_::type_manager::KindAPI;

pub struct TypeReader {}

impl TypeReader {
    pub(crate) fn get_labelled_type<T>(
        snapshot: &impl ReadableSnapshot,
        label: &Label<'_>,
    ) -> Result<Option<T::ReadOutput<'static>>, ConceptReadError>
    where
        T: ReadableType,
    {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => Ok(Some(T::read_from(Bytes::Array(value)))),
            Err(error) => Err(ConceptReadError::SnapshotGet { source: error }),
        }
    }

    // Used in type_manager to set supertype
    pub(crate) fn get_supertype_vertex(
        snapshot: &impl ReadableSnapshot,
        subtype: TypeVertex<'static>,
    ) -> Result<Option<TypeVertex<'static>>, ConceptReadError> {
        Ok(snapshot
            .iterate_range(KeyRange::new_within(build_edge_sub_prefix_from(subtype), TypeEdge::FIXED_WIDTH_ENCODING))
            .first_cloned()
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
            .map(|(key, _)| new_edge_sub(key.into_byte_array_or_ref()).to().into_owned()))
    }

    pub(crate) fn get_supertype<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Option<T::ReadOutput<'static>>, ConceptReadError>
    where
        T: ReadableType + TypeAPI<'static>,
    {
        Ok(Self::get_supertype_vertex(snapshot, subtype.into_vertex())?
            .map(|supertype_vertex| T::read_from(supertype_vertex.into_bytes())))
    }

    pub fn get_supertypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T::ReadOutput<'static>>, ConceptReadError>
    where
        T: ReadableType + TypeAPI<'static>,
    {
        // WARN: supertypes currently do NOT include themselves
        // ^ To fix, Just start with `let mut supertype = Some(type_)`
        let mut supertypes = Vec::new();
        let mut supervertex_opt = TypeReader::get_supertype_vertex(snapshot, subtype.clone().into_vertex())?;
        while let Some(supervertex) = supervertex_opt {
            supertypes.push(T::read_from(supervertex.clone().into_bytes()));
            supervertex_opt = TypeReader::get_supertype_vertex(snapshot, supervertex.clone())?;
        }
        Ok(supertypes)
    }

    fn get_subtypes_vertex(
        snapshot: &impl ReadableSnapshot,
        supertype: TypeVertex<'static>,
    ) -> Result<Vec<TypeVertex<'static>>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(
                build_edge_sub_reverse_prefix_from(supertype),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, _| new_edge_sub_reverse(Bytes::Reference(key.byte_ref())).to().into_owned())
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_subtypes<T>(
        snapshot: &impl ReadableSnapshot,
        supertype: T,
    ) -> Result<Vec<T::ReadOutput<'static>>, ConceptReadError>
    where
        T: ReadableType + TypeAPI<'static>,
    {
        Ok(Self::get_subtypes_vertex(snapshot, supertype.into_vertex())?
            .into_iter()
            .map(|subtype_vertex| T::read_from(subtype_vertex.into_bytes()))
            .collect::<Vec<T::ReadOutput<'static>>>())
    }

    pub fn get_subtypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T::ReadOutput<'static>>, ConceptReadError>
    where
        T: ReadableType + TypeAPI<'static>,
    {
        // WARN: subtypes currently do NOT include themselves
        // ^ To fix, Just start with `let mut stack = vec!(subtype.clone());`
        let mut subtypes = Vec::new();
        let mut stack = TypeReader::get_subtypes_vertex(snapshot, subtype.clone().into_vertex())?;
        while !stack.is_empty() {
            let subvertex = stack.pop().unwrap();
            subtypes.push(T::read_from(subvertex.clone().into_bytes()));
            stack.append(&mut TypeReader::get_subtypes_vertex(snapshot, subvertex.clone())?);
            // TODO: Should we pass an accumulator instead?
        }
        Ok(subtypes)
    }

    pub(crate) fn get_label(
        snapshot: &impl ReadableSnapshot,
        type_: impl TypeAPI<'static>,
    ) -> Result<Option<Label<'static>>, ConceptReadError> {
        let key = build_property_type_label(type_.into_vertex());
        snapshot
            .get_mapped(key.into_storage_key().as_reference(), |reference| {
                let value = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
                Label::parse_from(value)
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_owns(
        snapshot: &impl ReadableSnapshot,
        owner: impl OwnerAPI<'static>,
    ) -> Result<HashSet<Owns<'static>>, ConceptReadError> {
        let owns_prefix = build_edge_owns_prefix_from(owner.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let owns_edge = new_edge_owns(Bytes::Reference(key.byte_ref()));
                Owns::new(
                    ObjectType::new(owns_edge.from().into_owned()),
                    AttributeType::new(owns_edge.to().into_owned()),
                ) // TODO: Should we make this more type safe.
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_owns_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        owner: T,
    ) -> Result<HashMap<AttributeType<'static>, Owns<'static>>, ConceptReadError>
    where
        T: OwnerAPI<'static> + ReadableType<ReadOutput<'static> = T>, // ReadOutput=T is needed for supertype transitivity
    {
        // TODO: Should the owner of a transitive owns be the declaring owner or the inheriting owner?
        let mut transitive_owns: HashMap<AttributeType<'static>, Owns<'static>> = HashMap::new();
        let mut overridden_owns: HashSet<AttributeType<'static>> = HashSet::new(); // TODO: Should this store the owns? This feels more fool-proof if it's correct.
        let mut current_type = Some(owner);
        while current_type.is_some() {
            let declared_owns = Self::get_owns(snapshot, current_type.as_ref().unwrap().clone())?;
            for owns in declared_owns.into_iter() {
                let attribute = owns.attribute();
                if !overridden_owns.contains(&attribute) {
                    debug_assert!(!transitive_owns.contains_key(&attribute));
                    transitive_owns.insert(owns.attribute(), owns.clone());
                    if let Some(overridden) = Self::get_owns_override(snapshot, owns.clone())? {
                        overridden_owns.add(overridden.attribute());
                    }
                }
            }
            current_type = Self::get_supertype(snapshot, current_type.unwrap())?;
        }
        Ok(transitive_owns)
    }

    pub(crate) fn get_plays(
        snapshot: &impl ReadableSnapshot,
        player: impl PlayerAPI<'static>,
    ) -> Result<HashSet<Plays<'static>>, ConceptReadError> {
        let plays_prefix = build_edge_plays_prefix_from(player.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(plays_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let plays_edge = new_edge_plays(Bytes::Reference(key.byte_ref()));
                Plays::new(ObjectType::new(plays_edge.from().into_owned()), RoleType::new(plays_edge.to().into_owned()))
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_plays_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        player: T,
    ) -> Result<HashMap<RoleType<'static>, Plays<'static>>, ConceptReadError>
    where
        T: PlayerAPI<'static> + ReadableType<ReadOutput<'static> = T>, // ReadOutput=T is needed for supertype transitivity
    {
        // TODO: Should the player of a transitive plays be the declaring player or the inheriting player?
        let mut transitive_plays: HashMap<RoleType<'static>, Plays<'static>> = HashMap::new();
        let mut overridden_plays: HashSet<RoleType<'static>> = HashSet::new(); // TODO: Should this store the plays? This feels more fool-proof if it's correct.
        let mut current_type = Some(player);
        while current_type.is_some() {
            let declared_plays = Self::get_plays(snapshot, current_type.as_ref().unwrap().clone())?;
            for plays in declared_plays.into_iter() {
                let role = plays.role();
                if !overridden_plays.contains(&role) {
                    debug_assert!(!transitive_plays.contains_key(&role));
                    transitive_plays.insert(plays.role(), plays.clone());
                    if let Some(overridden) = Self::get_plays_override(snapshot, plays.clone())? {
                        overridden_plays.add(overridden.role());
                    }
                }
            }
            current_type = Self::get_supertype(snapshot, current_type.unwrap())?;
        }
        Ok(transitive_plays)
    }

    pub(crate) fn get_plays_override(
        snapshot: &impl ReadableSnapshot,
        plays: Plays<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let override_property_key = build_property_type_edge_override(plays.into_type_edge());
        snapshot
            .get_mapped(override_property_key.into_storage_key().as_reference(), |overridden_edge_bytes| {
                let overridden_edge = new_edge_plays(Bytes::Reference(overridden_edge_bytes));
                Plays::new(
                    ObjectType::new(overridden_edge.from().into_owned()),
                    RoleType::new(overridden_edge.to().into_owned()),
                )
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_relates(
        snapshot: &impl ReadableSnapshot,
        relation: RelationType<'static>,
    ) -> Result<HashSet<Relates<'static>>, ConceptReadError> {
        let relates_prefix = build_edge_relates_prefix_from(relation.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(relates_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                let relates_edge = new_edge_relates(Bytes::Reference(key.byte_ref()));
                Relates::new(
                    RelationType::new(relates_edge.from().into_owned()),
                    RoleType::new(relates_edge.to().into_owned()),
                )
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_relates_transitive(
        snapshot: &impl ReadableSnapshot,
        relation: RelationType<'static>,
    ) -> Result<HashMap<String, Relates<'static>>, ConceptReadError> {
        // TODO: Should the relation of a transitive relates be the declaring relation or the inheriting relation?
        let mut transitive_relates: HashMap<String, Relates<'static>> = HashMap::new();
        let mut overridden_relates: HashSet<RoleType<'static>> = HashSet::new(); // TODO: Should this store the relates? This feels more fool-proof if it's correct.
        let mut current_relation = Some(relation);
        while current_relation.is_some() {
            let declared_relates = Self::get_relates(snapshot, current_relation.as_ref().unwrap().clone())?;
            for relates in declared_relates.into_iter() {
                let role = relates.role();
                if !overridden_relates.contains(&role) {
                    let role_name = Self::get_label(snapshot, relates.role())?.unwrap().name.as_str().to_owned();
                    debug_assert!(!transitive_relates.contains_key(&role_name));
                    transitive_relates.insert(role_name, relates.clone());
                    if let Some(overridden) = Self::get_supertype(snapshot, relates.role().clone())? {
                        overridden_relates.add(overridden);
                    }
                }
            }
            current_relation = Self::get_supertype(snapshot, current_relation.unwrap())?;
        }
        Ok(transitive_relates)
    }

    pub(crate) fn get_relations(
        snapshot: &impl ReadableSnapshot,
        role: RoleType<'static>,
    ) -> Result<Relates<'static>, ConceptReadError> {
        let relates_prefix = build_edge_relates_reverse_prefix_from(role.into_vertex());
        snapshot
            .iterate_range(KeyRange::new_within(relates_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_vec(|key, _| {
                let relates_edge_reverse = new_edge_relates_reverse(Bytes::Reference(key.byte_ref()));
                Relates::new(
                    RelationType::new(relates_edge_reverse.to().into_owned()),
                    RoleType::new(relates_edge_reverse.from().into_owned()),
                )
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
            .map(|v| v.first().unwrap().clone())
    }

    pub(crate) fn get_value_type<'a>(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'a>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        snapshot
            .get_mapped(
                build_property_type_value_type(type_.into_vertex()).into_storage_key().as_reference(),
                |bytes| ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap())),
            )
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_type_annotations<'a, T: KindAPI<'a>>(
        snapshot: &impl ReadableSnapshot,
        type_: T,
    ) -> Result<HashSet<T::AnnotationType>, ConceptReadError> {
        snapshot
            .iterate_range(KeyRange::new_inclusive(
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MIN).into_storage_key(),
                TypeVertexProperty::build(type_.vertex(), Infix::ANNOTATION_MAX).into_storage_key(),
            ))
            .collect_cloned_hashset(|key, value| {
                let annotation_key = TypeVertexProperty::new(Bytes::Reference(key.byte_ref()));
                let annotation = match annotation_key.infix() {
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
                    | Infix::PropertyOverride
                    | Infix::PropertyHasOrder
                    | Infix::PropertyRolePlayerOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                };
                T::AnnotationType::from(annotation)
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    // TODO: Merge with plays_override when we get there.
    pub(crate) fn get_owns_override(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        let override_property_key = build_property_type_edge_override(owns.into_type_edge());
        snapshot
            .get_mapped(override_property_key.into_storage_key().as_reference(), |overridden_edge_bytes| {
                let overridden_edge = new_edge_owns(Bytes::Reference(overridden_edge_bytes));
                Owns::new(
                    ObjectType::new(overridden_edge.from().into_owned()),
                    AttributeType::new(overridden_edge.to().into_owned()),
                )
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    // TODO: this is currently breaking our architectural pattern that none of the Manager methods should operate graphs
    pub(crate) fn get_type_edge_annotations<'a>(
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
                    | Infix::PropertyOverride
                    | Infix::PropertyHasOrder
                    | Infix::PropertyRolePlayerOrder => {
                        unreachable!("Retrieved unexpected infixes while reading annotations.")
                    }
                }
            })
            .map_err(|err| ConceptReadError::SnapshotIterate { source: err.clone() })
    }

    pub(crate) fn get_type_ordering<'a>(
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'a>,
    ) -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(build_property_type_ordering(role_type.vertex()).into_storage_key().as_reference(), |bytes| {
                deserialise_ordering(bytes)
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }

    pub(crate) fn get_type_edge_ordering<'a>(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'a>,
    ) -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(
                build_property_type_edge_ordering(owns.into_type_edge()).into_storage_key().as_reference(),
                |bytes| deserialise_ordering(bytes),
            )
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }
}

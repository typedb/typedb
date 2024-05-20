/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use bytes::Bytes;
use encoding::{
    graph::type_::{
        edge::{
            build_edge_relates_prefix_from,
            build_edge_relates_reverse_prefix_from, build_edge_sub_prefix_from, build_edge_sub_reverse_prefix_from,
            new_edge_relates, new_edge_relates_reverse, new_edge_sub,
            new_edge_sub_reverse, TypeEdge,
        },
        index::LabelToTypeVertexIndex,
        property::{
            build_property_type_edge_ordering, build_property_type_edge_override, build_property_type_label,
            build_property_type_ordering, build_property_type_value_type, TypeEdgeProperty, TypeVertexProperty,
        },
    },
    layout::infix::Infix,
    value::{
        label::Label,
        string_bytes::StringBytes,
        value_type::{ValueType, ValueTypeBytes},
    },
    AsBytes, Keyable,
};

use encoding::graph::type_::Kind;
use iterator::Collector;
use resource::constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_KEY_INLINE};
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationDistinct, AnnotationIndependent, AnnotationKey, AnnotationUnique,
        },
        attribute_type::AttributeType,
        deserialise_annotation_cardinality, deserialise_annotation_regex, deserialise_ordering,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::KindAPI,
        IntoCanonicalTypeEdge, Ordering, OwnerAPI, PlayerAPI, TypeAPI,
    },
};
use crate::type_::type_manager::encoding_helper::EdgeEncoder;
use crate::type_::InterfaceEdge;

pub struct TypeReader {}

impl TypeReader {
    pub(crate) fn check_type_is_root(type_label: &Label<'_>, kind: Kind) -> bool {
        type_label == &kind.root_label()
    }

    pub(crate) fn get_labelled_type<T>(
        snapshot: &impl ReadableSnapshot,
        label: &Label<'_>,
    ) -> Result<Option<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        let key = LabelToTypeVertexIndex::build(label).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => Ok(Some(T::read_from(Bytes::Array(value)))),
            Err(error) => Err(ConceptReadError::SnapshotGet { source: error }),
        }
    }

    // TODO: Should get_{super/sub}type[s_transitive] return T or T::SelfStatic.
    // T::SelfStatic is the more consistent, more honest interface, but T is convenient.
    pub(crate) fn get_supertype<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Option<T>, ConceptReadError>
    where
        T: TypeAPI<'static>,
    {
        Ok(snapshot
            .iterate_range(KeyRange::new_within(build_edge_sub_prefix_from(subtype.into_vertex()), TypeEdge::FIXED_WIDTH_ENCODING))
            .first_cloned()
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
            .map(|(key, _)| T::SelfStatic::new(new_edge_sub(Bytes::Array(key.into_byte_array())).to().into_owned())))
    }

    pub fn get_supertypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T>, ConceptReadError>
    where
        T: KindAPI<'static>,
    {
        // supertypes do NOT include themselves by design
        let mut supertypes: Vec<T> = Vec::new();
        let mut supertype_opt = TypeReader::get_supertype(snapshot, subtype.clone())?;
        while let Some(supertype) = supertype_opt {
            supertypes.push(supertype.clone());
            supertype_opt = TypeReader::get_supertype(snapshot, supertype.clone())?;
        }
        Ok(supertypes)
    }

    pub(crate) fn get_subtypes<T>(
        snapshot: &impl ReadableSnapshot,
        supertype: T,
    ) -> Result<Vec<T>, ConceptReadError>
    where
        T: KindAPI<'static>,
    {
        Ok(snapshot
            .iterate_range(KeyRange::new_within(
                build_edge_sub_reverse_prefix_from(supertype.into_vertex()),
                TypeEdge::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, _| T::new(new_edge_sub_reverse(Bytes::Reference(key.byte_ref())).to().into_owned()))
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
        )
    }

    pub fn get_subtypes_transitive<T>(
        snapshot: &impl ReadableSnapshot,
        subtype: T,
    ) -> Result<Vec<T>, ConceptReadError>
    where
        T: KindAPI<'static>,
    {
        //subtypes DO NOT include themselves by design
        let mut subtypes : Vec<T> = Vec::new();
        let mut stack = TypeReader::get_subtypes(snapshot, subtype.clone())?;
        while let Some(subtype) = stack.pop() {
            subtypes.push(subtype.clone());
            stack.append(&mut TypeReader::get_subtypes(snapshot, subtype)?);
            // TODO: Should we pass an accumulator instead?
        }
        Ok(subtypes)
    }

    pub(crate) fn get_label(
        snapshot: &impl ReadableSnapshot,
        type_: impl KindAPI<'static>,
    ) -> Result<Option<Label<'static>>, ConceptReadError> {
        let key = build_property_type_label(type_.into_vertex());
        snapshot
            .get_mapped(key.into_storage_key().as_reference(), |reference| {
                let value = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
                Label::parse_from(value)
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_implemented_interfaces<IMPL>(
        snapshot: &impl ReadableSnapshot,
        owner: impl TypeAPI<'static>,
    ) -> Result<HashSet<IMPL>, ConceptReadError>
    where
    IMPL : InterfaceEdge<'static> + Hash + Eq
    {
        let owns_prefix = IMPL::Encoder::forward_seek_prefix(IMPL::ObjectType::new(owner.into_vertex()));
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                IMPL::Encoder::read_from(Bytes::Reference(key.byte_ref()))
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }


    pub(crate) fn get_implemented_interfaces_transitive<IMPL, T>(
        snapshot: &impl ReadableSnapshot,
        object_type: T,
    ) -> Result<HashMap<IMPL::InterfaceType, IMPL>, ConceptReadError>
        where
            T: TypeAPI<'static>,
            IMPL : InterfaceEdge<'static> + Hash + Eq {
        // TODO: Should the owner of a transitive owns be the declaring owner or the inheriting owner?
        let mut transitive_implementations: HashMap<IMPL::InterfaceType, IMPL> = HashMap::new();
        let mut overridden_interfaces: HashSet<IMPL::InterfaceType> = HashSet::new(); // TODO: Should this store the owns? This feels more fool-proof if it's correct.
        let mut current_type = Some(object_type);
        while current_type.is_some() {
            let declared_implementations = Self::get_implemented_interfaces::<IMPL>(snapshot, current_type.as_ref().unwrap().clone())?;
            for implementation in declared_implementations.into_iter() {
                let interface = implementation.interface();
                if !overridden_interfaces.contains(&interface) {
                    debug_assert!(!transitive_implementations.contains_key(&interface));
                    transitive_implementations.insert(implementation.interface(), implementation.clone());
                }
                // Has to be outside so we ignore transitively overridden ones too
                if let Some(overridden) = Self::get_implementation_override(snapshot, implementation.clone())? {
                    overridden_interfaces.add(overridden.interface());
                }
            }
            current_type = Self::get_supertype(snapshot, current_type.unwrap())?;
        }
        Ok(transitive_implementations)
    }


    pub(crate) fn get_implementation_override<IMPL>(
        snapshot: &impl ReadableSnapshot,
        implementation: IMPL,
    ) -> Result<Option<IMPL>, ConceptReadError>
    where
        IMPL : InterfaceEdge<'static> + Hash + Eq
    {
        let override_property_key = build_property_type_edge_override(implementation.into_type_edge());
        snapshot
            .get_mapped(override_property_key.into_storage_key().as_reference(), |overridden_edge_bytes| {
                IMPL::Encoder::read_from(Bytes::Reference(overridden_edge_bytes))
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn get_implementations_for_interface<IMPL>(
        snapshot: &impl ReadableSnapshot,
        interface_type: IMPL::InterfaceType,
    ) -> Result<HashSet<IMPL>, ConceptReadError>
    where IMPL : InterfaceEdge<'static> + Hash + Eq
    {
        let owns_prefix = IMPL::Encoder::reverse_seek_prefix(interface_type);
        snapshot
            .iterate_range(KeyRange::new_within(owns_prefix, TypeEdge::FIXED_WIDTH_ENCODING))
            .collect_cloned_hashset(|key, _| {
                IMPL::Encoder::read_from(Bytes::Reference(key.byte_ref()))
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn get_implementations_for_interface_transitive<IMPL>(
        snapshot: &impl ReadableSnapshot,
        interface_type: IMPL::InterfaceType,
    ) -> Result<HashMap<IMPL, Vec<ObjectType<'static>>>, ConceptReadError>
    where IMPL: InterfaceEdge<'static, ObjectType=ObjectType<'static>> + Hash + Eq
    {
        let mut impl_transitive : HashMap<IMPL, Vec<ObjectType<'static>>> = HashMap::new();
        let declared_impl_set: HashSet<IMPL> = Self::get_implementations_for_interface(snapshot, interface_type.clone())?;

        for declared_impl in declared_impl_set {
            let mut stack = Vec::new();
            stack.push(declared_impl.object());
            let mut object_types: Vec<ObjectType<'static>> = Vec::new();
            while let(Some(sub_object)) = stack.pop() {
                let mut declared_impl_was_overridden = false;
                for sub_owner_owns in Self::get_implemented_interfaces::<IMPL>(snapshot, sub_object.clone())? {
                    if let Some(overridden_impl) = Self::get_implementation_override(snapshot, sub_owner_owns.clone())? {
                        declared_impl_was_overridden = declared_impl_was_overridden || overridden_impl.interface() == interface_type;
                    }
                }
                if !declared_impl_was_overridden {
                    object_types.add(sub_object.clone());
                    match sub_object.clone() {
                        ObjectType::Entity(owner) => {
                            Self::get_subtypes(snapshot, owner)?.into_iter().for_each(|t| stack.push(ObjectType::new(t.into_vertex())))
                        }
                        ObjectType::Relation(owner) => {
                            Self::get_subtypes(snapshot, owner)?.into_iter().for_each(|t| stack.push(ObjectType::new(t.into_vertex())))
                        }
                    };
                }
            }
            impl_transitive.insert(declared_impl, object_types);
        }
        Ok(impl_transitive)
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
    ) -> Result<HashMap<RoleType<'static>, Relates<'static>>, ConceptReadError> {
        // TODO: Should the relation of a transitive relates be the declaring relation or the inheriting relation?
        let mut transitive_relates: HashMap<RoleType<'static>, Relates<'static>> = HashMap::new();
        let mut overridden_relates: HashSet<RoleType<'static>> = HashSet::new(); // TODO: Should this store the relates? This feels more fool-proof if it's correct.
        let mut current_relation = Some(relation);
        while current_relation.is_some() {
            let declared_relates = Self::get_relates(snapshot, current_relation.as_ref().unwrap().clone())?;
            for relates in declared_relates.into_iter() {
                let role = relates.role();
                if !overridden_relates.contains(&role) {
                    debug_assert!(!transitive_relates.contains_key(&role));
                    transitive_relates.insert(role, relates.clone());
                }
                if let Some(overridden) = Self::get_supertype(snapshot, relates.role().clone())? {
                    overridden_relates.add(overridden);
                }
            }
            current_relation = Self::get_supertype(snapshot, current_relation.unwrap())?;
        }
        Ok(transitive_relates)
    }

    pub(crate) fn get_relation(
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

    pub(crate) fn get_value_type(
        snapshot: &impl ReadableSnapshot,
        type_: AttributeType<'_>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        snapshot
            .get_mapped(
                build_property_type_value_type(type_.into_vertex()).into_storage_key().as_reference(),
                |bytes| ValueTypeBytes::new(bytes.bytes().try_into().unwrap()).to_value_type(),
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
                    Infix::PropertyAnnotationAbstract => Annotation::Abstract(AnnotationAbstract),
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::PropertyAnnotationRegex => Annotation::Regex(deserialise_annotation_regex(value)),
                    | Infix::_PropertyAnnotationLast
                    | Infix::PropertyAnnotationUnique
                    | Infix::PropertyAnnotationKey
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
                    Infix::PropertyAnnotationDistinct => Annotation::Distinct(AnnotationDistinct),
                    Infix::PropertyAnnotationIndependent => Annotation::Independent(AnnotationIndependent),
                    Infix::PropertyAnnotationUnique => Annotation::Unique(AnnotationUnique),
                    Infix::PropertyAnnotationKey => Annotation::Key(AnnotationKey),
                    Infix::PropertyAnnotationCardinality => {
                        Annotation::Cardinality(deserialise_annotation_cardinality(value))
                    }
                    Infix::PropertyAnnotationRegex => Annotation::Regex(deserialise_annotation_regex(value)),
                    | Infix::_PropertyAnnotationLast
                    | Infix::PropertyAnnotationAbstract
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

    pub(crate) fn get_type_ordering(
        snapshot: &impl ReadableSnapshot,
        role_type: RoleType<'_>,
    ) -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(build_property_type_ordering(role_type.vertex()).into_storage_key().as_reference(), |bytes| {
                deserialise_ordering(bytes)
            })
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }

    pub(crate) fn get_type_edge_ordering(
        snapshot: &impl ReadableSnapshot,
        owns: Owns<'_>,
    ) -> Result<Ordering, ConceptReadError> {
        let ordering = snapshot
            .get_mapped(
                build_property_type_edge_ordering(owns.into_type_edge()).into_storage_key().as_reference(),
                deserialise_ordering,
            )
            .map_err(|err| ConceptReadError::SnapshotGet { source: err })?;
        Ok(ordering.unwrap())
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, hash::Hash};

use encoding::{
    graph::{
        thing::{edge::ThingEdgeRolePlayer, vertex_object::ObjectVertex},
        type_::edge::TypeEdgeEncoding,
        Typed,
    },
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::{key_range::KeyRange, snapshot::ReadableSnapshot};

use crate::{
    thing::{
        object::ObjectAPI,
        relation::{RelationIterator, RolePlayerIterator},
        thing_manager::ThingManager,
    },
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCategory, AnnotationKey, AnnotationRange,
            AnnotationRegex, AnnotationValues,
        },
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::EntityType,
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{
            type_reader::TypeReader,
            validation::{get_label, SchemaValidationError},
        },
        InterfaceImplementation, KindAPI, ObjectTypeAPI, Ordering, TypeAPI,
    },
};
use crate::error::ConceptReadError;

pub(crate) fn validate_role_name_uniqueness_non_transitive<'a>(
    snapshot: &impl ReadableSnapshot,
    relation_type: RelationType<'static>,
    new_label: &Label<'static>,
) -> Result<(), SchemaValidationError> {
    let scoped_label = Label::build_scoped(
        new_label.name.as_str(),
        TypeReader::get_label(snapshot, relation_type).unwrap().unwrap().name().as_str(),
    );

    if TypeReader::get_labelled_type::<RoleType<'static>>(snapshot, &scoped_label)
        .map_err(SchemaValidationError::ConceptRead)?
        .is_some()
    {
        Err(SchemaValidationError::RoleNameShouldBeUniqueForRelationTypeHierarchy(new_label.clone(), scoped_label))
    } else {
        Ok(())
    }
}

pub(crate) fn type_is_abstract(
    snapshot: &impl ReadableSnapshot,
    type_: impl KindAPI<'static>,
) -> Result<bool, SchemaValidationError> {
    type_has_declared_annotation(snapshot, type_.clone(), Annotation::Abstract(AnnotationAbstract))
}

pub(crate) fn is_overridden_interface_object_supertype_or_self<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    overridden: T,
) -> Result<bool, ConceptReadError> {
    if type_ == overridden {
        return Ok(true);
    }

    Ok(TypeReader::get_supertypes(snapshot, type_.clone())?.contains(&overridden.clone()))
}

pub(crate) fn is_attribute_type_owns_overridden<T>(
    snapshot: &impl ReadableSnapshot,
    object_type: T,
    attribute_type: AttributeType<'static>,
) -> Result<bool, ConceptReadError>
    where
        T: ObjectTypeAPI<'static>,
{
    let all_overridden = TypeReader::get_overridden_interfaces::<Owns<'static>, T>(snapshot, object_type.clone())?;
    Ok(all_overridden.contains_key(&attribute_type))
}

pub(crate) fn is_role_type_plays_overridden<T>(
    snapshot: &impl ReadableSnapshot,
    player: T,
    role_type: RoleType<'static>,
) -> Result<bool, ConceptReadError>
    where
        T: ObjectTypeAPI<'static>,
{
    let all_overridden = TypeReader::get_overridden_interfaces::<Plays<'static>, T>(snapshot, player.clone())?;
    Ok(all_overridden.contains_key(&role_type))
}

// TODO: Try to wrap all these type_has_***annotation and edge_has_***annotation into several macros!

// TODO: Refactor to type_get_declared_annotation
pub(crate) fn type_has_declared_annotation<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    annotation: Annotation,
) -> Result<bool, SchemaValidationError> {
    let has = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .contains(&T::AnnotationType::from(annotation));
    Ok(has)
}

pub(crate) fn type_has_declared_annotation_category<T>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    annotation_category: AnnotationCategory,
) -> Result<bool, SchemaValidationError>
where
    T: KindAPI<'static>,
{
    let has = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .iter()
        .map(|annotation| annotation.clone().into().category())
        .any(|found_category| found_category == annotation_category);
    Ok(has)
}

pub(crate) fn type_has_annotation_category<T>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    annotation_category: AnnotationCategory,
) -> Result<bool, SchemaValidationError>
where
    T: KindAPI<'static>,
{
    let has = TypeReader::get_type_annotations(snapshot, type_.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .iter()
        .map(|(annotation, _)| annotation.clone().into().category())
        .any(|found_category| found_category == annotation_category);
    Ok(has)
}

pub(crate) fn type_get_annotation_by_category(
    snapshot: &impl ReadableSnapshot,
    type_: impl KindAPI<'static>,
    annotation_category: AnnotationCategory,
) -> Result<Option<Annotation>, SchemaValidationError> {
    let annotation = TypeReader::get_type_annotations(snapshot, type_.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .into_iter()
        .map(|(found_annotation, _)| found_annotation)
        .find(|found_annotation| found_annotation.clone().into().category() == annotation_category);
    Ok(annotation.map(|val| val.clone().into()))
}

pub(crate) fn edge_get_annotation_by_category<EDGE>(
    snapshot: &impl ReadableSnapshot,
    edge: EDGE,
    annotation_category: AnnotationCategory,
) -> Result<Option<Annotation>, SchemaValidationError>
where
    EDGE: InterfaceImplementation<'static> + Clone,
{
    let annotation = TypeReader::get_type_edge_annotations(snapshot, edge.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .into_iter()
        .map(|(found_annotation, _)| found_annotation)
        .find(|found_annotation| found_annotation.category() == annotation_category);
    Ok(annotation.map(|val| val.clone()))
}

pub(crate) fn is_ordering_compatible_with_distinct_annotation(ordering: Ordering, distinct_set: bool) -> bool {
    if distinct_set {
        ordering == Ordering::Ordered
    } else {
        true
    }
}

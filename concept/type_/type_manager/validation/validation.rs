/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::hash::Hash;

use encoding::{graph::type_::edge::TypeEdgeEncoding, value::label::Label};
use itertools::Itertools;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::object::ObjectAPI,
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationCategory},
        attribute_type::AttributeType,
        owns::Owns,
        plays::Plays,
        relation_type::RelationType,
        role_type::RoleType,
        type_manager::{type_reader::TypeReader, validation::SchemaValidationError},
        InterfaceImplementation, KindAPI, ObjectTypeAPI, Ordering, TypeAPI,
    },
};

pub(crate) fn get_label_or_concept_read_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, ConceptReadError> {
    TypeReader::get_label(snapshot, type_)?.ok_or(ConceptReadError::CannotGetLabelForExistingType)
}

pub(crate) fn get_label_or_schema_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, SchemaValidationError> {
    get_label_or_concept_read_err(snapshot, type_).map_err(SchemaValidationError::ConceptRead)
}

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
) -> Result<bool, ConceptReadError> {
    type_has_declared_annotation(snapshot, type_.clone(), Annotation::Abstract(AnnotationAbstract))
}

pub(crate) fn is_overridden_interface_object_one_of_supertypes_or_self<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    overridden: T,
) -> Result<bool, ConceptReadError> {
    if type_ == overridden {
        return Ok(true);
    }

    Ok(TypeReader::get_supertypes(snapshot, type_.clone())?.contains(&overridden.clone()))
}

pub(crate) fn is_overridden_interface_object_declared_supertype_or_self<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    overridden: T,
) -> Result<bool, ConceptReadError> {
    if type_ == overridden {
        return Ok(true);
    }

    Ok(TypeReader::get_supertype(snapshot, type_.clone())? == Some(overridden.clone()))
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

pub(crate) fn validate_declared_annotation_is_compatible_with_inherited_annotations(
    snapshot: &impl ReadableSnapshot,
    type_: impl KindAPI<'static>,
    annotation_category: AnnotationCategory,
) -> Result<(), SchemaValidationError> {
    let existing_annotations =
        TypeReader::get_type_annotations(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;

    for (existing_annotation, _) in existing_annotations {
        let existing_annotation_category = existing_annotation.clone().into().category();
        if !annotation_category.declarable_below(existing_annotation_category) {
            return Err(SchemaValidationError::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(
                annotation_category,
                existing_annotation_category,
                get_label_or_concept_read_err(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?,
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_declared_edge_annotation_is_compatible_with_inherited_annotations<EDGE>(
    snapshot: &impl ReadableSnapshot,
    edge: EDGE,
    annotation_category: AnnotationCategory,
) -> Result<(), SchemaValidationError>
where
    EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
{
    let existing_annotations =
        TypeReader::get_type_edge_annotations(snapshot, edge.clone()).map_err(SchemaValidationError::ConceptRead)?;

    for (existing_annotation, _) in existing_annotations {
        let existing_annotation_category = existing_annotation.category();
        if !annotation_category.declarable_below(existing_annotation_category) {
            let interface = edge.interface();
            return Err(SchemaValidationError::DeclaredAnnotationIsNotCompatibleWithInheritedAnnotation(
                annotation_category,
                existing_annotation_category,
                get_label_or_concept_read_err(snapshot, interface).map_err(SchemaValidationError::ConceptRead)?,
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_declared_annotation_is_compatible_with_declared_annotations(
    snapshot: &impl ReadableSnapshot,
    type_: impl KindAPI<'static>,
    annotation_category: AnnotationCategory,
) -> Result<(), SchemaValidationError> {
    let existing_annotations = TypeReader::get_type_annotations_declared(snapshot, type_.clone())
        .map_err(SchemaValidationError::ConceptRead)?;

    for existing_annotation in existing_annotations {
        let existing_annotation_category = existing_annotation.clone().into().category();
        if !existing_annotation_category.declarable_alongside(annotation_category) {
            return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                annotation_category,
                existing_annotation_category,
                get_label_or_concept_read_err(snapshot, type_).map_err(SchemaValidationError::ConceptRead)?,
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_declared_edge_annotation_is_compatible_with_declared_annotations<EDGE>(
    snapshot: &impl ReadableSnapshot,
    edge: EDGE,
    annotation_category: AnnotationCategory,
) -> Result<(), SchemaValidationError>
where
    EDGE: TypeEdgeEncoding<'static> + InterfaceImplementation<'static> + Clone,
{
    let existing_annotations = TypeReader::get_type_edge_annotations_declared(snapshot, edge.clone())
        .map_err(SchemaValidationError::ConceptRead)?;

    for existing_annotation in existing_annotations {
        let existing_annotation_category = existing_annotation.category();
        if !existing_annotation_category.declarable_alongside(annotation_category) {
            let interface = edge.interface();
            return Err(SchemaValidationError::AnnotationIsNotCompatibleWithDeclaredAnnotation(
                annotation_category,
                existing_annotation_category,
                get_label_or_concept_read_err(snapshot, interface).map_err(SchemaValidationError::ConceptRead)?,
            ));
        }
    }

    Ok(())
}

// TODO: Try to wrap all these type_has_***annotation and edge_has_***annotation into several macros!

// TODO: Refactor to type_get_declared_annotation
pub(crate) fn type_has_declared_annotation<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    annotation: Annotation,
) -> Result<bool, ConceptReadError> {
    let has = TypeReader::get_type_annotations_declared(snapshot, type_.clone())?
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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::label::Label;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationCardinality, AnnotationCategory, AnnotationKey, AnnotationRange, AnnotationRegex,
            AnnotationValues,
        },
        Capability,
        KindAPI,
        Ordering,
        owns::Owns,
        relation_type::RelationType, role_type::RoleType, type_manager::{type_reader::TypeReader, TypeManager, validation::SchemaValidationError}, TypeAPI,
    },
};

pub(crate) fn get_label_or_concept_read_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, ConceptReadError> {
    TypeReader::get_label(snapshot, type_)?.ok_or(ConceptReadError::CorruptMissingLabelOfType)
}

pub(crate) fn get_label_or_schema_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, SchemaValidationError> {
    get_label_or_concept_read_err(snapshot, type_).map_err(SchemaValidationError::ConceptRead)
}

pub(crate) fn validate_role_name_uniqueness_non_transitive(
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

pub(crate) fn is_interface_overridden<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    object_type: CAP::ObjectType,
    interface_type: CAP::InterfaceType,
) -> Result<bool, ConceptReadError> {
    let all_overridden = TypeReader::get_overridden_interfaces::<CAP>(snapshot, object_type)?;
    Ok(all_overridden.contains_key(&interface_type))
}

pub(crate) fn validate_declared_annotation_is_compatible_with_other_inherited_annotations(
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

pub(crate) fn validate_declared_edge_annotation_is_compatible_with_other_inherited_annotations<EDGE>(
    snapshot: &impl ReadableSnapshot,
    edge: EDGE,
    annotation_category: AnnotationCategory,
) -> Result<(), SchemaValidationError>
where
    EDGE: Capability<'static>,
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

pub(crate) fn validate_key_narrows_inherited_cardinality<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    edge: CAP,
    overridden_edge: CAP,
) -> Result<(), SchemaValidationError> {
    let supertype_cardinality =
        overridden_edge.get_cardinality(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?;

    if supertype_cardinality.narrowed_correctly_by(&AnnotationKey::CARDINALITY) {
        Ok(())
    } else {
        Err(SchemaValidationError::KeyShouldNarrowInheritedCardinality(
            get_label_or_schema_err(snapshot, edge.object())?,
            get_label_or_schema_err(snapshot, overridden_edge.object())?,
            get_label_or_schema_err(snapshot, edge.interface())?,
            supertype_cardinality,
        ))
    }
}

pub(crate) fn validate_cardinality_narrows_inherited_cardinality<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    edge: CAP,
    overridden_edge: CAP,
    cardinality: AnnotationCardinality,
) -> Result<(), SchemaValidationError> {
    let overridden_cardinality =
        overridden_edge.get_cardinality(snapshot, type_manager).map_err(SchemaValidationError::ConceptRead)?;

    if overridden_cardinality.narrowed_correctly_by(&cardinality) {
        Ok(())
    } else {
        Err(SchemaValidationError::CardinalityDoesNotNarrowInheritedCardinality(
            CAP::KIND,
            get_label_or_schema_err(snapshot, edge.object())?,
            get_label_or_schema_err(snapshot, overridden_edge.object())?,
            get_label_or_schema_err(snapshot, edge.interface())?,
            cardinality,
            overridden_cardinality,
        ))
    }
}

pub(crate) fn validate_type_regex_narrows_inherited_regex<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    attribute_type: T,
    supertype: Option<T>,
    regex: AnnotationRegex,
) -> Result<(), SchemaValidationError> {
    let supertype = match supertype {
        None => {
            TypeReader::get_supertype(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => supertype,
    };

    if let Some(supertype) = supertype {
        if let Some(supertype_annotation) =
            type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Regex)?
        {
            match supertype_annotation {
                Annotation::Regex(supertype_regex) => {
                    return if supertype_regex.regex() == regex.regex() {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::OnlyOneRegexCanBeSetForTypeHierarchy(
                            get_label_or_schema_err(snapshot, attribute_type)?,
                            get_label_or_schema_err(snapshot, supertype)?,
                            regex.clone(),
                            supertype_regex.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Regex-related function"),
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_edge_regex_narrows_inherited_regex<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    owns: CAP,
    overridden_owns: Option<CAP>,
    regex: AnnotationRegex,
) -> Result<(), SchemaValidationError> {
    let overridden_owns = match overridden_owns {
        None => {
            TypeReader::get_capability_override(snapshot, owns.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => overridden_owns,
    };

    if let Some(override_owns) = overridden_owns {
        if let Some(supertype_annotation) =
            edge_get_annotation_by_category(snapshot, override_owns.clone(), AnnotationCategory::Regex)?
        {
            match supertype_annotation {
                Annotation::Regex(supertype_regex) => {
                    return if supertype_regex.regex() == regex.regex() {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::OnlyOneRegexCanBeSetForTypeEdgeHierarchy(
                            get_label_or_schema_err(snapshot, owns.object())?,
                            get_label_or_schema_err(snapshot, override_owns.object())?,
                            get_label_or_schema_err(snapshot, owns.interface())?,
                            regex.clone(),
                            supertype_regex.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Regex-related function"),
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_type_range_narrows_inherited_range<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    attribute_type: T,
    supertype: Option<T>,
    range: AnnotationRange,
) -> Result<(), SchemaValidationError> {
    let supertype = match supertype {
        None => {
            TypeReader::get_supertype(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => supertype,
    };

    if let Some(supertype) = supertype {
        if let Some(supertype_annotation) =
            type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Range)?
        {
            match supertype_annotation {
                Annotation::Range(supertype_range) => {
                    return if supertype_range.narrowed_correctly_by(&range) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::RangeShouldNarrowInheritedRange(
                            get_label_or_schema_err(snapshot, attribute_type)?,
                            get_label_or_schema_err(snapshot, supertype)?,
                            range.clone(),
                            supertype_range.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Range-related function"),
            }
        }
    }

    Ok(())
}

// TODO: Wrap into macro all these similar checks?
pub(crate) fn validate_edge_range_narrows_inherited_range<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    owns: CAP,
    overridden_owns: Option<CAP>,
    range: AnnotationRange,
) -> Result<(), SchemaValidationError> {
    let overridden_owns = match overridden_owns {
        None => {
            TypeReader::get_capability_override(snapshot, owns.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => overridden_owns,
    };

    if let Some(override_owns) = overridden_owns {
        if let Some(supertype_annotation) =
            edge_get_annotation_by_category(snapshot, override_owns.clone(), AnnotationCategory::Range)?
        {
            match supertype_annotation {
                Annotation::Range(supertype_range) => {
                    return if supertype_range.narrowed_correctly_by(&range) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::RangeShouldNarrowInheritedEdgeRange(
                            get_label_or_schema_err(snapshot, owns.object())?,
                            get_label_or_schema_err(snapshot, override_owns.object())?,
                            get_label_or_schema_err(snapshot, owns.interface())?,
                            range.clone(),
                            supertype_range.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Range-related function"),
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_type_values_narrows_inherited_values<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    attribute_type: T,
    supertype: Option<T>,
    values: AnnotationValues,
) -> Result<(), SchemaValidationError> {
    let supertype = match supertype {
        None => {
            TypeReader::get_supertype(snapshot, attribute_type.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => supertype,
    };

    if let Some(supertype) = supertype {
        if let Some(supertype_annotation) =
            type_get_annotation_by_category(snapshot, supertype.clone(), AnnotationCategory::Values)?
        {
            match supertype_annotation {
                Annotation::Values(supertype_values) => {
                    return if supertype_values.narrowed_correctly_by(&values) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::ValuesShouldNarrowInheritedValues(
                            get_label_or_schema_err(snapshot, attribute_type)?,
                            get_label_or_schema_err(snapshot, supertype)?,
                            values.clone(),
                            supertype_values.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Values-related function"),
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_edge_values_narrows_inherited_values<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    owns: CAP,
    overridden_owns: Option<CAP>,
    values: AnnotationValues,
) -> Result<(), SchemaValidationError> {
    let overridden_owns = match overridden_owns {
        None => {
            TypeReader::get_capability_override(snapshot, owns.clone()).map_err(SchemaValidationError::ConceptRead)?
        }
        Some(_) => overridden_owns,
    };

    if let Some(override_owns) = overridden_owns {
        if let Some(supertype_annotation) =
            edge_get_annotation_by_category(snapshot, override_owns.clone(), AnnotationCategory::Values)?
        {
            match supertype_annotation {
                Annotation::Values(supertype_values) => {
                    return if supertype_values.narrowed_correctly_by(&values) {
                        Ok(())
                    } else {
                        Err(SchemaValidationError::ValuesShouldNarrowInheritedEdgeValues(
                            get_label_or_schema_err(snapshot, owns.object())?,
                            get_label_or_schema_err(snapshot, override_owns.object())?,
                            get_label_or_schema_err(snapshot, owns.interface())?,
                            values.clone(),
                            supertype_values.clone(),
                        ))
                    };
                }
                _ => unreachable!("Should not reach it for Values-related function"),
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_type_annotations_narrowing_of_inherited_annotations<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    subtype: T,
    supertype: T,
    subtype_annotation: T::AnnotationType,
) -> Result<(), SchemaValidationError> {
    match subtype_annotation.into() {
        Annotation::Abstract(_) => {
            validate_type_supertype_abstractness(snapshot, subtype.clone(), Some(supertype.clone()), Some(true))?
        }
        Annotation::Regex(regex) => {
            validate_type_regex_narrows_inherited_regex(snapshot, subtype.clone(), Some(supertype.clone()), regex)?
        }
        Annotation::Range(range) => {
            validate_type_range_narrows_inherited_range(snapshot, subtype.clone(), Some(supertype.clone()), range)?
        }
        Annotation::Values(values) => {
            validate_type_values_narrows_inherited_values(snapshot, subtype.clone(), Some(supertype.clone()), values)?
        }
        | Annotation::Distinct(_)
        | Annotation::Independent(_)
        | Annotation::Unique(_)
        | Annotation::Key(_)
        | Annotation::Cardinality(_)
        | Annotation::Cascade(_) => {}
    }
    Ok(())
}

pub(crate) fn validate_edge_annotations_narrowing_of_inherited_annotations<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    edge: CAP,
    overridden_edge: CAP,
    edge_annotation: Annotation,
) -> Result<(), SchemaValidationError> {
    match edge_annotation {
        Annotation::Cardinality(cardinality) => validate_cardinality_narrows_inherited_cardinality(
            snapshot,
            type_manager,
            edge.clone(),
            overridden_edge.clone(),
            cardinality,
        )?,
        Annotation::Key(_) => {
            validate_key_narrows_inherited_cardinality(snapshot, type_manager, edge.clone(), overridden_edge.clone())?
        }
        Annotation::Regex(regex) => {
            validate_edge_regex_narrows_inherited_regex(snapshot, edge.clone(), Some(overridden_edge.clone()), regex)?
        }
        Annotation::Range(range) => {
            validate_edge_range_narrows_inherited_range(snapshot, edge.clone(), Some(overridden_edge.clone()), range)?
        }
        Annotation::Values(values) => validate_edge_values_narrows_inherited_values(
            snapshot,
            edge.clone(),
            Some(overridden_edge.clone()),
            values,
        )?,
        | Annotation::Abstract(_)
        | Annotation::Distinct(_)
        | Annotation::Independent(_)
        | Annotation::Unique(_)
        | Annotation::Cascade(_) => {}
    }
    Ok(())
}

pub(crate) fn validate_type_supertype_ordering_match(
    snapshot: &impl ReadableSnapshot,
    type_: RoleType<'static>,
    supertype: RoleType<'static>,
    set_subtype_role_ordering: Option<Ordering>,
) -> Result<(), SchemaValidationError> {
    let supertype_ordering =
        TypeReader::get_type_ordering(snapshot, supertype.clone()).map_err(SchemaValidationError::ConceptRead)?;
    let type_ordering = set_subtype_role_ordering
        .unwrap_or(TypeReader::get_type_ordering(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?);

    if type_ordering == supertype_ordering {
        Ok(())
    } else {
        Err(SchemaValidationError::OrderingDoesNotMatchWithSupertype(
            get_label_or_schema_err(snapshot, type_)?,
            get_label_or_schema_err(snapshot, supertype)?,
            type_ordering,
            supertype_ordering,
        ))
    }
}

pub(crate) fn validate_edge_override_ordering_match(
    snapshot: &impl ReadableSnapshot,
    edge: Owns<'static>,
    overridden_edge: Owns<'static>,
    set_subtype_owns_ordering: Option<Ordering>,
) -> Result<(), SchemaValidationError> {
    let edge_ordering = set_subtype_owns_ordering.unwrap_or(
        TypeReader::get_type_edge_ordering(snapshot, edge.clone()).map_err(SchemaValidationError::ConceptRead)?,
    );
    let overridden_edge_ordering = TypeReader::get_type_edge_ordering(snapshot, overridden_edge.clone())
        .map_err(SchemaValidationError::ConceptRead)?;

    if edge_ordering == overridden_edge_ordering {
        Ok(())
    } else {
        Err(SchemaValidationError::OrderingDoesNotMatchWithOverride(
            get_label_or_schema_err(snapshot, edge.owner())?,
            get_label_or_schema_err(snapshot, overridden_edge.owner())?,
            get_label_or_schema_err(snapshot, edge.attribute())?,
            edge_ordering,
            overridden_edge_ordering,
        ))
    }
}

pub(crate) fn validate_type_supertype_abstractness<T>(
    snapshot: &impl ReadableSnapshot,
    subtype: T,
    supertype: Option<T>,
    set_subtype_abstract: Option<bool>,
) -> Result<(), SchemaValidationError>
where
    T: KindAPI<'static>,
{
    let supertype = match supertype {
        None => TypeReader::get_supertype(snapshot, subtype.clone()).map_err(SchemaValidationError::ConceptRead)?,
        Some(_) => supertype,
    };

    if let Some(supertype) = supertype {
        let subtype_abstract = set_subtype_abstract.unwrap_or(type_has_declared_annotation_category(
            snapshot,
            subtype.clone(),
            AnnotationCategory::Abstract,
        )?);
        let supertype_abstract =
            type_has_declared_annotation_category(snapshot, supertype.clone(), AnnotationCategory::Abstract)?;

        match (subtype_abstract, supertype_abstract) {
            (false, false) | (false, true) | (true, true) => Ok(()),
            (true, false) => Err(SchemaValidationError::AbstractTypesSupertypeHasToBeAbstract(
                get_label_or_schema_err(snapshot, supertype)?,
                get_label_or_schema_err(snapshot, subtype)?,
            )),
        }
    } else {
        Ok(())
    }
}

// TODO: Try to wrap all these type_has_***annotation and edge_has_***annotation into several macros!

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
        .keys()
        .map(|annotation| annotation.clone().into().category())
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
        .into_keys()
        .find(|found_annotation| found_annotation.clone().into().category() == annotation_category);
    Ok(annotation.map(|val| val.clone().into()))
}

pub(crate) fn edge_get_annotation_by_category<EDGE>(
    snapshot: &impl ReadableSnapshot,
    edge: EDGE,
    annotation_category: AnnotationCategory,
) -> Result<Option<Annotation>, SchemaValidationError>
where
    EDGE: Capability<'static> + Clone,
{
    let annotation = TypeReader::get_type_edge_annotations(snapshot, edge.clone())
        .map_err(SchemaValidationError::ConceptRead)?
        .into_keys()
        .find(|found_annotation| found_annotation.category() == annotation_category);
    Ok(annotation)
}

pub(crate) fn type_get_owner_of_annotation_category<T: KindAPI<'static>>(
    snapshot: &impl ReadableSnapshot,
    type_: T,
    annotation_category: AnnotationCategory,
) -> Result<Option<T>, SchemaValidationError> {
    let annotations =
        TypeReader::get_type_annotations(snapshot, type_.clone()).map_err(SchemaValidationError::ConceptRead)?;
    let found_annotation = annotations
        .iter()
        .map(|(existing_annotation, source)| (existing_annotation.clone().into().category(), source))
        .find(|(existing_category, _)| existing_category == &annotation_category);

    Ok(found_annotation.map(|(_, owner)| owner.clone()))
}

pub(crate) fn edge_get_owner_of_annotation_category<CAP: Capability<'static>>(
    snapshot: &impl ReadableSnapshot,
    edge: CAP,
    annotation_category: AnnotationCategory,
) -> Result<Option<CAP>, SchemaValidationError> {
    let annotations =
        TypeReader::get_type_edge_annotations(snapshot, edge.clone()).map_err(SchemaValidationError::ConceptRead)?;
    let found_annotation = annotations
        .iter()
        .map(|(existing_annotation, source)| (existing_annotation.clone().category(), source))
        .find(|(existing_category, _)| existing_category == &annotation_category);

    Ok(found_annotation.map(|(_, owner)| owner.clone()))
}

pub(crate) fn is_ordering_compatible_with_distinct_annotation(ordering: Ordering, distinct_set: bool) -> bool {
    if distinct_set {
        ordering == Ordering::Ordered
    } else {
        true
    }
}

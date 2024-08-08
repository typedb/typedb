/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    thing::thing_manager::ThingManager,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory,
            AnnotationDistinct, AnnotationError, AnnotationIndependent, AnnotationKey, AnnotationRange,
            AnnotationRegex, AnnotationUnique, AnnotationValues, DefaultFrom,
        },
        owns::Owns,
        type_manager::{type_reader::TypeReader, TypeManager},
        Capability,
    },
};

pub struct Constraint {}

macro_rules! compute_constraint_one_to_one_annotation {
    ($func_name:ident, $annotation_enum:path, $annotation_type:ident) => {
        pub fn $func_name<'a, A: Into<Annotation> + Clone + 'a>(
            annotations: impl IntoIterator<Item = &'a A>,
        ) -> Option<$annotation_type> {
            annotations
                .into_iter()
                .filter_map(|annotation| match annotation.clone().into() {
                    $annotation_enum(annotation) => Some(annotation),
                    _ => None,
                })
                .next()
        }
    };
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ConstraintValidationMode {
    Type,                       // or Capability
    TypeAndSiblings,            // or CapabilityAndSiblings (siblings = override the same capability)
    TypeAndSiblingsAndSubtypes, // or CapabilityAndSiblingsAndOverridingCapabilities
}

impl Constraint {
    compute_constraint_one_to_one_annotation!(compute_abstract, Annotation::Abstract, AnnotationAbstract);
    compute_constraint_one_to_one_annotation!(compute_distinct, Annotation::Distinct, AnnotationDistinct);
    compute_constraint_one_to_one_annotation!(compute_independent, Annotation::Independent, AnnotationIndependent);
    compute_constraint_one_to_one_annotation!(compute_key, Annotation::Key, AnnotationKey);

    pub fn compute_unique<'a, A: Into<Annotation> + Clone + 'a>(
        annotations: impl IntoIterator<Item = &'a A>,
    ) -> Option<AnnotationUnique> {
        annotations
            .into_iter()
            .filter_map(|annotation| match annotation.clone().into() {
                // Unique and Key cannot be set together
                Annotation::Unique(_) | Annotation::Key(_) => Some(AnnotationUnique),
                _ => None,
            })
            .next()
    }

    pub fn compute_cardinality<'a, A: Into<Annotation> + Clone + 'a>(
        annotations: impl IntoIterator<Item = &'a A>,
        default: Option<AnnotationCardinality>,
    ) -> Option<AnnotationCardinality> {
        let cardinality = annotations
            .into_iter()
            .filter_map(|annotation| match annotation.clone().into() {
                // Cardinality and Key cannot be set together
                Annotation::Cardinality(card) => Some(card),
                Annotation::Key(_) => Some(AnnotationKey::CARDINALITY),
                _ => None,
            })
            .next();
        return if cardinality.is_some() { cardinality } else { default };
    }

    compute_constraint_one_to_one_annotation!(compute_regex, Annotation::Regex, AnnotationRegex);
    compute_constraint_one_to_one_annotation!(compute_cascade, Annotation::Cascade, AnnotationCascade);
    compute_constraint_one_to_one_annotation!(compute_range, Annotation::Range, AnnotationRange);
    compute_constraint_one_to_one_annotation!(compute_values, Annotation::Values, AnnotationValues);

    pub fn inherited_constraint_validation_mode(
        annotation_category: AnnotationCategory,
    ) -> HashSet<ConstraintValidationMode> {
        match annotation_category {
            AnnotationCategory::Abstract => HashSet::from([ConstraintValidationMode::Type]),
            AnnotationCategory::Distinct => HashSet::from([ConstraintValidationMode::Type]),
            AnnotationCategory::Independent => HashSet::from([]),
            AnnotationCategory::Unique => HashSet::from([ConstraintValidationMode::TypeAndSiblingsAndSubtypes]),
            AnnotationCategory::Cardinality =>
            // ::Type for min, ::TypeAndSiblings for max
            {
                HashSet::from([ConstraintValidationMode::Type, ConstraintValidationMode::TypeAndSiblings])
            }
            AnnotationCategory::Key => Self::inherited_constraint_validation_mode(AnnotationCategory::Unique)
                .union(&Self::inherited_constraint_validation_mode(AnnotationCategory::Cardinality))
                .cloned()
                .collect(),
            AnnotationCategory::Regex => HashSet::from([ConstraintValidationMode::Type]),
            AnnotationCategory::Cascade => HashSet::from([]),
            AnnotationCategory::Range => HashSet::from([ConstraintValidationMode::Type]),
            AnnotationCategory::Values => HashSet::from([ConstraintValidationMode::Type]),
        }
    }

    pub(crate) fn sort_annotations_by_inherited_constraint_validation_modes<'a, A: Into<Annotation> + Clone + 'a>(
        annotations: impl IntoIterator<Item = A>,
    ) -> Result<HashMap<ConstraintValidationMode, HashSet<Annotation>>, AnnotationError> {
        let mut map: HashMap<ConstraintValidationMode, HashSet<Annotation>> = HashMap::new();

        for annotation in annotations.into_iter() {
            let annotation = annotation.clone().into();
            let modes = Self::inherited_constraint_validation_mode(annotation.category());

            for mode in modes {
                map.entry(mode).or_insert_with(HashSet::default).insert(annotation.clone());
            }
        }

        Ok(map)
    }
}

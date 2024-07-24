/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{
            Annotation, AnnotationAbstract, AnnotationCardinality, AnnotationCascade, AnnotationCategory,
            AnnotationDistinct, AnnotationIndependent, AnnotationKey, AnnotationRange, AnnotationRegex,
            AnnotationUnique, AnnotationValues, DefaultFrom,
        },
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
}

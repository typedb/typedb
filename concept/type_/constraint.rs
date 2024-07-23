/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use primitive::maybe_owns::MaybeOwns;

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationKey},
        Capability,
    },
};

pub struct Constraint {}

impl Constraint {
    pub fn compute_cardinality<'a, 'b, CAP: Capability<'a>>(
        annotations: MaybeOwns<'b, HashMap<CAP::AnnotationType, CAP>>,
        default: AnnotationCardinality,
    ) -> AnnotationCardinality {
        annotations
            .iter()
            .filter_map(|(annotation, _)| match annotation.clone().into() {
                // Cardinality and Key cannot be set together
                Annotation::Cardinality(card) => Some(card),
                Annotation::Key(_) => Some(AnnotationKey::CARDINALITY),
                _ => None,
            })
            .next()
            .unwrap_or(default)
    }
}

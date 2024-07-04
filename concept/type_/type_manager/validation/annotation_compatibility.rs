/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use crate::type_::annotation::Annotation;

pub(crate) fn is_annotation_inheritable<T, TAnnotation>(
    supertype_annotation: &TAnnotation,
    effective_annotations: &HashMap<TAnnotation, T>,
) -> bool
where
    TAnnotation: Clone + Into<Annotation>,
{
    let supertype_category = supertype_annotation.clone().into().category();
    if !supertype_category.inheritable() {
        return false;
    }

    for (effective_annotation, _) in effective_annotations {
        let effective_category = effective_annotation.clone().into().category();
        if effective_category == supertype_category {
            return false;
        }
        if !supertype_category.inheritable_alongside(effective_category) {
            return false;
        }
    }

    true
}

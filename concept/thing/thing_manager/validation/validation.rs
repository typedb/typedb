/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::{label::Label, ValueEncodable};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::{
        object::ObjectAPI,
        thing_manager::{validation::DataValidationError, ThingManager},
    },
    type_::{
        annotation::{AnnotationCategory, AnnotationKey, AnnotationUnique},
        owns::{Owns, OwnsAnnotation},
        type_manager::TypeManager,
        Capability, TypeAPI,
    },
};

pub(crate) fn get_label_or_concept_read_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, ConceptReadError> {
    type_.get_label(snapshot, type_manager).map(|label| label.clone())
}

pub(crate) fn get_label_or_data_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, DataValidationError> {
    get_label_or_concept_read_err(snapshot, type_manager, type_).map_err(DataValidationError::ConceptRead)
}

pub(crate) fn get_uniqueness_source(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    owns: Owns<'static>,
) -> Result<Option<Owns<'static>>, ConceptReadError> {
    debug_assert!(
        !AnnotationCategory::Unique.declarable_below(AnnotationCategory::Key)
            && AnnotationCategory::Key.declarable_below(AnnotationCategory::Unique),
        "This function uses the fact that @key is always below @unique. Revalidate the logic!"
    );

    if owns.is_unique(snapshot, thing_manager.type_manager())? {
        let unique_source = thing_manager.type_manager().get_capability_annotation_source(
            snapshot,
            owns.clone(),
            OwnsAnnotation::Unique(AnnotationUnique),
        )?;
        Ok(match unique_source {
            Some(_) => unique_source,
            None => {
                let key_source = thing_manager.type_manager().get_capability_annotation_source(
                    snapshot,
                    owns.clone(),
                    OwnsAnnotation::Key(AnnotationKey),
                )?;
                match key_source {
                    Some(_) => key_source,
                    None => panic!("AnnotationUnique or AnnotationKey should exist if owns is unique!"),
                }
            }
        })
    } else {
        Ok(None)
    }
}

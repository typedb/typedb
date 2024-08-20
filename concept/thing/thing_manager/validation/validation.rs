/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::{label::Label};
use storage::snapshot::ReadableSnapshot;

use crate::{
    thing::{
        object::ObjectAPI,
        thing_manager::{validation::DataValidationError},
    },
    type_::{
        type_manager::TypeManager,
        Capability, TypeAPI,
    },
};

pub(crate) fn get_label_or_data_err<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: impl TypeAPI<'a>,
) -> Result<Label<'static>, DataValidationError> {
    type_.get_label_cloned(snapshot, type_manager).map(|label| label.into_owned()).map_err(DataValidationError::ConceptRead)
}

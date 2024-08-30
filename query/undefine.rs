/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use storage::snapshot::WritableSnapshot;
use typeql::query::schema::Undefine;

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    todo!()
}

#[derive(Debug)]
pub enum UndefineError {
    Unimplemented,
}

impl fmt::Display for UndefineError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for UndefineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        todo!()
    }
}

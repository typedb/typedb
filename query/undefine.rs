/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::query::schema::Undefine;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use error::typedb_error;
use storage::snapshot::WritableSnapshot;

pub(crate) fn execute(
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    thing_manager: &ThingManager,
    undefine: Undefine,
) -> Result<(), UndefineError> {
    Err(UndefineError::Unimplemented { description: "Undefine queries are not yet implemented.".to_string() })
}

typedb_error!(
    pub UndefineError(component = "Undefine execution", prefix = "UEX") {
        Unimplemented(1, "Unimplemented undefine functionality: {description}", description: String),
    }
);

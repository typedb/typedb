/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::{ConceptReadError, ConceptWriteError};
use error::typedb_error;

pub(crate) mod write_instruction;

typedb_error! {
    pub WriteError(component = "Write execution", prefix = "WEX") {
        ConceptWrite(1, "Write execution failed due to a concept write error.", typedb_source: Box<ConceptWriteError>),
        ConceptRead(2, "Write execution failed due to a concept read error.", typedb_source: Box<ConceptReadError>),
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;

use crate::TypeDBError;

// Adapt TypeQL error as a TypeDB error so the errors are visible in the stack trace
impl TypeDBError for typeql::Error {
    fn variant_name(&self) -> &'static str {
        "TypeQLUsage"
    }

    fn component(&self) -> &'static str {
        "TypeQL usage"
    }

    fn code(&self) -> &'static str {
        "TQL0"
    }

    fn code_prefix(&self) -> &'static str {
        "TQL"
    }

    fn code_number(&self) -> usize {
        0
    }

    fn format_description(&self) -> String {
        format!("{}\nCaused: Error in usage of TypeQL.", self)
    }

    fn source(&self) -> Option<&(dyn Error + Sync)> {
        None
    }

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)> {
        None
    }
}

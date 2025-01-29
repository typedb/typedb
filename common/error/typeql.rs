/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use typeql::common::Span;

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

    fn source_error(&self) -> Option<&(dyn Error + Sync)> {
        None
    }

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)> {
        None
    }

    fn source_query(&self) -> Option<&str> {
        // don't plug into TypeDB's source_query, since this error manages its own query+span indicators
        None
    }

    fn source_span(&self) -> Option<(Span)> {
        // don't plug into TypeDB's source_span, since this error manages its own query+span indicators
        None
    }
}

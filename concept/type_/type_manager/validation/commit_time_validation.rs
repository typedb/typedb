/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use storage::snapshot::ReadableSnapshot;
use crate::error::ConceptReadError;
use crate::type_::type_manager::validation::SchemaValidationError;

pub struct CommitTimeValidation { }

impl CommitTimeValidation {
    pub(crate) fn validate<Snapshot>(snapshot: &Snapshot) -> Result<Vec<SchemaValidationError>, ConceptReadError>
    where Snapshot: ReadableSnapshot
    {
        // TODO: See https://github.com/vaticle/typedb/pull/6981/ for an outline on validation
        Ok(vec!())
    }
}
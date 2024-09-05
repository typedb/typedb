/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use error::typedb_error;

typedb_error!(
    pub ReadExecutionError(domain = "ReadExecution", prefix = "REX") {
        Interrupted(1, "Execution was interrupted."),
        ConceptRead(2, "Concept read error.", ( source: ConceptReadError )),
        CreatingIterator(3, "Error creating iterator from {instruction_name} instruction.", instruction_name: String, ( source: ConceptReadError )),
        AdvancingIteratorTo(4, "Error moving iterator (by steps or seek) to target value.", ( source: ConceptReadError )),
    }
);

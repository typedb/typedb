/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::annotation::expression::instructions::ExpressionEvaluationError;
use concept::error::ConceptReadError;
use error::typedb_error;

use crate::InterruptType;

typedb_error!(
    pub ReadExecutionError(component = "Read execution", prefix = "REX") {
        Interrupted(1, "Execution interrupted by to a concurrent {interrupt}.", interrupt: InterruptType),
        ConceptRead(2, "Concept read error.", ( source: Box<ConceptReadError> )),
        CreatingIterator(3, "Error creating iterator from {instruction_name} instruction.", instruction_name: String, ( source: Box<ConceptReadError> )),
        AdvancingIteratorTo(4, "Error moving iterator (by steps or seek) to target value.", ( source: Box<ConceptReadError> )),
        ExpressionEvaluate(5, "Error evaluating expression", ( source: ExpressionEvaluationError )),

        UnimplementedCyclicFunctions(999, "A cyclic function-call was detected. The results are sound but may be incomplete. This will be fixed in a later alpha release."),
    }
);

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use crate::annotation::expression::instructions::{op_codes::ExpressionOpCode, ExpressionInstruction};

pub struct ListConstructor {}
pub struct ListIndex {}
pub struct ListIndexRange {}

impl ExpressionInstruction for ListConstructor {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListConstructor;
}

impl ExpressionInstruction for ListIndex {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListIndex;
}

impl ExpressionInstruction for ListIndexRange {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::ListIndexRange;
}

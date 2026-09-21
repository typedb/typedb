/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::annotation::expression::instructions::{ExpressionInstruction, op_codes::ExpressionOpCode};

pub struct LoadVariable;
pub struct LoadConstant;
pub struct MayShortCircuitValue;
pub struct MayShortCircuitList;

impl ExpressionInstruction for LoadVariable {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadVariable;
}

impl ExpressionInstruction for LoadConstant {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::LoadConstant;
}

impl ExpressionInstruction for MayShortCircuitValue {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::MayShortCircuitValue;
}

impl ExpressionInstruction for MayShortCircuitList {
    const OP_CODE: ExpressionOpCode = ExpressionOpCode::MayShortCircuitList;
}

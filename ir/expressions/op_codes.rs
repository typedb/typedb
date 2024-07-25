/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug)]
pub enum ExpressionOpCode {
    // Load
    LoadConstant,
    LoadVariable,

    // Casts
    CastUnaryLongToDouble,
    CastLeftLongToDouble,
    CastRightLongToDouble,

    // Operators
    OpLongAddLong,
    OpDoubleAddDouble,
    OpLongMultiplyLong,

    // BuiltIns, maybe by domain?
    MathCeilDouble,
    MathFloorDouble,
}

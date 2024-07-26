/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: Rewrite so we generate the dispatcher macro along with the enum. SEe https://cprohm.de/blog/rust-macros/
#[derive(Debug)]
pub enum ExpressionOpCode {
    // Load
    LoadConstant,
    LoadVariable,

    // Casts
    // TODO: We can't cast arguments for functions of arity > 2. It may require rewriting compilation.
    CastUnaryLongToDouble,
    CastLeftLongToDouble,
    CastRightLongToDouble,

    // Operators
    OpLongAddLong,
    OpDoubleAddDouble,
    OpLongMultiplyLong,

    OpLongSubtractLong,
    OpLongDivideLong,
    OpLongModuloLong,
    OpLongPowerLong,

    OpDoubleSubtractDouble,
    OpDoubleMultiplyDouble,
    OpDoubleDivideDouble,
    OpDoubleModuloDouble,
    OpDoublePowerDouble,

    // BuiltIns, maybe by domain?
    MathAbsLong,
    MathAbsDouble,
    MathRemainderLong,
    MathRoundDouble,
    MathCeilDouble,
    MathFloorDouble,
}

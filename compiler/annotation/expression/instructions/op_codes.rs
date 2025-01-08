/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

// TODO: Rewrite so we generate the dispatcher macro along with the enum. SEe https://cprohm.de/blog/rust-macros/
#[derive(Debug, Clone)]
pub enum ExpressionOpCode {
    // Basics
    LoadConstant,
    LoadVariable,
    ListConstructor,
    ListIndex,
    ListIndexRange,

    // Casts
    // TODO: We can't cast arguments for functions of arity > 2. It may require rewriting compilation.
    CastUnaryIntegerToDouble,
    CastLeftIntegerToDouble,
    CastRightIntegerToDouble,
    CastUnaryIntegerToDecimal,

    CastLeftIntegerToDecimal,
    CastRightIntegerToDecimal,

    CastUnaryDecimalToDouble,
    CastLeftDecimalToDouble,
    CastRightDecimalToDouble,

    // Operators
    OpIntegerAddInteger,
    OpIntegerMultiplyInteger,
    OpIntegerSubtractInteger,
    OpIntegerDivideInteger,
    OpIntegerModuloInteger,
    OpIntegerPowerInteger,

    OpDoubleAddDouble,
    OpDoubleSubtractDouble,
    OpDoubleMultiplyDouble,
    OpDoubleDivideDouble,
    OpDoubleModuloDouble,
    OpDoublePowerDouble,

    OpDecimalAddDecimal,
    OpDecimalSubtractDecimal,
    OpDecimalMultiplyDecimal,

    // BuiltIns, maybe by domain?
    MathAbsDouble,
    MathAbsInteger,
    MathRemainderInteger,
    MathRoundDouble,
    MathCeilDouble,
    MathFloorDouble,
}

impl Display for ExpressionOpCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionOpCode::LoadConstant => write!(f, "load-constant"),
            ExpressionOpCode::LoadVariable => write!(f, "load-variable"),
            ExpressionOpCode::ListConstructor => write!(f, "list-constructor"),
            ExpressionOpCode::ListIndex => write!(f, "list-index"),
            ExpressionOpCode::ListIndexRange => write!(f, "list-range"),
            ExpressionOpCode::CastUnaryIntegerToDouble => write!(f, "cast-integer-to-double"),
            ExpressionOpCode::CastLeftIntegerToDouble => write!(f, "cast-left-integer-to-double"),
            ExpressionOpCode::CastRightIntegerToDouble => write!(f, "cast-right-integer-to-double"),
            ExpressionOpCode::CastUnaryDecimalToDouble => write!(f, "cast-decimal-to-double"),
            ExpressionOpCode::CastLeftDecimalToDouble => write!(f, "cast-left-decimal-to-double"),
            ExpressionOpCode::CastRightDecimalToDouble => write!(f, "cast-right-decimal-to-double"),
            ExpressionOpCode::OpIntegerAddInteger => write!(f, "add-integer"),
            ExpressionOpCode::OpIntegerMultiplyInteger =>  write!(f, "multiply-integer"),
            ExpressionOpCode::OpIntegerSubtractInteger =>  write!(f, "subtract-integer"),
            ExpressionOpCode::OpIntegerDivideInteger =>  write!(f, "divide-integer"),
            ExpressionOpCode::OpIntegerModuloInteger =>  write!(f, "modulo-integer"),
            ExpressionOpCode::OpIntegerPowerInteger =>  write!(f, "power-integer"),
            ExpressionOpCode::OpDoubleAddDouble => write!(f, "add-double"),
            ExpressionOpCode::OpDoubleSubtractDouble =>  write!(f, "subtract-double"),
            ExpressionOpCode::OpDoubleMultiplyDouble => write!(f, "multiply-double"),
            ExpressionOpCode::OpDoubleDivideDouble =>  write!(f, "divide-double"),
            ExpressionOpCode::OpDoubleModuloDouble => write!(f, "modulo-double"),
            ExpressionOpCode::OpDoublePowerDouble => write!(f, "power-double"),
            ExpressionOpCode::MathAbsInteger =>  write!(f, "abs-integer"),
            ExpressionOpCode::MathRemainderInteger => write!(f, "remainder-integer"),
            ExpressionOpCode::MathAbsDouble =>  write!(f, "abs-double"),
            ExpressionOpCode::MathRoundDouble =>  write!(f, "round-double"),
            ExpressionOpCode::MathCeilDouble => write!(f, "ceil-double"),
            ExpressionOpCode::MathFloorDouble =>  write!(f, "floor-double"),
        }
    }
}

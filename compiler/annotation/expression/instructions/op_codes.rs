/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

#[macro_export]
macro_rules! for_each_opcode {
    ($macro:ident) => {
        $macro! {
            LoadConstant
            LoadVariable
            ListConstructor
            ListIndex
            ListIndexRange

            CastUnaryIntegerToDouble
            CastLeftIntegerToDouble
            CastRightIntegerToDouble
            CastUnaryIntegerToDecimal
            CastLeftIntegerToDecimal
            CastRightIntegerToDecimal
            CastUnaryDecimalToDouble
            CastLeftDecimalToDouble
            CastRightDecimalToDouble

            OpIntegerAddInteger
            OpIntegerMultiplyInteger
            OpIntegerSubtractInteger
            OpIntegerDivideInteger
            OpIntegerModuloInteger
            OpIntegerPowerInteger

            OpDoubleAddDouble
            OpDoubleSubtractDouble
            OpDoubleMultiplyDouble
            OpDoubleDivideDouble
            OpDoubleModuloDouble
            OpDoublePowerDouble

            OpDecimalAddDecimal
            OpDecimalSubtractDecimal
            OpDecimalMultiplyDecimal

            OpDateSubtractDate

            OpDateTimeAddDuration
            OpDateTimeSubtractDuration
            OpDateTimeSubtractDateTime
            OpDateTimeSubtractDate

            OpDateTimeTZAddDuration
            OpDateTimeTZSubtractDuration
            OpDateTimeTZSubtractDateTimeTZ

            OpDurationAddDuration
            OpDurationSubtractDuration

            OpStringAddString

            MathAbsDouble
            MathAbsDecimal
            MathAbsInteger

            MathRemainderInteger

            MathRoundDouble
            MathCeilDouble
            MathFloorDouble

            MathRoundDecimal
            MathCeilDecimal
            MathFloorDecimal

            MathMinIntegerInteger
            MathMinDoubleDouble
            MathMinDecimalDecimal

            MathMaxIntegerInteger
            MathMaxDoubleDouble
            MathMaxDecimalDecimal

            LenString
        }
    };
}

macro_rules! define_opcode_enum {
    ($($name:ident)*) => {
        #[derive(Debug, Clone)]
        pub enum ExpressionOpCode {
            $($name,)*
        }
    };
}

macro_rules! define_opcode_fmt_display {
    ($($name:ident)*) => {
        impl fmt::Display for ExpressionOpCode {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    $(Self::$name => write!(f, stringify!($name)),)*
                }
            }
        }
    };
}

for_each_opcode!(define_opcode_enum);
for_each_opcode!(define_opcode_fmt_display);

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    fmt,
    ops::{Add, Mul, Sub},
};

const FRACTIONAL_PART_DENOMINATOR: u64 = 10_000_000_000_000_000_000;

#[allow(clippy::assertions_on_constants)]
const _ASSERT: () = {
    assert!(FRACTIONAL_PART_DENOMINATOR > u64::MAX / 10);
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixedPoint {
    integer: i64,
    fractional: u64,
}

impl FixedPoint {
    pub const MIN: Self = Self::new(i64::MIN, 0);
    pub const MAX: Self = Self::new(i64::MAX, FRACTIONAL_PART_DENOMINATOR - 1);

    pub(crate) const fn new(integer: i64, fractional: u64) -> Self {
        assert!(fractional < FRACTIONAL_PART_DENOMINATOR);
        Self { integer, fractional }
    }

    pub(crate) fn integer_part(&self) -> i64 {
        self.integer
    }

    pub(crate) fn fractional_part(&self) -> u64 {
        self.fractional
    }
}

impl Add for FixedPoint {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let lhs = self;
        let (fractional, carry) = match lhs.fractional.overflowing_add(rhs.fractional) {
            (frac, false) if frac < FRACTIONAL_PART_DENOMINATOR => (frac, 0),
            (frac, true) if frac < FRACTIONAL_PART_DENOMINATOR => (frac + (u64::MAX - FRACTIONAL_PART_DENOMINATOR), 1),
            (frac, false) => (frac - u64::MAX, 1),
            (_, true) => unreachable!(),
        };
        let integer = lhs.integer + rhs.integer + carry;

        Self::new(integer, fractional)
    }
}

impl Sub for FixedPoint {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let lhs = self;
        let (fractional, carry) = match lhs.fractional.overflowing_sub(rhs.fractional) {
            (frac, false) => (frac, 0),
            (frac, true) => (frac.wrapping_add(FRACTIONAL_PART_DENOMINATOR), 1),
        };
        let integer = lhs.integer - rhs.integer - carry;

        Self::new(integer, fractional)
    }
}

impl Mul for FixedPoint {
    type Output = FixedPoint;

    fn mul(self, rhs: Self) -> Self::Output {
        let lhs = self;

        let extended_denominator = FRACTIONAL_PART_DENOMINATOR as u128;
        let fractional = (lhs.fractional as u128 * rhs.fractional as u128 + /* rounding! */ extended_denominator / 2)
            / extended_denominator;
        let carry = fractional / extended_denominator;
        let fractional = (fractional % extended_denominator) as u64;

        let integer = (lhs.integer * rhs.integer) as i128 // intentionally letting overflow occur before extending
            + lhs.fractional as i128 * rhs.integer as i128 / FRACTIONAL_PART_DENOMINATOR as i128
            + lhs.integer as i128 * rhs.fractional as i128 / FRACTIONAL_PART_DENOMINATOR as i128
            + carry as i128;

        Self::new(integer as i64, fractional)
    }
}

macro_rules! impl_integer_ops {
    (
        $($optrait:ident::$opname:ident),+ $(,)?
        for
        $types:tt
    ) => {$(impl_integer_ops! { @op $optrait::$opname for $types})+};
    (
        @op $optrait:ident::$opname:ident
        for
        { $($int:ty),+ $(,)? }
    ) => {$(
        impl $optrait<$int> for FixedPoint {
            type Output = FixedPoint;
            fn $opname(self, rhs: $int) -> Self::Output {
                $optrait::$opname(self, FixedPoint::new(rhs as i64, 0))
            }
        }
        impl $optrait<FixedPoint> for $int {
            type Output = FixedPoint;
            fn $opname(self, rhs: FixedPoint) -> Self::Output {
                $optrait::$opname(FixedPoint::new(self as i64, 0), rhs)
            }
        }
        impl $optrait<&FixedPoint> for $int {
            type Output = FixedPoint;
            fn $opname(self, rhs: &FixedPoint) -> Self::Output {
                $optrait::$opname(FixedPoint::new(self as i64, 0), rhs)
            }
        }
        impl $optrait<FixedPoint> for &$int {
            type Output = FixedPoint;
            fn $opname(self, rhs: FixedPoint) -> Self::Output {
                $optrait::$opname(FixedPoint::new(*self as i64, 0), rhs)
            }
        }
        impl $optrait<&FixedPoint> for &$int {
            type Output = FixedPoint;
            fn $opname(self, rhs: &FixedPoint) -> Self::Output {
                $optrait::$opname(FixedPoint::new(*self as i64, 0), rhs)
            }
        }
    )+};
}

impl_integer_ops! {
    Add::add, Sub::sub, Mul::mul
    for
    { u8, u16, u32, u64, i8, i16, i32, i64 }
}

macro_rules! impl_ref_ops {
    ($($optrait:ident::$opname:ident),+ $(,)?) => {$(
        impl<T> $optrait<&T> for FixedPoint
        where
            T: Copy,
            FixedPoint: $optrait<T>,
        {
            type Output = <FixedPoint as $optrait<T>>::Output;
            fn $opname(self, rhs: &T) -> Self::Output {
                <FixedPoint as $optrait<T>>::$opname(self, *rhs)
            }
        }

        impl<T> $optrait<T> for &FixedPoint
        where
            T: Copy,
            FixedPoint: $optrait<T>,
        {
            type Output = <FixedPoint as $optrait<T>>::Output;
            fn $opname(self, rhs: T) -> Self::Output {
                <FixedPoint as $optrait<T>>::$opname(*self, rhs)
            }
        }
    )+};
}

impl_ref_ops! { Add::add, Sub::sub, Mul::mul }

impl fmt::Display for FixedPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO
        fmt::Display::fmt(&(self.integer as f64 + self.fractional as f64 / FRACTIONAL_PART_DENOMINATOR as f64), f)
    }
}

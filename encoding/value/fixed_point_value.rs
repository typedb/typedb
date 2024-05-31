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
            (frac, true) if frac < FRACTIONAL_PART_DENOMINATOR => {
                (frac + 0u64.wrapping_sub(FRACTIONAL_PART_DENOMINATOR), 1)
            }
            (frac, false) => (frac - FRACTIONAL_PART_DENOMINATOR, 1),
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

#[cfg(test)]
mod tests {
    use std::ops::{RangeBounds, RangeInclusive};

    use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};

    use super::{FixedPoint, FRACTIONAL_PART_DENOMINATOR};

    fn random_fixed_point(rng: &mut impl Rng) -> FixedPoint {
        FixedPoint { integer: rng.gen(), fractional: rng.gen_range(0..FRACTIONAL_PART_DENOMINATOR) }
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn fractional_part_overflow_is_handled_correctly() {
        let sub_one = 1 - FixedPoint::new(0, 1);
        assert_eq!(sub_one, FixedPoint::new(0, FRACTIONAL_PART_DENOMINATOR - 1));
        assert_eq!(sub_one + sub_one, 2 - FixedPoint::new(0, 2));

        assert!(FRACTIONAL_PART_DENOMINATOR > u64::MAX / 2);

        let u64_max_div_denom =
            FixedPoint::new((u64::MAX / FRACTIONAL_PART_DENOMINATOR) as i64, u64::MAX % FRACTIONAL_PART_DENOMINATOR);
        assert_eq!(
            FixedPoint::new(0, FRACTIONAL_PART_DENOMINATOR - 1)
                + FixedPoint::new(0, 0u64.wrapping_sub(FRACTIONAL_PART_DENOMINATOR)),
            u64_max_div_denom
        );

        assert_eq!(sub_one * sub_one, 1 - FixedPoint::new(0, 2)); // rounded to nearest
    }

    #[test]
    fn randomized_tests() {
        const fn as_i128(lhs: FixedPoint) -> i128 {
            lhs.integer as i128 * FRACTIONAL_PART_DENOMINATOR as i128 + lhs.fractional as i128
        }

        let seed = thread_rng().gen();
        let mut rng = SmallRng::seed_from_u64(seed);
        eprintln!("Running with seed: {seed}");

        let range = as_i128(FixedPoint::MIN)..=as_i128(FixedPoint::MAX);
        for _ in 0..1_000_000 {
            let lhs = random_fixed_point(&mut rng);
            let rhs = random_fixed_point(&mut rng);

            if as_i128(lhs).checked_add(as_i128(rhs)).is_some_and(|res| range.contains(&res)) {
                assert_eq!(as_i128(lhs + rhs), as_i128(lhs) + as_i128(rhs));
            }
            if as_i128(lhs).checked_sub(as_i128(rhs)).is_some_and(|res| range.contains(&res)) {
                assert_eq!(as_i128(lhs - rhs), as_i128(lhs) - as_i128(rhs));
            }
            if as_i128(lhs).checked_mul(rhs.integer as i128).is_some_and(|res| range.contains(&res))
                && as_i128(lhs).checked_mul(rhs.integer as i128 + 1).is_some_and(|res| range.contains(&res))
            {
                assert_eq!(as_i128(lhs - rhs), as_i128(lhs) - as_i128(rhs));
            }
        }
    }
}

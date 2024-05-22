/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// https://github.com/rust-lang/rust/issues/49601 workaround
pub trait FnMutHktHelper<T, U>: FnMut(T) -> U {}
impl<F, T, U> FnMutHktHelper<T, U> for F where F: FnMut(T) -> U {}

pub trait Hkt {
    type HktSelf<'a>;
}

impl<'s, T: ?Sized + 'static> Hkt for &'s T {
    type HktSelf<'a> = &'a T;
}

impl<T: Hkt, U: Hkt> Hkt for (T, U) {
    type HktSelf<'a> = (T::HktSelf<'a>, U::HktSelf<'a>);
}

impl Hkt for String {
    type HktSelf<'a> = Self;
}

impl<T: Hkt> Hkt for Option<T> {
    type HktSelf<'a> = Option<T::HktSelf<'a>>;
}

impl<T: Hkt, E: 'static> Hkt for Result<T, E> {
    type HktSelf<'a> = Result<T::HktSelf<'a>, E>;
}



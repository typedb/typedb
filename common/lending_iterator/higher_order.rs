/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

// https://github.com/rust-lang/rust/issues/49601 workaround
pub trait FnMutHktHelper<T, U>: FnMut(T) -> U + 'static {}
impl<F, T, U> FnMutHktHelper<T, U> for F where F: FnMut(T) -> U + 'static + ?Sized {}

pub trait FnHktHelper<T, U>: Fn(T) -> U + 'static {}
impl<F, T, U> FnHktHelper<T, U> for F where F: Fn(T) -> U + 'static + ?Sized {}

pub trait Hkt: 'static {
    type HktSelf<'a>;
}

impl<T: ?Sized> Hkt for &'static T {
    type HktSelf<'a> = &'a T;
}

impl<T: Hkt, U: Hkt> Hkt for (T, U) {
    type HktSelf<'a> = (T::HktSelf<'a>, U::HktSelf<'a>);
}

impl<T: Hkt, U: Hkt, V: Hkt> Hkt for (T, U, V) {
    type HktSelf<'a> = (T::HktSelf<'a>, U::HktSelf<'a>, V::HktSelf<'a>);
}

macro_rules! trivial_hkt {
    ($($ty:ty),+ $(,)?) => {
        $(impl Hkt for $ty { type HktSelf<'a> = $ty; })+
    };
}

trivial_hkt! {
    (),
    bool,
    u8, u16, u32, u64, usize,
    i8, i16, i32, i64, isize,
    String,
}

impl<T: Hkt> Hkt for Option<T> {
    type HktSelf<'a> = Option<T::HktSelf<'a>>;
}

impl<T: Hkt, E: 'static> Hkt for Result<T, E> {
    type HktSelf<'a> = Result<T::HktSelf<'a>, E>;
}

pub struct AdHocHkt<B: 'static> {
    _pd: PhantomData<B>,
}

impl<B> Hkt for AdHocHkt<B> {
    type HktSelf<'a> = B;
}

#[macro_export]
macro_rules! AsHkt {
    {$($path_element:ident)::+ <'_ $(, $generic_param:tt)*> } => { $($path_element)::+<'static $(, $generic_param)*> };
    {<$ty:ty as $trait:ty>::$($path_element:ident)::+ <'_ $(, $generic_param:tt)*> } => {
        <$ty as $trait>::$($path_element)::+<'static $(, $generic_param)*>
    };
    {& mut $ty:ty  } => {&'static mut $ty};
    {& $ty:ty  } => {&'static $ty};
}

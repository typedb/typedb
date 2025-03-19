/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[macro_export]
macro_rules! deref_for_trivial_struct {
    ($outer:ty => $inner:ty) => {
        impl std::ops::Deref for $outer {
            type Target = $inner;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

#[macro_export]
macro_rules! impl_from_for_enum {
    // Nothing, we just need the compile error when the feature is deleted
    ($enum_name:ident from $inner:ident as $variant:ident) => {
        impl From<$inner> for $enum_name {
            fn from(value: $inner) -> Self {
                $enum_name::$variant(value)
            }
        }
    };
    ($enum_name:ident from $inner_and_variant:ident) => {
        impl_from_for_enum!($enum_name from $inner_and_variant as $inner_and_variant);
    };
}

#[macro_export]
macro_rules! enum_dispatch {
    ( $block:block for $inner:ident in [ $($variant:ident)* ] on $match_on:ident) => {
        match $match_on {
            $(Self::$variant($inner) => $block) *,
        }
    }
}

#[macro_export]
macro_rules! enum_dispatch_method {
    ($vis:vis fn $method_name:ident(&mut self $(, $arg:ident : $argtype:ty)* ) -> $ret:ty [ $($variant:ident)* ];) => {
        $vis fn $method_name(&mut self$(, $arg : $argtype)* ) -> $ret {
            utils::enum_dispatch!( {inner.$method_name($($arg,)*) } for inner in [ $($variant)* ] on self)
        }
    };

    ( $vis:vis fn $method_name:ident(&self $(, $arg:ident : $argtype:ty)* ) -> $ret:ty [ $($variant:ident)* ]; ) => {
        $vis fn $method_name(&self$(, $arg : $argtype)* ) -> $ret {
            utils::enum_dispatch!( {inner.$method_name($($arg,)*) } for inner in [ $($variant)* ] on self)
        }
    };

    ( $vis:vis fn $method_name:ident(self $(, $arg:ident : $argtype:ty)* ) -> $ret:ty [ $($variant:ident)* ]; ) => {
        $vis fn $method_name(self$(, $arg : $argtype)* ) -> $ret {
            utils::enum_dispatch!( {inner.$method_name($($arg,)*) } for inner in [ $($variant)* ] on self)
        }
    };

    ( foreach $variants:tt [ $($vis:vis fn $method_name:ident $args:tt -> $ret:ty;)* ] ) => {
        $( utils::enum_dispatch_method!{
            $vis fn $method_name $args -> $ret $variants;
        })*
    }
}

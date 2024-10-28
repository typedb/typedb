/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[macro_export]
macro_rules! enum_dispatch {
    ( $var:ident in [$($variant:path)*] as $inner:ident => $action:block ) => {
        match $var {
            $(
                $variant($inner) => $action,
            )+
        }
    };
}

// Can be specialised to an enum specific macro
// macro_rules! my_enum_dispatch {
//     ($var:ident as $inner:ident => $action:block) => {
//         enum_dispatch!( $var in [MyEnum::VariantA MyEnum::VariantB] as $inner => $action)
//     }
// }
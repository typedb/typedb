/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

macro_rules! assert_matches {
    ($expression:expr, $pattern:pat $(if $guard:expr)? $(,)?) => {
        {
            match $expression {
                $pattern $(if $guard)? => (),
                expr => panic!(
                    concat!(
                        "assertion `matches!(expression, ",
                        stringify!($pattern $(if $guard)?),
                        ")` failed\n",
                        "expression evaluated to: {:?}"
                    ),
                    expr
                )
            }
        }
    };
}
pub(crate) use assert_matches;

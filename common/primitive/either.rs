/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// TODO: Why don't we use itertools::Either?
#[derive(Debug)]
pub enum Either<F, S> {
    First(F),
    Second(S),
}

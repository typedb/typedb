/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::pattern::pattern::Pattern;

pub struct FunctionIR {
    pattern: Pattern,

    // filter, offset, limit, sort [modifiers list]
}

impl FunctionIR {
    fn new(pattern: Pattern) -> Self {
        Self { pattern }
    }
}

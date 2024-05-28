/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use crate::pattern::pattern::Pattern;

use crate::program::function::Function;

struct Program {
    entry: Pattern,
    // modifiers: list of modifiers conceptually applied after the program executes
    functions: HashMap<u64, Function>, // TODO: use function ID
}

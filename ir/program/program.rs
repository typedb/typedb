/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use encoding::graph::definition::definition_key::DefinitionKey;
use crate::pattern::pattern::Pattern;
use crate::program::function::FunctionIR;

struct Program {
    entry: Pattern,
    // modifiers: list of modifiers conceptually applied after the program executes
    // also: reduce operation if needed
    functions: HashMap<DefinitionKey<'static>, FunctionIR>,
}

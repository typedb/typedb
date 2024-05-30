/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::definition::definition_key::DefinitionKey;
use ir::program::function::{FunctionIR, FunctionValuePrototype};

/// Function represents the user-defined structure:
/// fun <name>(<args>) -> <return type> { <body> }
pub struct Function {
    definition_key: DefinitionKey<'static>,

    // parsed representation
    name: String,
    arguments: Vec<FunctionArgument>,
    return_type: FunctionReturn,

    // pre-compiled arguments, body, return
    ir_body: FunctionIR,
}

struct FunctionArgument {
    name: String,
    type_: FunctionValuePrototype,
}

enum FunctionReturn {
    Stream(Vec<FunctionValuePrototype>),
    Single(FunctionValuePrototype),
}

impl Function {

    // TODO: receive a string, which can either come from the User or from Storage (deserialised)
    fn new(definition_key: DefinitionKey, definition: &str) -> Self {
        // 1. parse into TypeQL
        // 2. extract into data structures
        // 3. create IR & apply inference
        todo!()
    }
}

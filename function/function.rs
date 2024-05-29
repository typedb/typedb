/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValuePrototype;
use encoding::graph::definition::definition::DefinitionKey;
use ir::program::function::FunctionIR;

/// Function represents the user-defined structure:
/// fun <name>(<args>) -> <return type> { <body> }
struct Function {
    definition_key: DefinitionKey,

    // parsed representation
    name: String,
    arguments: Vec<FunctionArgument>,
    return_type: FunctionReturn,

    // pre-compiled body
    body_ir: FunctionIR,
}

struct FunctionArgument {
    name: String,
    type_: VariableValuePrototype,
}

enum FunctionReturn {
    Stream(Vec<VariableValuePrototype>),
    Single(VariableValuePrototype),
}


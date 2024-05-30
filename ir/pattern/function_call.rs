/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::pattern::variable::Variable;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FunctionCall {
    function_id: DefinitionKey<'static>,
    call_variable_mapping: HashMap<Variable, Variable>,
    call_variable_prototypes: HashMap<Variable, VariableValuePrototype>
}

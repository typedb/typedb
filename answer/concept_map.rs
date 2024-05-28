/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use ir::pattern::variable::Variable;

use crate::variable_value::{VariableValue, VariableValuePrototype};

// TODO: we could actually optimise this data structure into an array given the prototype + a index mapping
pub struct ConceptMap<'a> {
    map: HashMap<Variable, VariableValue<'a>>,
}

pub struct ConceptMapPrototype {
    map: HashMap<Variable, VariableValuePrototype>,
}

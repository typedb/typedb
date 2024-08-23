/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{hash_map, HashMap};

use crate::{variable::Variable, variable_value::VariableValue};

// TODO: we could actually optimise this data structure into an array given the prototype + a index mapping
// TODO: what about optionality and lists
pub struct AnswerMap<'a> {
    map: HashMap<Variable, VariableValue<'a>>,
}

impl<'a> IntoIterator for AnswerMap<'a> {
    type Item = (Variable, VariableValue<'a>);
    type IntoIter = hash_map::IntoIter<Variable, VariableValue<'a>>;
    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl<'a, 'b> IntoIterator for &'b AnswerMap<'a> {
    type Item = (&'b Variable, &'b VariableValue<'a>);
    type IntoIter = hash_map::Iter<'b, Variable, VariableValue<'a>>;
    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

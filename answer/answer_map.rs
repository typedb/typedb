/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use ir::pattern::variable::Variable;

use crate::{Concept};

// TODO: we could actually optimise this data structure into an array given the prototype + a index mapping
// TODO: what about optionality and lists
pub struct AnswerMap<'a> {
    map: HashMap<Variable, Concept<'a>>,
}

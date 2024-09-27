/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;

use crate::program::function::Reducer;

#[derive(Debug, Clone)]
pub struct Reduce {
    pub assigned_reductions: Vec<(Variable, Reducer)>,
    pub within_group: Vec<Variable>,
}

impl Reduce {
    pub(crate) fn new(assigned_reductions: Vec<(Variable, Reducer)>, within_group: Vec<Variable>) -> Self {
        Self { assigned_reductions, within_group }
    }
}

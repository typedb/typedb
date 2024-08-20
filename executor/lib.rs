/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use answer::variable::Variable;
use compiler::VariablePosition;
use ir::pattern::IrID;

mod accumulator;
pub mod batch;
pub mod expression_executor;
mod function_executor;
pub(crate) mod instruction;
pub mod pattern_executor;
pub mod pipeline;
pub mod program_executor;
pub mod write;

// TODO: use a bit-vec, since we have a continuously allocated range of positions
// ---> for now, using a byte vec, which is 8x wasteful and on the heap!
pub(crate) struct SelectedPositions {
    selected: Vec<VariablePosition>,
}

impl SelectedPositions {
    fn new(selected_variables: &[Variable], variable_positions: &HashMap<Variable, VariablePosition>) -> Self {
        Self { selected: selected_variables.iter().map(|var| variable_positions[var]).collect() }
    }

    fn iter_selected(&self) -> impl Iterator<Item = VariablePosition> + '_ {
        self.selected.iter().copied()
    }
}

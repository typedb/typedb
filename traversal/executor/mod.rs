/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use answer::variable::Variable;

use ir::pattern::IrID;

mod function_executor;
mod instruction;
pub mod pattern_executor;
pub mod program_executor;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Position {
    position: u32,
}

impl Position {
    pub(crate) fn new(position: u32) -> Self {
        Position { position }
    }

    pub fn as_usize(&self) -> usize {
        self.position as usize
    }
}

// TODO: use a bit-vec, since we have a continuously allocated range of positions
// ---> for now, using a byte vec, which is 8x wasteful and on the heap!
pub(crate) struct SelectedPositions {
    selected: Vec<Position>
}

impl SelectedPositions {
    fn new(selected_variables: &Vec<Variable>, variable_positions: &HashMap<Variable, Position>) -> Self {
        Self {
            selected: selected_variables.iter().map(|pos| variable_positions[pos]).collect()
        }
    }

    fn iter_selected(&self) -> impl Iterator<Item=Position> + '_ {
        self.selected.iter().copied()
    }
}

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "P_{}", self.position)
    }
}

impl IrID for Position {}


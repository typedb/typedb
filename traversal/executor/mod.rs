/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};

use ir::pattern::IrID;

mod function_executor;
mod iterator;
mod pattern_executor;
pub mod program_executor;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct Position {
    position: u32,
}

impl Position {
    pub(crate) fn new(position: u32) -> Self {
        Position { position }
    }

    pub(crate) fn as_usize(&self) -> usize {
        self.position as usize
    }
}

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "P_{}", self.position)
    }
}

impl IrID for Position {}

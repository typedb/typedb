/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::slice;

use compiler::VariablePosition;

pub mod batch;
pub mod expression_executor;
mod function_executor;
pub(crate) mod instruction;
pub mod pattern_executor;
pub mod pipeline;
pub mod program_executor;
pub mod row;
pub mod write;

// TODO: use a bit-vec, since we have a continuously allocated range of positions
// ---> for now, using a byte vec, which is 8x wasteful and on the heap!
pub(crate) struct SelectedPositions {
    selected: Vec<VariablePosition>,
}

impl SelectedPositions {
    fn new(selected: Vec<VariablePosition>) -> Self {
        Self { selected }
    }
}

impl<'a> IntoIterator for &'a SelectedPositions {
    type Item = &'a VariablePosition;

    type IntoIter = slice::Iter<'a, VariablePosition>;

    fn into_iter(self) -> Self::IntoIter {
        self.selected.iter()
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::Has;

use crate::executor::{instruction::iterator::InstructionIterator, pattern_executor::Row, Position};

pub(crate) struct HasReverseIteratorExecutor {
    has: Has<Position>,
}

impl HasReverseIteratorExecutor {
    pub(crate) fn new(has: Has<Position>) -> HasReverseIteratorExecutor {
        Self { has }
    }
}

impl HasReverseIteratorExecutor {
    pub(crate) fn get_iterator(&self, row: &Row) -> InstructionIterator {
        todo!()
    }
}

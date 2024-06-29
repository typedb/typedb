/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use ir::pattern::constraint::Has;
use crate::executor::iterator::ConstraintIterator;
use crate::executor::pattern_executor::Row;
use crate::executor::Position;
use crate::planner::pattern_plan::SortedIterateMode;

pub(crate) struct HasReverseProvider {
    has: Has<Position>,
    iterate_mode: SortedIterateMode,
}

impl HasReverseProvider {
    pub(crate) fn new(has: Has<Position>, iterate_mode: SortedIterateMode) -> HasReverseProvider {
        Self { has, iterate_mode }
    }
}

impl HasReverseProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

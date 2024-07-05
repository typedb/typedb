/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::Has;

use crate::{
    executor::{iterator::ConstraintIterator, pattern_executor::Row, Position},
    planner::pattern_plan::IterateMode,
};

pub(crate) struct HasReverseProvider {
    has: Has<Position>,
    iterate_mode: IterateMode,
}

impl HasReverseProvider {
    pub(crate) fn new(has: Has<Position>, iterate_mode: IterateMode) -> HasReverseProvider {
        Self { has, iterate_mode }
    }
}

impl HasReverseProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

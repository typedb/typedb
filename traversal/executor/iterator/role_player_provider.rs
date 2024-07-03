/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::RolePlayer;

use crate::{
    executor::{iterator::ConstraintIterator, pattern_executor::Row, Position},
    planner::pattern_plan::IterateMode,
};

pub(crate) struct RolePlayerProvider {
    role_player: RolePlayer<Position>,
    iterate_mode: IterateMode,
}

impl RolePlayerProvider {
    pub(crate) fn new(role_player: RolePlayer<Position>, iterate_mode: IterateMode) -> RolePlayerProvider {
        Self { role_player, iterate_mode }
    }
}

impl RolePlayerProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

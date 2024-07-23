/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::RolePlayer;

use crate::executor::VariablePosition;

pub(crate) struct RolePlayerIteratorExecutor {
    role_player: RolePlayer<VariablePosition>,
}

impl RolePlayerIteratorExecutor {
    pub(crate) fn new(role_player: RolePlayer<VariablePosition>) -> RolePlayerIteratorExecutor {
        Self { role_player }
    }
}

impl RolePlayerIteratorExecutor {}

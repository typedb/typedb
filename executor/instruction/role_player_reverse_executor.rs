/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::RolePlayer;

use crate::VariablePosition;

pub(crate) struct RolePlayerReverseExecutor {
    role_player: RolePlayer<VariablePosition>,
}

impl RolePlayerReverseExecutor {
    pub(crate) fn new(role_player: RolePlayer<VariablePosition>) -> RolePlayerReverseExecutor {
        Self { role_player }
    }
}

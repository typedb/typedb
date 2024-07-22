/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::constraint::RolePlayer;

use crate::executor::{batch::Row, Position};

pub(crate) struct RolePlayerReverseIteratorExecutor {
    role_player: RolePlayer<Position>,
}

impl RolePlayerReverseIteratorExecutor {
    pub(crate) fn new(role_player: RolePlayer<Position>) -> RolePlayerReverseIteratorExecutor {
        Self { role_player }
    }
}


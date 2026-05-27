/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub mod server;
pub mod users;

use crate::command::CommandRegistry;

pub fn base_commands() -> CommandRegistry {
    let registry = CommandRegistry::new();
    let registry = server::register(registry);
    users::register(registry)
}

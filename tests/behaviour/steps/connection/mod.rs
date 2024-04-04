/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use macro_rules_attribute::apply;
use server::typedb;
use test_utils::create_tmp_dir;

use crate::{generic_step, Context};

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    let server_dir = create_tmp_dir();
    context.server = Some(typedb::Server::recover(&server_dir).unwrap());
    context.server_dir = Some(server_dir);
}

#[apply(generic_step)]
#[step("connection opens with default authentication")]
#[step("connection has been opened")]
pub async fn connection_ignore(_: &mut Context) {}

#[apply(generic_step)]
#[step("connection does not have any database")]
pub async fn connection_does_not_have_any_database(context: &mut Context) {
    assert!(context.server.as_ref().unwrap().databases().is_empty())
}

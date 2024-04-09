/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use macro_rules_attribute::apply;
use server::typedb;
use test_utils::create_tmp_dir;

use crate::{generic_step, Context};

mod database;

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

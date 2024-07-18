/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cell::OnceCell;
use std::sync::Arc;
use macro_rules_attribute::apply;
use server::typedb;
use test_utils::{create_tmp_dir, TempDir};

use crate::{generic_step, Context};

mod database;
mod transaction;

static TYPEDB: OnceCell<(TempDir, Arc<typedb::Server>)> = OnceCell::new();

#[apply(generic_step)]
#[step("typedb starts")]
pub async fn typedb_starts(context: &mut Context) {
    let (_, server) = TYPEDB.get_or_init(|| {
        let server_dir = create_tmp_dir();
        let server = typedb::Server::open(&server_dir).unwrap();
        (server_dir, Arc::new(server))
    });
    context.server = Some(server.clone());
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

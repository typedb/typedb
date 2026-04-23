/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Postgres wire protocol server integration.
//!
//! Bridges the `projection` crate's pgwire listener into the TypeDB
//! server by implementing [`PgAuthenticator`] and [`QueryHandler`]
//! against `ServerState` and a `MaterializedCatalog`.

use std::sync::Arc;

use projection::{
    catalog::MaterializedCatalog,
    pgwire::{
        authenticator::PgAuthenticator,
        connection::{QueryHandler, QueryOutcome, SessionContext},
        query_executor::{execute_raw_query_batch_with_session, execute_raw_query_with_session},
    },
};

use crate::state::BoxServerState;

// ── ServerStateAuthenticator ───────────────────────────────────────

/// Bridges [`PgAuthenticator`] to [`ServerState::user_verify_password`].
#[derive(Debug)]
pub struct ServerStateAuthenticator {
    server_state: Arc<BoxServerState>,
}

impl ServerStateAuthenticator {
    pub fn new(server_state: Arc<BoxServerState>) -> Self {
        Self { server_state }
    }
}

impl PgAuthenticator for ServerStateAuthenticator {
    fn verify_password(&self, username: &str, password: &str) -> Result<(), String> {
        self.server_state.user_verify_password(username, password).map_err(|e| format!("{e:?}"))
    }
}

// ── CatalogQueryHandler ────────────────────────────────────────────

/// Bridges [`QueryHandler`] to [`MaterializedCatalog`] via the SQL
/// parser and query executor from the `projection` crate.
#[derive(Debug)]
pub struct CatalogQueryHandler {
    catalog: MaterializedCatalog,
}

impl CatalogQueryHandler {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl QueryHandler for CatalogQueryHandler {
    fn handle_query(&self, sql: &str) -> QueryOutcome {
        execute_raw_query_with_session(&self.catalog, sql, &SessionContext::default())
    }

    fn handle_query_with_session(&self, sql: &str, session: &SessionContext) -> QueryOutcome {
        execute_raw_query_with_session(&self.catalog, sql, session)
    }

    fn handle_query_batch_with_session(&self, sql: &str, session: &SessionContext) -> Vec<QueryOutcome> {
        execute_raw_query_batch_with_session(&self.catalog, sql, session)
    }
}

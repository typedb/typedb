/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, fmt::Formatter};

use storage::durability_client::WALClient;

use crate::transaction::TransactionRead;

pub mod database_importer;

#[derive(Debug, PartialEq, Eq)]
pub struct Checksums {
    pub entity_count: i64,
    pub attribute_count: i64,
    pub relation_count: i64,
    pub role_count: i64,
    pub ownership_count: i64,
}

impl Checksums {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Checksums {
    fn default() -> Self {
        Self { entity_count: 0, attribute_count: 0, relation_count: 0, role_count: 0, ownership_count: 0 }
    }
}

impl fmt::Display for Checksums {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let Self { entity_count, attribute_count, relation_count, role_count, ownership_count } = self;
        write!(f, "[{entity_count} entities, {attribute_count} attributes, {relation_count} relations, {role_count} roles, {ownership_count} ownerships]")
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;

pub(crate) mod export_service;
pub(crate) mod import_service;
pub(crate) mod item;

#[derive(Debug, PartialEq, Eq)]
struct Checksums {
    entity_count: i64,
    attribute_count: i64,
    relation_count: i64,
    role_count: i64,
    ownership_count: i64,
}

impl Checksums {
    fn new() -> Self {
        Self::default()
    }
}

impl Default for Checksums {
    fn default() -> Self {
        Self { entity_count: 0, attribute_count: 0, relation_count: 0, role_count: 0, ownership_count: 0 }
    }
}

impl fmt::Display for Checksums {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { entity_count, attribute_count, relation_count, role_count, ownership_count } = self;
        write!(f, "[{entity_count} entities, {attribute_count} attributes, {relation_count} relations, {role_count} roles, {ownership_count} ownerships]")
    }
}

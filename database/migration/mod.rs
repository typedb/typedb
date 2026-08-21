/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fmt, fmt::Formatter};

use concept::error::ConceptReadError;
use error::typedb_error;
use ir::pipeline::FunctionReadError;
use storage::durability_client::DurabilityClient;

use crate::transaction::TransactionRead;

pub mod database_exporter;
pub mod database_import_handler;
pub mod database_importer;
pub mod item;

#[derive(Debug, Clone, PartialEq, Eq)]
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
        write!(
            f,
            "[{entity_count} entities, {attribute_count} attributes, {relation_count} relations, {role_count} roles, {ownership_count} ownerships]"
        )
    }
}

pub fn transaction_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, MigrationExportError> {
    let types_syntax = transaction_types_syntax(transaction)?;
    let functions_syntax = transaction_functions_syntax(transaction)?;
    Ok(format!("{}\n{}{}\n", typeql::token::Clause::Define, types_syntax, functions_syntax).trim().to_owned())
}

pub fn transaction_type_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, MigrationExportError> {
    let types_syntax = transaction_types_syntax(transaction)?;
    Ok(format!("{}\n{}\n", typeql::token::Clause::Define, types_syntax).trim().to_owned())
}

fn transaction_types_syntax<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, MigrationExportError> {
    transaction
        .type_manager
        .get_types_syntax(transaction.snapshot())
        .map_err(|typedb_source| MigrationExportError::ConceptRead { typedb_source })
}

fn transaction_functions_syntax<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, MigrationExportError> {
    transaction
        .function_manager
        .get_functions_syntax(transaction.snapshot())
        .map_err(|typedb_source| MigrationExportError::FunctionRead { typedb_source })
}

typedb_error! {
    pub MigrationExportError(component = "Migration export", prefix = "MEX") {
        ConceptRead(1, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        FunctionRead(2, "Error reading functions.", typedb_source: FunctionReadError),
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{transaction::TransactionRead, Database};
use options::TransactionOptions;
use storage::durability_client::DurabilityClient;

use crate::service::ServiceError;

pub(crate) fn get_database_schema<D: DurabilityClient>(database: Arc<Database<D>>) -> Result<String, ServiceError> {
    let transaction = TransactionRead::open(database, TransactionOptions::default())
        .map_err(|err| ServiceError::FailedToOpenPrerequisiteTransaction {})?;
    let types_syntax = get_types_syntax(&transaction)?;
    let functions_syntax = get_functions_syntax(&transaction)?;

    let schema = match types_syntax.is_empty() & functions_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{} {}", typeql::token::Clause::Define, types_syntax, functions_syntax),
    };
    Ok(schema)
}

pub(crate) fn get_database_type_schema<D: DurabilityClient>(
    database: Arc<Database<D>>,
) -> Result<String, ServiceError> {
    let transaction = TransactionRead::open(database, TransactionOptions::default())
        .map_err(|err| ServiceError::FailedToOpenPrerequisiteTransaction {})?;
    let types_syntax = get_types_syntax(&transaction)?;

    let type_schema = match types_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{}", typeql::token::Clause::Define, types_syntax),
    };
    Ok(type_schema)
}

fn get_types_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, ServiceError> {
    transaction
        .type_manager
        .get_types_syntax(transaction.snapshot())
        .map_err(|err| ServiceError::ConceptReadError { typedb_source: err })
}

fn get_functions_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, ServiceError> {
    transaction
        .function_manager
        .get_functions_syntax(transaction.snapshot())
        .map_err(|err| ServiceError::FunctionReadError { typedb_source: err })
}

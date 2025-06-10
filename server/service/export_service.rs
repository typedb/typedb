/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use concept::error::ConceptReadError;
use database::transaction::{TransactionError, TransactionRead};
use error::typedb_error;
use ir::pipeline::FunctionReadError;
use storage::durability_client::DurabilityClient;

pub(crate) fn get_transaction_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, DatabaseExportError> {
    let types_syntax = get_types_syntax(transaction)?;
    let functions_syntax = get_functions_syntax(transaction)?;

    let schema = match types_syntax.is_empty() & functions_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{}{}\n", typeql::token::Clause::Define, types_syntax, functions_syntax),
    };
    Ok(schema)
}

pub(crate) fn get_transaction_type_schema<D: DurabilityClient>(
    transaction: &TransactionRead<D>,
) -> Result<String, DatabaseExportError> {
    let types_syntax = get_types_syntax(transaction)?;

    let type_schema = match types_syntax.is_empty() {
        true => String::new(),
        false => format!("{}\n{}\n", typeql::token::Clause::Define, types_syntax),
    };
    Ok(type_schema)
}

fn get_types_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, DatabaseExportError> {
    transaction
        .type_manager
        .get_types_syntax(transaction.snapshot())
        .map_err(|err| DatabaseExportError::ConceptRead { typedb_source: err })
}

fn get_functions_syntax<D: DurabilityClient>(transaction: &TransactionRead<D>) -> Result<String, DatabaseExportError> {
    transaction
        .function_manager
        .get_functions_syntax(transaction.snapshot())
        .map_err(|err| DatabaseExportError::FunctionRead { typedb_source: err })
}

typedb_error! {
    pub(crate) DatabaseExportError(component = "Database export", prefix = "DBE") {
        TransactionFailed(1, "Transaction failed.", typedb_source: TransactionError),
        ConceptRead(2, "Error reading concepts.", typedb_source: Box<ConceptReadError>),
        FunctionRead(3, "Error reading functions.", typedb_source: FunctionReadError),
    }
}

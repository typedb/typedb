/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
pub use grpc::{IntoGrpcStatus, IntoProtocolErrorMessage};
use serde::{Deserialize, Serialize};

pub(crate) mod export_service;
pub(crate) mod grpc;
pub mod http;
mod import_service;
mod transaction_service;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum TransactionType {
    Read,
    Write,
    Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum QueryType {
    Read,
    Write,
    Schema,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum AnswerType {
    Ok,
    ConceptRows,
    ConceptDocuments,
}

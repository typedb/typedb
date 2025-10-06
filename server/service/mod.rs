/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::query_structure::{PipelineStructure, QueryStructureConjunctionID};
use concept::error::ConceptReadError;
use options::QueryOptions;
use serde::{Deserialize, Serialize};

pub(crate) mod export_service;
pub(crate) mod grpc;
pub mod http;
mod import_service;
mod transaction_service;
pub mod typedb_service;

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

pub(crate) enum IncludeInvolvedBlocks {
    True { always_involved: Vec<QueryStructureConjunctionID> },
    False,
}

pub(crate) fn may_encode_pipeline_structure<T>(
    options: &QueryOptions,
    pipeline: Option<&PipelineStructure>,
    encoder: impl Fn(&PipelineStructure) -> Result<T, Box<ConceptReadError>>,
) -> Result<(Option<T>, IncludeInvolvedBlocks), Box<ConceptReadError>> {
    match (&options.include_query_structure, pipeline) {
        (false, _) | (true, None) => Ok((None, IncludeInvolvedBlocks::False)),
        (true, Some(structure)) => {
            let include_involved_blocks = IncludeInvolvedBlocks::True {
                always_involved: structure.parametrised_structure.always_involved_blocks(),
            };
            encoder(structure).map(|encoded| (Some(encoded), include_involved_blocks))
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use compiler::query_structure::{PipelineStructure, QueryStructureConjunctionID};
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

pub(crate) enum IncludeInvolvedBlocks {
    True { always_involved: Vec<QueryStructureConjunctionID> },
    False,
}

impl IncludeInvolvedBlocks {
    pub(crate) fn build(pipeline_structure: Option<&PipelineStructure>) -> Self {
        match pipeline_structure {
            None => IncludeInvolvedBlocks::False,
            Some(structure) => {
                let always_involved = structure.parametrised_structure.always_involved_blocks();
                IncludeInvolvedBlocks::True { always_involved }
            }
        }
    }
}

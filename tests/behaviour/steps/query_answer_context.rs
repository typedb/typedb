/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable_value::VariableValue;
use executor::document::ConceptDocument;

#[derive(Debug, Clone)]
pub enum QueryAnswer {
    ConceptRows(Vec<HashMap<String, VariableValue<'static>>>),
    ConceptDocuments(Vec<ConceptDocument>),
}

impl QueryAnswer {
    pub fn len(&self) -> usize {
        match self {
            QueryAnswer::ConceptRows(rows) => rows.len(),
            QueryAnswer::ConceptDocuments(documents) => documents.len(),
        }
    }
}

macro_rules! with_rows_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        match $context.query_answer.as_ref().unwrap() {
            $crate::query_answer_context::QueryAnswer::ConceptRows($answer) => $expr,
            $crate::query_answer_context::QueryAnswer::ConceptDocuments($answer) => {
                panic!("Expected ConceptRows, got ConceptDocuments")
            }
        }
    };
}
pub(crate) use with_rows_answer;

macro_rules! with_documents_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        match $context.query_answer.as_ref().unwrap() {
            $crate::query_answer_context::QueryAnswer::ConceptRows($answer) => {
                panic!("Expected ConceptDocuments, got ConceptRows")
            }
            $crate::query_answer_context::QueryAnswer::ConceptDocuments($answer) => $expr,
        }
    };
}
pub(crate) use with_documents_answer;

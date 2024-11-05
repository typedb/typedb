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

macro_rules! with_rows_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_rows();
        $expr
    };
}
pub(crate) use with_rows_answer;

macro_rules! with_documents_answer {
    ($context:ident, |$answer:ident| $expr:expr) => {
        let $answer = $context.query_answer.as_ref().unwrap().as_documents();
        $expr
    };
}
pub(crate) use with_documents_answer;

impl QueryAnswer {
    pub fn as_rows(&self) -> &Vec<HashMap<String, VariableValue<'static>>> {
        match self {
            Self::ConceptRows(rows) => rows,
            Self::ConceptDocuments(_) => panic!("Expected ConceptRows, got ConceptDocuments"),
        }
    }

    pub fn as_documents(&self) -> &Vec<ConceptDocument> {
        match self {
            Self::ConceptRows(_) => {
                panic!("Expected ConceptDocuments, got ConceptRows")
            }
            Self::ConceptDocuments(documents) => documents,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            QueryAnswer::ConceptRows(rows) => rows.len(),
            QueryAnswer::ConceptDocuments(documents) => documents.len(),
        }
    }

    pub fn as_documents_json(&self) -> Vec<String> {
        let documents = self.as_documents();
        vec![]
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;

use compiler::VariablePosition;
use executor::{batch::Batch, document::ConceptDocument, pipeline::PipelineExecutionError, row::MaybeOwnedRow};
use lending_iterator::LendingIterator;

use crate::AnswerConsumer;

pub type PackedResult<OK, ERR, T> = Result<(OK, T), (ERR, T)>;

pub fn pack_result<OK, ERR, T>(result: Result<OK, ERR>, t: T) -> Result<(OK, T), (ERR, T)> {
    match result {
        Ok(ok) => Ok((ok, t)),
        Err(err) => Err((err, t)),
    }
}

pub fn unpack_result<OK, ERR, T>(result: Result<(OK, T), (ERR, T)>) -> (Result<OK, ERR>, T) {
    match result {
        Ok((ok, t)) => (Ok(ok), t),
        Err((err, t)) => (Err(err), t),
    }
}

pub enum CollectedAnswer {
    Rows { descriptor: HashMap<String, VariablePosition>, rows: Batch },
    Documents { documents: Vec<ConceptDocument> },
}

// ANSWER CONSUMER

pub struct CountResults;
impl AnswerConsumer for CountResults {
    type Output = usize;
    fn consume_rows<Iter>(iter: &mut Iter) -> Result<Self::Output, Box<PipelineExecutionError>>
    where
        for<'a> Iter: LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>>,
    {
        let mut count: usize = 0;
        while let Some(row) = iter.next() {
            if let Err(err) = row {
                return Err(err);
            }
            count += 1;
        }
        Ok(count)
    }

    fn consume_docs(
        iter: &mut impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>,
    ) -> Result<Self::Output, Box<PipelineExecutionError>> {
        let mut count: usize = 0;
        while let Some(row) = iter.next() {
            if let Err(err) = row {
                return Err(err);
            }
            count += 1;
        }
        Ok(count)
    }
}

/// Warning: doesn't execute the last stages
pub struct IgnoreRowsForWriteQuery;
impl AnswerConsumer for IgnoreRowsForWriteQuery {
    type Output = ();
    fn consume_rows<Iter>(_iter: &mut Iter) -> Result<Self::Output, Box<PipelineExecutionError>>
    where
        for<'a> Iter: LendingIterator<Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>>,
    {
        Ok(())
    }

    fn consume_docs(
        _iter: &mut impl Iterator<Item = Result<ConceptDocument, Box<PipelineExecutionError>>>,
    ) -> Result<Self::Output, Box<PipelineExecutionError>> {
        Ok(())
    }
}

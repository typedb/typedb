/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datagen::RandomDataGen;
use executor::{document::ConceptDocument, pipeline::PipelineExecutionError, row::MaybeOwnedRow};
use lending_iterator::LendingIterator;
use query::given_rows::GivenRowEntry;

use crate::query::AnswerConsumer;

pub mod benchmark;
pub mod datagen;
pub mod profiles;
pub mod reports;
pub mod run_configs;
mod workload;

#[derive(Clone, Copy)]
pub enum TxType {
    Read,
    Write,
}

#[derive(Clone)]
pub struct QueryDescriptor {
    pub name: String,
    pub query: String,
    pub variables: Vec<String>,
    pub produce_row: Option<fn(&mut RandomDataGen) -> Vec<GivenRowEntry>>,
    pub tx_type: TxType,
}

#[derive(Clone, Copy)]
pub struct RunDescriptor {
    pub parallelism: usize,
    pub total_txns: usize,
    pub n_queries_per_tx: usize,
    pub n_rows_per_query: usize,
}

impl RunDescriptor {
    const WARMUP: RunDescriptor =
        RunDescriptor { parallelism: 1, total_txns: 10, n_queries_per_tx: 1, n_rows_per_query: 10 };

    pub fn total_rows(&self) -> usize {
        self.total_txns * self.n_queries_per_tx * self.n_rows_per_query
    }
}

pub fn transaction_options_with_profiling() -> options::TransactionOptions {
    let mut tx_options = options::TransactionOptions::default();
    tx_options.enable_profiling = Some(true);
    tx_options
}

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

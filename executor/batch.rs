/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    array,
    iter::{Map, Take, Zip},
    vec,
};

use answer::variable_value::VariableValue;
use itertools::Itertools;
use lending_iterator::LendingIterator;

use crate::{
    error::ReadExecutionError,
    row::{MaybeOwnedRow, Row},
};

const FIXED_BATCH_ROWS_MAX: u32 = 64;

#[derive(Debug)]
pub struct FixedBatch {
    width: u32,
    entries: u32,
    data: Vec<VariableValue<'static>>,
    multiplicities: [u64; FIXED_BATCH_ROWS_MAX as usize],
}

impl FixedBatch {
    pub(crate) const INIT_MULTIPLICITIES: [u64; FIXED_BATCH_ROWS_MAX as usize] = [1; FIXED_BATCH_ROWS_MAX as usize];
    pub(crate) const SINGLE_EMPTY_ROW: FixedBatch =
        FixedBatch { width: 0, entries: 1, data: Vec::new(), multiplicities: FixedBatch::INIT_MULTIPLICITIES };

    pub(crate) const EMPTY: FixedBatch =
        FixedBatch { width: 0, entries: 0, data: Vec::new(), multiplicities: FixedBatch::INIT_MULTIPLICITIES };

    pub(crate) fn new(width: u32) -> Self {
        let size = width * FIXED_BATCH_ROWS_MAX;
        FixedBatch {
            width,
            data: vec![VariableValue::Empty; size as usize],
            entries: 0,
            multiplicities: FixedBatch::INIT_MULTIPLICITIES,
        }
    }

    pub fn width(&self) -> u32 {
        self.width
    }

    pub(crate) fn len(&self) -> u32 {
        self.entries
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.multiplicities[..self.entries as usize].iter().all(|&mul| mul == 0)
    }

    pub(crate) fn is_full(&self) -> bool {
        self.entries == FIXED_BATCH_ROWS_MAX
    }

    pub(crate) fn get_row(&self, index: u32) -> MaybeOwnedRow<'_> {
        debug_assert!(index <= self.entries);
        let start = (index * self.width) as usize;
        let end = ((index + 1) * self.width) as usize;
        let slice = &self.data[start..end];
        MaybeOwnedRow::new_borrowed(slice, &self.multiplicities[index as usize])
    }

    pub(crate) fn get_row_mut(&mut self, index: u32) -> Row<'_> {
        debug_assert!(index <= self.entries);
        self.row_internal_mut(index)
    }

    pub(crate) fn append<T>(&mut self, writer: impl FnOnce(Row<'_>) -> T) -> T {
        debug_assert!(!self.is_full());
        let row = self.row_internal_mut(self.entries);
        let result = writer(row);
        self.entries += 1;
        result
    }

    fn row_internal_mut(&mut self, index: u32) -> Row<'_> {
        let start = (index * self.width) as usize;
        let end = ((index + 1) * self.width) as usize;
        let slice = &mut self.data[start..end];
        Row::new(slice, &mut self.multiplicities[index as usize])
    }
}

impl<'a> From<MaybeOwnedRow<'a>> for FixedBatch {
    fn from(row: MaybeOwnedRow<'a>) -> Self {
        let width = row.len() as u32;
        let mut multiplicities = FixedBatch::INIT_MULTIPLICITIES;
        multiplicities[0] = row.multiplicity();
        FixedBatch { width, data: row.row().to_owned(), entries: 1, multiplicities }
    }
}

impl IntoIterator for FixedBatch {
    type IntoIter = Map<
        Take<Zip<vec::IntoIter<Vec<VariableValue<'static>>>, array::IntoIter<u64, { FIXED_BATCH_ROWS_MAX as usize }>>>,
        fn((Vec<VariableValue<'static>>, u64)) -> MaybeOwnedRow<'static>,
    >;

    type Item = MaybeOwnedRow<'static>;

    fn into_iter(self) -> Self::IntoIter {
        let rows = if self.width == 0 {
            vec![vec![]; self.entries as usize]
        } else {
            self.data.into_iter().chunks(self.width as usize).into_iter().map(|chunk| chunk.collect_vec()).collect_vec()
        };
        rows.into_iter()
            .zip(self.multiplicities)
            .take(self.entries as usize)
            .map(|(row, mult)| MaybeOwnedRow::new_owned(row, mult))
    }
}

#[derive(Debug)]
pub struct FixedBatchRowIterator {
    batch: Result<FixedBatch, Box<ReadExecutionError>>,
    index: u32,
}

impl FixedBatchRowIterator {
    pub(crate) fn new(batch: Result<FixedBatch, Box<ReadExecutionError>>) -> Self {
        Self { batch, index: 0 }
    }

    fn has_next(&self) -> bool {
        self.batch.as_ref().is_ok_and(|batch| self.index < batch.len())
    }
}

impl LendingIterator for FixedBatchRowIterator {
    type Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.batch.as_mut() {
            Ok(batch) => {
                if self.index >= batch.len() {
                    None
                } else {
                    let row = batch.get_row(self.index);
                    self.index += 1;
                    Some(Ok(row))
                }
            }
            Err(err) => Some(Err(err)),
        }
    }
}

#[derive(Debug)]
pub struct Batch {
    width: u32,
    entries: usize,
    data: Vec<VariableValue<'static>>,
    multiplicities: Vec<u64>,
}

impl Batch {
    pub(crate) fn new(width: u32, length: usize) -> Self {
        let size = width as usize * length;
        Batch { width, data: vec![VariableValue::Empty; size], entries: 0, multiplicities: vec![1; length] }
    }

    pub fn len(&self) -> usize {
        self.entries
    }

    pub(crate) fn is_full(&self) -> bool {
        (self.entries * self.width as usize) == self.data.len()
    }

    pub(crate) fn get_multiplicities(&self) -> &[u64] {
        self.multiplicities.as_ref()
    }

    pub(crate) fn get_row(&self, index: usize) -> MaybeOwnedRow<'_> {
        debug_assert!(index <= self.entries);
        let start = index * self.width as usize;
        let end = (index + 1) * self.width as usize;
        let slice = &self.data[start..end];
        MaybeOwnedRow::new_borrowed(slice, &self.multiplicities[index])
    }

    pub(crate) fn get_row_mut(&mut self, index: usize) -> Row<'_> {
        debug_assert!(index <= self.entries);
        self.row_internal_mut(index)
    }

    pub(crate) fn append(&mut self, row: MaybeOwnedRow<'_>) {
        let mut destination_row = self.row_internal_mut(self.entries);
        destination_row.copy_from_row(row);
        self.entries += 1;
    }

    fn row_internal_mut(&mut self, index: usize) -> Row<'_> {
        let start = index * self.width as usize;
        let end = (index + 1) * self.width as usize;
        if end > self.data.len() {
            self.data.resize(end, VariableValue::Empty);
            self.multiplicities.resize(index + 1, 1);
        }
        let slice = &mut self.data[start..end];
        Row::new(slice, &mut self.multiplicities[index])
    }

    pub fn into_iterator_mut(self) -> MutableBatchRowIterator {
        MutableBatchRowIterator::new(self)
    }

    pub fn into_iterator(self) -> BatchRowIterator {
        BatchRowIterator::new(self)
    }
}

pub struct MutableBatchRowIterator {
    batch: Batch,
    index: usize,
}

impl MutableBatchRowIterator {
    pub(crate) fn new(batch: Batch) -> Self {
        Self { batch, index: 0 }
    }
}

impl LendingIterator for MutableBatchRowIterator {
    type Item<'a> = Row<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.index >= self.batch.len() {
            None
        } else {
            let row = self.batch.get_row_mut(self.index);
            self.index += 1;
            Some(row)
        }
    }
}
#[derive(Debug)]
pub struct BatchRowIterator {
    batch: Batch,
    index: usize,
}

impl BatchRowIterator {
    pub(crate) fn new(batch: Batch) -> Self {
        Self { batch, index: 0 }
    }
}

impl LendingIterator for BatchRowIterator {
    type Item<'a> = MaybeOwnedRow<'a>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.index >= self.batch.len() {
            None
        } else {
            let row = self.batch.get_row(self.index);
            self.index += 1;
            Some(row)
        }
    }
}

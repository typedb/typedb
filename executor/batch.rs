/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
    vec,
};

use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use lending_iterator::LendingIterator;

use crate::VariablePosition;

const BATCH_ROWS_MAX: u32 = 64;

#[derive(Debug)]
pub struct Batch {
    width: u32,
    entries: u32,
    data: Vec<VariableValue<'static>>,
    multiplicities: [u64; BATCH_ROWS_MAX as usize],
}

impl Batch {
    pub(crate) const INIT_MULTIPLICITIES: [u64; BATCH_ROWS_MAX as usize] = [1; BATCH_ROWS_MAX as usize];
    pub(crate) const EMPTY_SINGLE_ROW: Batch =
        Batch { width: 0, entries: 1, data: Vec::new(), multiplicities: Batch::INIT_MULTIPLICITIES };

    pub(crate) fn new(width: u32) -> Self {
        let size = width * BATCH_ROWS_MAX;
        Batch {
            width,
            data: vec![VariableValue::Empty; size as usize],
            entries: 0,
            multiplicities: Batch::INIT_MULTIPLICITIES,
        }
    }

    fn rows_count(&self) -> u32 {
        self.entries
    }

    pub(crate) fn is_full(&self) -> bool {
        (self.entries * self.width) as usize == self.data.len()
    }

    fn get_row(&self, index: u32) -> ImmutableRow<'_> {
        debug_assert!(index <= self.entries);
        let start = (index * self.width) as usize;
        let end = ((index + 1) * self.width) as usize;
        let slice = &self.data[start..end];
        ImmutableRow::new(slice, self.multiplicities[index as usize])
    }

    fn get_row_mut(&mut self, index: u32) -> Row<'_> {
        debug_assert!(index <= self.entries);
        self.row_internal_mut(index)
    }

    pub(crate) fn append<T>(&mut self, mut writer: impl FnMut(Row<'_>) -> T) -> T {
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

    pub(crate) fn into_iterator(self) -> BatchRowIterator {
        BatchRowIterator::new(Ok(self))
    }
}

pub struct BatchRowIterator {
    batch: Result<Batch, ConceptReadError>,
    index: u32,
}

impl BatchRowIterator {
    pub(crate) fn new(batch: Result<Batch, ConceptReadError>) -> Self {
        Self { batch, index: 0 }
    }

    fn has_next(&self) -> bool {
        self.batch.as_ref().is_ok_and(|batch| self.index < batch.rows_count())
    }
}

impl LendingIterator for BatchRowIterator {
    type Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        match self.batch.as_mut() {
            Ok(batch) => {
                if self.index >= batch.rows_count() {
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
pub struct Row<'a> {
    row: &'a mut [VariableValue<'static>],
    multiplicity: &'a mut u64,
}

impl<'a> Row<'a> {
    // TODO: pub(crate)
    pub fn new(row: &'a mut [VariableValue<'static>], multiplicity: &'a mut u64) -> Self {
        Self { row, multiplicity }
    }

    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }

    pub(crate) fn get(&self, position: VariablePosition) -> &VariableValue<'static> {
        &self.row[position.as_usize()]
    }

    pub(crate) fn set(&mut self, position: VariablePosition, value: VariableValue<'static>) {
        debug_assert!(*self.get(position) == VariableValue::Empty || *self.get(position) == value);
        self.row[position.as_usize()] = value;
    }

    pub(crate) fn copy_from(&mut self, row: &[VariableValue<'static>], multiplicity: u64) {
        debug_assert!(self.len() == row.len());
        self.row.clone_from_slice(row);
        *self.multiplicity = multiplicity;
    }

    pub(crate) fn multiplicity(&self) -> u64 {
        *self.multiplicity
    }

    pub(crate) fn set_multiplicity(&mut self, multiplicity: u64) {
        *self.multiplicity = multiplicity;
    }
}

impl<'a> Display for Row<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} x [  ", self.multiplicity)?;
        for value in &*self.row {
            write!(f, "{value}  ")?
        }
        writeln!(f, "]")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableRow<'a> {
    row: Cow<'a, [VariableValue<'static>]>,
    multiplicity: u64,
}

impl<'a> ImmutableRow<'a> {
    pub(crate) fn new(row: &'a [VariableValue<'static>], multiplicity: u64) -> Self {
        Self { row: Cow::Borrowed(row), multiplicity }
    }

    pub fn width(&self) -> usize {
        self.row.len()
    }

    pub fn get(&self, position: VariablePosition) -> &VariableValue<'_> {
        &self.row[position.as_usize()]
    }

    pub fn get_multiplicity(&self) -> u64 {
        self.multiplicity
    }

    pub fn into_owned(self) -> ImmutableRow<'static> {
        let cloned: Vec<VariableValue<'static>> = self.row.iter().map(|value| value.clone().into_owned()).collect();
        ImmutableRow { row: Cow::Owned(cloned), multiplicity: self.multiplicity }
    }

    pub fn as_reference(&self) -> ImmutableRow<'_> {
        ImmutableRow { row: Cow::Borrowed(self.row.as_ref()), multiplicity: self.multiplicity }
    }
}

impl IntoIterator for ImmutableRow<'static> {
    type Item = VariableValue<'static>;
    type IntoIter = vec::IntoIter<VariableValue<'static>>;
    fn into_iter(self) -> Self::IntoIter {
        #[allow(clippy::unnecessary_to_owned)]
        self.row.into_owned().into_iter()
    }
}

impl<'a> Display for ImmutableRow<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} x [  ", self.multiplicity)?;
        for value in &*self.row {
            write!(f, "{value}  ")?
        }
        writeln!(f, "]")?;
        Ok(())
    }
}

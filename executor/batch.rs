/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    array,
    borrow::Cow,
    cmp::Ordering,
    iter::{Map, Take, Zip},
    vec,
};

use answer::{Thing, variable_value::VariableValue};
use encoding::value::value::Value;
use error::unimplemented_feature;
use itertools::Itertools;
use lending_iterator::LendingIterator;
use resource::{constants::traversal::FIXED_BATCH_ROWS_MAX, profile::StorageCounters};
use storage::snapshot::ReadableSnapshot;

use crate::{
    Provenance,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    row::{MaybeOwnedRow, Row},
};

#[derive(Debug)]
pub struct FixedBatch {
    width: u32,
    entries: u32,
    data: Vec<VariableValue<'static>>,
    multiplicities: [u64; FIXED_BATCH_ROWS_MAX as usize],
    provenance: [Provenance; FIXED_BATCH_ROWS_MAX as usize],
}

impl FixedBatch {
    pub(crate) const INIT_MULTIPLICITIES: [u64; FIXED_BATCH_ROWS_MAX as usize] = [1; FIXED_BATCH_ROWS_MAX as usize];
    pub(crate) const INIT_PROVENANCES: [Provenance; FIXED_BATCH_ROWS_MAX as usize] =
        [Provenance(0); FIXED_BATCH_ROWS_MAX as usize];
    pub(crate) const SINGLE_EMPTY_ROW: FixedBatch = FixedBatch {
        width: 0,
        entries: 1,
        data: Vec::new(),
        multiplicities: FixedBatch::INIT_MULTIPLICITIES,
        provenance: FixedBatch::INIT_PROVENANCES,
    };

    pub(crate) const EMPTY: FixedBatch = FixedBatch {
        width: 0,
        entries: 0,
        data: Vec::new(),
        multiplicities: FixedBatch::INIT_MULTIPLICITIES,
        provenance: FixedBatch::INIT_PROVENANCES,
    };

    pub(crate) fn new(width: u32) -> Self {
        let size = width * FIXED_BATCH_ROWS_MAX;
        FixedBatch {
            width,
            data: vec![VariableValue::None; size as usize],
            entries: 0,
            multiplicities: FixedBatch::INIT_MULTIPLICITIES,
            provenance: FixedBatch::INIT_PROVENANCES,
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
        debug_assert!(index < self.entries);
        let slice = &self.data[row_range(index as usize, self.width)];
        MaybeOwnedRow::new_borrowed(slice, &self.multiplicities[index as usize], &self.provenance[index as usize])
    }

    pub(crate) fn get_row_mut(&mut self, index: u32) -> Row<'_> {
        debug_assert!(index < self.entries);
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
        let slice = &mut self.data[row_range(index as usize, self.width)];
        Row::new(slice, &mut self.multiplicities[index as usize], &mut self.provenance[index as usize])
    }

    pub(crate) fn retain_rows<E>(
        &mut self,
        mut keep: impl FnMut(MaybeOwnedRow<'_>) -> Result<bool, E>,
    ) -> Result<(), E> {
        let width = self.width as usize;
        let mut kept = 0usize;
        for index in 0..self.entries as usize {
            if keep(self.get_row(index as u32))? {
                if index != kept {
                    let (front, back) = self.data.split_at_mut(index * width);
                    front[kept * width..(kept + 1) * width].swap_with_slice(&mut back[..width]);
                    self.multiplicities[kept] = self.multiplicities[index];
                    self.provenance[kept] = self.provenance[index];
                }
                kept += 1;
            }
        }
        self.data[kept * width..self.entries as usize * width].fill(VariableValue::None);
        self.entries = kept as u32;
        Ok(())
    }

    pub(crate) fn narrow(&mut self, width: u32) {
        debug_assert!(width <= self.width);
        if width == self.width {
            return;
        }
        let (old_width, new_width, entries) = (self.width as usize, width as usize, self.entries as usize);
        for row in 1..entries {
            for column in 0..new_width {
                self.data.swap(row * new_width + column, row * old_width + column);
            }
        }
        self.data[entries * new_width..entries * old_width].fill(VariableValue::None);
        self.width = width;
    }
}

impl<'a> From<MaybeOwnedRow<'a>> for FixedBatch {
    fn from(row: MaybeOwnedRow<'a>) -> Self {
        let width = row.len() as u32;
        let mut multiplicities = FixedBatch::INIT_MULTIPLICITIES;
        multiplicities[0] = row.multiplicity();
        let mut branch_provenance = FixedBatch::INIT_PROVENANCES;
        branch_provenance[0] = row.provenance();
        FixedBatch { width, data: row.row().to_owned(), entries: 1, multiplicities, provenance: branch_provenance }
    }
}

impl IntoIterator for FixedBatch {
    type Item = MaybeOwnedRow<'static>;

    type IntoIter = Map<
        Take<
            Zip<
                vec::IntoIter<Vec<VariableValue<'static>>>,
                Zip<
                    array::IntoIter<u64, { FIXED_BATCH_ROWS_MAX as usize }>,
                    array::IntoIter<Provenance, { FIXED_BATCH_ROWS_MAX as usize }>,
                >,
            >,
        >,
        fn((Vec<VariableValue<'static>>, (u64, Provenance))) -> MaybeOwnedRow<'static>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        let rows = if self.width == 0 {
            vec![vec![]; self.entries as usize]
        } else {
            self.data.into_iter().chunks(self.width as usize).into_iter().map(|chunk| chunk.collect_vec()).collect_vec()
        };
        rows.into_iter()
            .zip(self.multiplicities.into_iter().zip(self.provenance.into_iter()))
            .take(self.entries as usize)
            .map(|(row, (mult, provenance))| MaybeOwnedRow::new_owned(row, mult, provenance))
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
    data: Vec<VariableValue<'static>>,
    multiplicities: Vec<u64>,
    provenance: Vec<Provenance>,
}

impl Batch {
    pub fn new(width: u32, capacity: usize) -> Self {
        let size = width as usize * capacity;
        Batch {
            width,
            data: Vec::with_capacity(size),
            multiplicities: Vec::with_capacity(capacity),
            provenance: Vec::with_capacity(capacity),
        }
    }

    pub fn new_single_empty_row() -> Batch {
        let mut this = Self::new(0, 1);
        this.append_row(MaybeOwnedRow::empty());
        this
    }

    pub fn width(&self) -> u32 {
        self.width
    }

    pub fn len(&self) -> usize {
        debug_assert!(self.multiplicities.len() * self.width as usize == self.data.len());
        self.multiplicities.len()
    }

    pub(crate) fn get_multiplicities(&self) -> &[u64] {
        &self.multiplicities
    }

    pub(crate) fn get_row(&self, index: usize) -> MaybeOwnedRow<'_> {
        debug_assert!(index < self.len());
        let slice = &self.data[row_range(index, self.width)];
        MaybeOwnedRow::new_borrowed(slice, &self.multiplicities[index], &self.provenance[index])
    }

    pub(crate) fn get_row_mut(&mut self, index: usize) -> Row<'_> {
        debug_assert!(index < self.len());
        let slice = &mut self.data[row_range(index, self.width)];
        Row::new(slice, &mut self.multiplicities[index], &mut self.provenance[index])
    }

    pub(crate) fn append_row(&mut self, row: MaybeOwnedRow<'_>) {
        self.append(|mut appended| appended.copy_from_row(row))
    }

    pub fn append<T>(&mut self, writer: impl FnOnce(Row<'_>) -> T) -> T {
        self.data.resize(self.data.len() + self.width as usize, VariableValue::None);
        self.multiplicities.push(1);
        self.provenance.push(Provenance::INITIAL);
        debug_assert!(self.data.len() == self.multiplicities.len() * self.width as usize);
        debug_assert!(self.multiplicities.len() == self.provenance.len());
        writer(self.get_row_mut(self.len() - 1))
    }

    pub fn into_iterator_mut(self) -> MutableBatchRowIterator {
        MutableBatchRowIterator::new(self)
    }

    pub fn into_iterator(self) -> BatchRowIterator {
        BatchRowIterator::new(self)
    }

    pub fn iter(&self) -> impl Iterator<Item = MaybeOwnedRow<'_>> {
        (0..self.len()).map(|i| self.get_row(i))
    }

    pub(crate) fn indices_sorted_by(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot>,
        sort_by: &[(usize, bool)],
        storage_counters: StorageCounters,
    ) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..self.len()).collect();
        indices.sort_by(|x, y| {
            let x_row_as_row = self.get_row(*x);
            let y_row_as_row = self.get_row(*y);
            let x_row = x_row_as_row.row();
            let y_row = y_row_as_row.row();
            for (idx, asc) in sort_by.iter() {
                let ord = get_value(&x_row[*idx], context, storage_counters.clone())
                    .partial_cmp(&get_value(&y_row[*idx], context, storage_counters.clone()))
                    .expect("Sort on variable with uncomparable values should have been caught at query-compile time");
                match (asc, ord) {
                    (true, Ordering::Less) | (false, Ordering::Greater) => return Ordering::Less,
                    (true, Ordering::Greater) | (false, Ordering::Less) => return Ordering::Greater,
                    (true, Ordering::Equal) | (false, Ordering::Equal) => {}
                };
            }
            Ordering::Equal
        });
        indices
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

    pub(crate) fn initial_len(&self) -> usize {
        self.batch.len()
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

fn row_range(index: usize, width: u32) -> std::ops::Range<usize> {
    let start = index * width as usize;
    let end = start + width as usize;
    start..end
}

fn get_value<'a, T: ReadableSnapshot>(
    entry: &'a VariableValue<'a>,
    context: &'a ExecutionContext<T>,
    storage_counters: StorageCounters,
) -> Option<Cow<'a, Value<'a>>> {
    let snapshot: &T = &context.snapshot;
    match entry {
        VariableValue::Value(value) => Some(Cow::Borrowed(value)),
        VariableValue::Thing(Thing::Attribute(attribute)) => {
            Some(Cow::Owned(attribute.get_value(snapshot, &context.thing_manager, storage_counters).unwrap()))
        }
        VariableValue::None => None,
        VariableValue::Type(_) | VariableValue::Thing(_) => {
            unreachable!("Should have been caught earlier")
        }

        VariableValue::ThingList(_) => unimplemented_feature!(Lists),
        VariableValue::ValueList(_) => unimplemented_feature!(Lists),
    }
}

#[cfg(test)]
mod tests {
    use answer::variable_value::VariableValue;
    use compiler::VariablePosition;
    use encoding::value::value::Value;

    use super::FixedBatch;
    use crate::Provenance;

    fn integer(value: i64) -> VariableValue<'static> {
        VariableValue::Value(Value::Integer(value))
    }

    /// Two-column batch: row i holds (values[i], 10 * values[i]) with multiplicity i + 1 and provenance 100 + i.
    fn batch_of(values: &[i64]) -> FixedBatch {
        let mut batch = FixedBatch::new(2);
        for (index, &value) in values.iter().enumerate() {
            batch.append(|mut row| {
                row.set(VariablePosition::new(0), integer(value));
                row.set(VariablePosition::new(1), integer(value * 10));
                row.set_multiplicity(index as u64 + 1);
                row.set_provenance(Provenance(index as u64 + 100));
            });
        }
        batch
    }

    fn column(batch: &FixedBatch, position: u32) -> Vec<i64> {
        (0..batch.len())
            .map(|index| match batch.get_row(index).get(VariablePosition::new(position)) {
                VariableValue::Value(Value::Integer(value)) => *value,
                other => panic!("unexpected value {other:?}"),
            })
            .collect()
    }

    fn is_even(row: &super::MaybeOwnedRow<'_>) -> bool {
        matches!(row.get(VariablePosition::new(0)), VariableValue::Value(Value::Integer(value)) if value % 2 == 0)
    }

    #[test]
    fn retain_rows_compacts_in_order_and_moves_row_metadata() {
        let mut batch = batch_of(&[1, 2, 3, 4, 5, 6]);
        batch.retain_rows::<()>(|row| Ok(is_even(&row))).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(column(&batch, 0), vec![2, 4, 6]);
        assert_eq!(column(&batch, 1), vec![20, 40, 60]);
        // the rows for 2, 4, 6 were appended at indices 1, 3, 5
        assert_eq!((0..3).map(|i| batch.get_row(i).multiplicity()).collect::<Vec<_>>(), vec![2, 4, 6]);
        assert_eq!((0..3).map(|i| batch.get_row(i).provenance().0).collect::<Vec<_>>(), vec![101, 103, 105]);
    }

    #[test]
    fn retain_rows_keeping_everything_leaves_the_batch_unchanged() {
        let mut batch = batch_of(&[7, 8, 9]);
        batch.retain_rows::<()>(|_| Ok(true)).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(column(&batch, 0), vec![7, 8, 9]);
        assert_eq!((0..3).map(|i| batch.get_row(i).multiplicity()).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn retain_rows_keeping_nothing_empties_the_batch() {
        let mut batch = batch_of(&[1, 3, 5]);
        batch.retain_rows::<()>(|_| Ok(false)).unwrap();
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn retain_rows_on_a_full_batch() {
        let values: Vec<i64> = (0..super::FIXED_BATCH_ROWS_MAX as i64).collect();
        let mut batch = batch_of(&values);
        assert!(batch.is_full());
        batch.retain_rows::<()>(|row| Ok(is_even(&row))).unwrap();
        assert_eq!(batch.len(), super::FIXED_BATCH_ROWS_MAX / 2);
        assert!(!batch.is_full());
        assert_eq!(column(&batch, 0), values.iter().copied().filter(|v| v % 2 == 0).collect::<Vec<_>>());
    }

    #[test]
    fn retain_rows_leaves_no_values_beyond_the_kept_rows() {
        let mut batch = batch_of(&[1, 2, 3, 4]);
        batch.retain_rows::<()>(|row| Ok(is_even(&row))).unwrap();
        assert!(batch.data[batch.len() as usize * 2..].iter().all(|value| *value == VariableValue::None));
    }

    #[test]
    fn narrow_keeps_the_leading_columns_of_every_row() {
        let mut batch = batch_of(&[1, 2, 3, 4, 5]);
        batch.retain_rows::<()>(|row| Ok(!is_even(&row))).unwrap();
        batch.narrow(1);
        assert_eq!(batch.width(), 1);
        assert_eq!(batch.len(), 3);
        assert_eq!(column(&batch, 0), vec![1, 3, 5]);
        assert_eq!((0..3).map(|i| batch.get_row(i).multiplicity()).collect::<Vec<_>>(), vec![1, 3, 5]);
        assert!(batch.data[3..].iter().all(|value| *value == VariableValue::None));
    }

    #[test]
    fn narrow_to_the_same_width_is_a_no_op() {
        let mut batch = batch_of(&[7, 8]);
        batch.narrow(2);
        assert_eq!(batch.width(), 2);
        assert_eq!(column(&batch, 1), vec![70, 80]);
    }

    #[test]
    fn narrow_handles_empty_single_row_and_full_batches() {
        let mut empty = FixedBatch::new(3);
        empty.narrow(1);
        assert_eq!((empty.width(), empty.len()), (1, 0));

        let mut single = FixedBatch::from(super::MaybeOwnedRow::new_owned(
            vec![integer(1), integer(2), integer(3)],
            4,
            crate::Provenance(9),
        ));
        single.narrow(2);
        assert_eq!((single.width(), single.len()), (2, 1));
        assert_eq!(column(&single, 1), vec![2]);
        assert_eq!((single.get_row(0).multiplicity(), single.get_row(0).provenance().0), (4, 9));
        assert!(single.data[2..].iter().all(|value| *value == VariableValue::None));

        let values: Vec<i64> = (0..super::FIXED_BATCH_ROWS_MAX as i64).collect();
        let mut full = batch_of(&values);
        full.narrow(1);
        assert_eq!(column(&full, 0), values);
        assert!(full.data[values.len()..].iter().all(|value| *value == VariableValue::None));

        let mut widthless = batch_of(&[5, 6]);
        widthless.narrow(0);
        assert_eq!((widthless.width(), widthless.len()), (0, 2));
        assert!(widthless.data.iter().all(|value| *value == VariableValue::None));
    }
}

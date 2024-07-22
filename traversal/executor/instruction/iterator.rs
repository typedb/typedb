/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::iter::Iterator;
use std::ops::Range;

use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use lending_iterator::{LendingIterator, Peekable};

use crate::executor::{
    batch::Row,
    instruction::has_executor::HasUnboundedSortedOwnerIterator,
};
use crate::executor::instruction::has_executor::{HasBoundedSortedAttributeIterator, HasUnboundedSortedAttributeMergedIterator, HasUnboundedSortedAttributeSingleIterator};
use crate::executor::instruction::tuple::{Tuple, TupleIndex, TuplePositions, TupleResult};

// TODO: the 'check' can deduplicate against all relevant variables as soon as an anonymous variable is no longer relevant.
//       if the deduplicated answer leads to an answer, we should not re-emit it again (we will rediscover the same answers)
//       if the deduplicated answer fails to lead to an answer, we should not re-emit it again as it will fail again

pub(crate) enum InstructionTuplesIterator {
    HasUnbounded(SortedTupleIterator<HasUnboundedSortedOwnerIterator>),
    HasUnboundedInvertedOrderSingle(SortedTupleIterator<HasUnboundedSortedAttributeSingleIterator>),
    HasUnboundedInvertedOrderMerged(SortedTupleIterator<HasUnboundedSortedAttributeMergedIterator>),
    HasBounded(SortedTupleIterator<HasBoundedSortedAttributeIterator>),
}

impl InstructionTuplesIterator {
    pub(crate) fn write_values(&mut self, row: &mut Row<'_>) {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.write_values(row),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.write_values(row),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.write_values(row),
            InstructionTuplesIterator::HasBounded(iter) => iter.write_values(row),
        }
    }

    pub(crate) fn peek(&mut self) -> Option<Result<&Tuple<'_>, ConceptReadError>> {
        let value = match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.peek(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.peek(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.peek(),
            InstructionTuplesIterator::HasBounded(iter) => iter.peek(),
        };
        value.map(|result| result.as_ref().map_err(|err| err.clone()))
    }

    pub(crate) fn advance_past(&mut self) -> Result<usize, ConceptReadError> {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.advance_past(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.advance_past(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.advance_past(),
            InstructionTuplesIterator::HasBounded(iter) => iter.advance_past(),
        }
    }

    pub(crate) fn advance_until_index_is(
        &mut self,
        index: TupleIndex,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, ConceptReadError> {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.skip_until_value(index, value),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.skip_until_value(index, value),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.skip_until_value(index, value),
            InstructionTuplesIterator::HasBounded(iter) => iter.skip_until_value(index, value),
        }
    }

    pub(crate) fn advance_single(&mut self) -> Result<(), ConceptReadError> {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.advance_single(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.advance_single(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.advance_single(),
            InstructionTuplesIterator::HasBounded(iter) => iter.advance_single(),
        }
    }

    pub(crate) fn peek_first_unbound_value(&mut self) -> Option<Result<&VariableValue<'_>, ConceptReadError>> {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.peek_first_unbound_value(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.peek_first_unbound_value(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.peek_first_unbound_value(),
            InstructionTuplesIterator::HasBounded(iter) => iter.peek_first_unbound_value(),
        }
    }

    pub(crate) fn first_unbound_index(&self) -> TupleIndex {
        match self {
            InstructionTuplesIterator::HasUnbounded(iter) => iter.first_unbound_index(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderSingle(iter) => iter.first_unbound_index(),
            InstructionTuplesIterator::HasUnboundedInvertedOrderMerged(iter) => iter.first_unbound_index(),
            InstructionTuplesIterator::HasBounded(iter) => iter.first_unbound_index(),
        }
    }
}

pub(crate) trait TupleIterator {
    fn write_values(&mut self, row: &mut Row<'_>);

    fn peek(&mut self) -> Option<&Result<Tuple<'_>, ConceptReadError>>;

    /// Advance the iterator past the current answer, and return the number duplicate answers were skipped
    fn advance_past(&mut self) -> Result<usize, ConceptReadError>;

    fn advance_single(&mut self) -> Result<(), ConceptReadError>;

    fn positions(&self) -> &TuplePositions;
}

pub(crate) struct SortedTupleIterator<Iterator: for<'a> LendingIterator<Item<'a>=TupleResult<'a>>> {
    iterator: Peekable<Iterator>,
    positions: TuplePositions,
    tuple_length: usize,
    first_unbound: TupleIndex,
    enumerate_range: Range<TupleIndex>,
    enumerate_or_count_range: Range<TupleIndex>,

    // examples:
    //   [ enumerate, enumerate ] --> no special action, just advance()
    //   [ enumerate, count ] --> advance() until POSITION changes
    //   [ enumerate, check ] --> (same) or seek to POSITION+1
    //   [ count, count ] --> count all (TODO: how to unifiy with other counts until POSITION changes)
    //   [ count, check ] --> match $x has name "john"; $y; select $y;

    // [ enumerate, count, count ] --> match $r links! ($rt: $x); select $r;
    // [ enumerate, count, check ] -->

    // basically: we can have 1 Enumerate, 1 Count and 1 Check position (most RHS count....) what about check, check?

    // Count position: last Enumerated. Optimisation: advance() until Enumerated changes, return Count.
    //  Checks: advance() must loop until a counted or enumerated variable changes
    //  We store two positions: LastEnumerated position, and LastEnumeratedOrCounted position.
    //    --> advance_past_enumerated() will advance_past_counted() until LastEnumerated changes.
    //    --> advance_past_counted() will advance() until LastEnumeratedOrCounted changes.
    //    --> advance() will move forward 1 position... or it could seek to `lastEnumerateOrCounted` + 1?
}

impl<Iterator: for<'a> LendingIterator<Item<'a>=TupleResult<'a>>> SortedTupleIterator<Iterator> {
    pub(crate) fn new(
        iterator: Iterator,
        tuple_positions: TuplePositions,
        first_unbound: TupleIndex,
        enumerate_range: Range<TupleIndex>,
        enumerate_or_count_range: Range<TupleIndex>,
    ) -> Self {
        debug_assert!(!enumerate_or_count_range.is_empty());
        Self {
            iterator: Peekable::new(iterator),
            tuple_length: tuple_positions.positions().len(),
            positions: tuple_positions,
            first_unbound,
            enumerate_range,
            enumerate_or_count_range,
        }
    }

    fn count_until_changes(&mut self, range: Range<TupleIndex>) -> Result<usize, ConceptReadError> {
        debug_assert!(self.peek().is_some() && !range.is_empty());

        if range.len() == self.tuple_length {
            self.advance_single()?;
            return Ok(1)
        }

        let current = self.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into_owned();
        let current_range = &current.values()[range.start as usize..range.end as usize];
        self.iterator.next().unwrap()?;
        let mut count = 1;
        loop {
            let peek = self.iterator.peek();
            match peek {
                None => return Ok(count),
                Some(Ok(tuple)) => {
                    let values = &tuple.values()[range.start as usize..range.end as usize];
                    if values != current_range {
                        return Ok(count);
                    } else {
                        count += 1;
                        let _ = self.iterator.next().unwrap()?;
                    }
                }
                Some(Err(err)) => return Err(err.clone()),
            }
        }
    }

    fn first_unbound_index(&self) -> TupleIndex {
        self.first_unbound
    }

    fn skip_until_changes(&mut self, range: Range<TupleIndex>) -> Result<(), ConceptReadError> {
        // TODO: this should be optimisable with seek(to peek[index].increment())
        self.count_until_changes(range).map(|_| ())
    }

    fn skip_until_value(&mut self, index: TupleIndex, target: &VariableValue<'_>) -> Result<Option<Ordering>, ConceptReadError> {
        // TODO: this should use seek if index == self.first_unbound()
        loop {
            let peek = self.peek();
            match peek {
                None => return Ok(None),
                Some(Ok(tuple)) => {
                    let value = &tuple.values()[index as usize];
                    match value.partial_cmp(target).unwrap() {
                        Ordering::Less => {
                            self.advance_single()?;
                        }
                        Ordering::Equal => {
                            // matched exactly
                            return Ok(Some(Ordering::Equal));
                        }
                        Ordering::Greater => {
                            // overshot
                            return Ok(Some(Ordering::Greater));
                        }
                    }
                }
                Some(Err(err)) => return Err(err.clone()),
            }
        }
    }

    fn peek_first_unbound_value(&mut self) -> Option<Result<&VariableValue<'_>, ConceptReadError>> {
        self.peek_current_value_at(self.first_unbound)
    }

    fn peek_current_value_at(&mut self, index: TupleIndex) -> Option<Result<&VariableValue<'_>, ConceptReadError>> {
        self.peek().map(|result| result.as_ref().map(|tuple| &tuple.values()[index as usize]).map_err(|err| err.clone()))
    }
}

impl<Iterator: for<'a> LendingIterator<Item<'a>=TupleResult<'a>>> TupleIterator for SortedTupleIterator<Iterator> {
    fn write_values(&mut self, row: &mut Row<'_>) {
        debug_assert!(self.peek().is_some() && self.peek().unwrap().is_ok());
        // note: can't use self.peek() since it will cause mut and immutable reference to self
        let tuple = self.iterator.peek().unwrap().as_ref().unwrap();
        match tuple {
            Tuple::Single([value]) => {
                row.set(self.positions.as_single()[0], value.clone().into_owned());
            }
            Tuple::Pair([value_1, value_2]) => {
                let positions = self.positions.as_pair();
                row.set(positions[0], value_1.clone().into_owned());
                row.set(positions[1], value_2.clone().into_owned());
            }
            Tuple::Triple([value_1, value_2, value_3]) => {
                let positions = self.positions.as_triple();
                row.set(positions[0], value_1.clone().into_owned());
                row.set(positions[1], value_2.clone().into_owned());
                row.set(positions[2], value_3.clone().into_owned());
            }
            Tuple::Quintuple([value_1, value_2, value_3, value_4, value_5]) => {
                let positions = self.positions.as_quintuple();
                row.set(positions[0], value_1.clone().into_owned());
                row.set(positions[1], value_2.clone().into_owned());
                row.set(positions[2], value_3.clone().into_owned());
                row.set(positions[3], value_4.clone().into_owned());
                row.set(positions[4], value_5.clone().into_owned());
            }
            Tuple::Arbitrary() => {
                todo!()
            }
        }
    }

    fn peek(&mut self) -> Option<&Result<Tuple<'_>, ConceptReadError>> {
        self.iterator.peek()
    }

    fn advance_past(&mut self) -> Result<usize, ConceptReadError> {
        debug_assert!(self.peek().is_some());

        match (!self.enumerate_range.is_empty(), self.enumerate_range != self.enumerate_or_count_range) {
            (true, true) => {
                let mut count = 1;
                let current = self.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into_owned();
                let enumerated = &current.values()[self.enumerate_range.start as usize..self.enumerate_range.end as usize];
                loop {
                    // TODO: this feels inefficient since each skip() call does a copy of the current tuple
                    self.skip_until_changes(self.enumerate_or_count_range.clone())?;
                    match self.iterator.peek() {
                        None => return Ok(count),
                        Some(Ok(tuple)) => {
                            if &tuple.values()[self.enumerate_range.start as usize..self.enumerate_range.end as usize] != enumerated {
                                return Ok(count);
                            } else {
                                count += 1;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (true, false) => {
                return self.count_until_changes(self.enumerate_range.clone())
            }
            (false, true) => {
                if self.enumerate_or_count_range.len() == self.tuple_length {
                    return Ok(self.iterator.count_as_ref())
                } else {
                    let mut count = 1;
                    // TODO: this feels inefficient since each skip() call does a copy of the current tuple
                    while self.peek().is_some() {
                        self.skip_until_changes(self.enumerate_or_count_range.clone())?;
                        count += 1;
                    }
                    return Ok(count)
                }
            }
            (false, false) => {
                // just check it exists and end the iterator
                todo!()
            }
        }
    }

    fn advance_single(&mut self) -> Result<(), ConceptReadError> {
        let _ = self.iterator.next().unwrap()?;
        Ok(())
    }

    fn positions(&self) -> &TuplePositions {
        &self.positions
    }
}

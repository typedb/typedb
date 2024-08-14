/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, iter::Iterator, ops::RangeInclusive};

use answer::variable_value::VariableValue;
use concept::error::ConceptReadError;
use lending_iterator::{LendingIterator, Peekable};

use super::{
    isa_executor::{
        IsaBoundedSortedType, IsaUnboundedSortedThingMerged, IsaUnboundedSortedThingSingle,
        IsaUnboundedSortedTypeMerged, IsaUnboundedSortedTypeSingle,
    },
    isa_reverse_executor::{
        IsaReverseBoundedSortedThing, IsaReverseUnboundedSortedThingMerged, IsaReverseUnboundedSortedThingSingle,
        IsaReverseUnboundedSortedTypeMerged, IsaReverseUnboundedSortedTypeSingle,
    },
};
use crate::{
    batch::Row,
    instruction::{
        has_executor::{
            HasBoundedSortedAttribute, HasUnboundedSortedAttributeMerged, HasUnboundedSortedAttributeSingle,
            HasUnboundedSortedOwner,
        },
        has_reverse_executor::{
            HasReverseBoundedSortedOwner, HasReverseUnboundedSortedAttribute, HasReverseUnboundedSortedOwnerMerged,
            HasReverseUnboundedSortedOwnerSingle,
        },
        links_executor::{
            LinksBoundedRelationPlayer, LinksBoundedRelationSortedPlayer, LinksUnboundedSortedPlayerMerged,
            LinksUnboundedSortedPlayerSingle, LinksUnboundedSortedRelation,
        },
        links_reverse_executor::{
            LinksReverseBoundedPlayerRelation, LinksReverseBoundedPlayerSortedRelation,
            LinksReverseUnboundedSortedPlayer, LinksReverseUnboundedSortedRelationMerged,
            LinksReverseUnboundedSortedRelationSingle,
        },
        tuple::{Tuple, TupleIndex, TuplePositions, TupleResult},
        VariableMode, VariableModes,
    },
};

// TODO: the 'check' can deduplicate against all relevant variables as soon as an anonymous variable is no longer relevant.
//       if the deduplicated answer leads to an answer, we should not re-emit it again (we will rediscover the same answers)
//       if the deduplicated answer fails to lead to an answer, we should not re-emit it again as it will fail again

macro_rules! dispatch_tuple_iterator {
    { $(#[$meta:meta])* $vis:vis enum $ident:ident $variants:tt impl $impl:tt } => {
        dispatch_tuple_iterator! { @enum $(#[$meta])* $vis $ident $variants }
        dispatch_tuple_iterator! { @impl $ident $variants $impl }
    };
    { @enum $(#[$meta:meta])* $vis:vis $ident:ident { $($variant:ident ( $inner:ty ) ),+ $(,)? } } => {
        $(#[$meta])*
        $vis enum $ident {
            $($variant ( $inner ) ),+
        }
    };
    { @impl $ident:ident $variants:tt {
        $($fn_vis:vis fn $fn:ident $args:tt $(-> $ret:ty)?; )*
    } } => {
        impl $ident {$(
            dispatch_tuple_iterator! { @impl_one $variants $fn_vis $fn $args $(-> $ret)? }
        )*}
    };
    { @impl_one $variants:tt $fn_vis:vis $fn:ident(&self $(, $arg:ident : $argty:ty)* $(,)?) $(-> $ret:ty)? } => {
        $fn_vis fn $fn(&self $(, $arg: $argty)*) $(-> $ret)? {
             dispatch_tuple_iterator! { @dispatch self $variants $fn ($($arg),*) }
        }
    };
    { @impl_one $variants:tt $fn_vis:vis $fn:ident(&mut self $(, $arg:ident : $argty:ty)* $(,)?) $(-> $ret:ty)? } => {
        $fn_vis fn $fn(&mut self $(, $arg: $argty)*) $(-> $ret)? {
             dispatch_tuple_iterator! { @dispatch self $variants $fn ($($arg),*) }
        }
    };
    { @dispatch $self:ident { $($variant:ident ( $inner:ty ) ),+ $(,)? } $fn:ident $args:tt } => {
        match $self {$(
            Self::$variant(iter) => iter.$fn $args,
        )+}
    };
}

dispatch_tuple_iterator! {
#[allow(clippy::large_enum_variant)]
pub(crate) enum TupleIterator {
    IsaUnboundedSingle(SortedTupleIterator<IsaUnboundedSortedThingSingle>),
    IsaUnboundedMerged(SortedTupleIterator<IsaUnboundedSortedThingMerged>),
    IsaUnboundedInvertedSingle(SortedTupleIterator<IsaUnboundedSortedTypeSingle>),
    IsaUnboundedInvertedMerged(SortedTupleIterator<IsaUnboundedSortedTypeMerged>),
    IsaBounded(SortedTupleIterator<IsaBoundedSortedType>),

    IsaReverseUnboundedSingle(SortedTupleIterator<IsaReverseUnboundedSortedTypeSingle>),
    IsaReverseUnboundedMerged(SortedTupleIterator<IsaReverseUnboundedSortedTypeMerged>),
    IsaReverseUnboundedInvertedSingle(SortedTupleIterator<IsaReverseUnboundedSortedThingSingle>),
    IsaReverseUnboundedInvertedMerged(SortedTupleIterator<IsaReverseUnboundedSortedThingMerged>),
    IsaReverseBounded(SortedTupleIterator<IsaReverseBoundedSortedThing>),

    HasUnbounded(SortedTupleIterator<HasUnboundedSortedOwner>),
    HasUnboundedInvertedSingle(SortedTupleIterator<HasUnboundedSortedAttributeSingle>),
    HasUnboundedInvertedMerged(SortedTupleIterator<HasUnboundedSortedAttributeMerged>),
    HasBounded(SortedTupleIterator<HasBoundedSortedAttribute>),

    HasReverseUnbounded(SortedTupleIterator<HasReverseUnboundedSortedAttribute>),
    HasReverseUnboundedInvertedSingle(SortedTupleIterator<HasReverseUnboundedSortedOwnerSingle>),
    HasReverseUnboundedInvertedMerged(SortedTupleIterator<HasReverseUnboundedSortedOwnerMerged>),
    HasReverseBounded(SortedTupleIterator<HasReverseBoundedSortedOwner>),

    LinksUnbounded(SortedTupleIterator<LinksUnboundedSortedRelation>),
    LinksUnboundedInvertedSingle(SortedTupleIterator<LinksUnboundedSortedPlayerSingle>),
    LinksUnboundedInvertedMerged(SortedTupleIterator<LinksUnboundedSortedPlayerMerged>),
    LinksBoundedRelation(SortedTupleIterator<LinksBoundedRelationSortedPlayer>),
    LinksBoundedRelationPlayer(SortedTupleIterator<LinksBoundedRelationPlayer>),

    LinksReverseUnbounded(SortedTupleIterator<LinksReverseUnboundedSortedPlayer>),
    LinksReverseUnboundedInvertedSingle(SortedTupleIterator<LinksReverseUnboundedSortedRelationSingle>),
    LinksReverseUnboundedInvertedMerged(SortedTupleIterator<LinksReverseUnboundedSortedRelationMerged>),
    LinksReverseBoundedPlayer(SortedTupleIterator<LinksReverseBoundedPlayerSortedRelation>),
    LinksReverseBoundedPlayerRelation(SortedTupleIterator<LinksReverseBoundedPlayerRelation>),
}

impl {
    pub(crate) fn write_values(&mut self, row: &mut Row<'_>);
    pub(crate) fn peek(&mut self) -> Option<&Result<Tuple<'_>, ConceptReadError>>;
    pub(crate) fn advance_past(&mut self) -> Result<usize, ConceptReadError> ;
    fn skip_until_value(
        &mut self,
        index: TupleIndex,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, ConceptReadError> ;
    pub(crate) fn advance_single(&mut self) -> Result<(), ConceptReadError> ;
    pub(crate) fn peek_first_unbound_value(&mut self) -> Option<Result<&VariableValue<'_>, ConceptReadError>> ;
    pub(crate) fn first_unbound_index(&self) -> TupleIndex ;
}
}

impl TupleIterator {
    pub(crate) fn advance_until_index_is(
        &mut self,
        index: TupleIndex,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, ConceptReadError> {
        self.skip_until_value(index, value)
    }
}

pub(crate) trait TupleIteratorAPI {
    fn write_values(&mut self, row: &mut Row<'_>);

    fn peek(&mut self) -> Option<&Result<Tuple<'_>, ConceptReadError>>;

    /// Advance the iterator past the current answer, and return the number duplicate answers were skipped
    fn advance_past(&mut self) -> Result<usize, ConceptReadError>;

    fn advance_single(&mut self) -> Result<(), ConceptReadError>;

    fn positions(&self) -> &TuplePositions;
}

pub(crate) struct SortedTupleIterator<Iterator: for<'a> LendingIterator<Item<'a> = TupleResult<'a>>> {
    iterator: Peekable<Iterator>,
    positions: TuplePositions,
    tuple_length: usize,
    first_unbound: TupleIndex,
    last_enumerated: Option<TupleIndex>,
    last_enumerated_or_counted: TupleIndex,
}

impl<Iterator: for<'a> LendingIterator<Item<'a> = TupleResult<'a>>> SortedTupleIterator<Iterator> {
    pub(crate) fn new(iterator: Iterator, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> Self {
        let first_unbound = first_unbound(variable_modes, &tuple_positions);
        let last_enumerated = last_enumerated(variable_modes, &tuple_positions);
        let last_enumerated_or_counted = last_enumerated_or_counted(variable_modes, &tuple_positions).unwrap();
        Self {
            iterator: Peekable::new(iterator),
            tuple_length: tuple_positions.len(),
            positions: tuple_positions,
            first_unbound,
            last_enumerated,
            last_enumerated_or_counted,
        }
    }

    fn count_until_changes(&mut self, range: RangeInclusive<usize>) -> Result<usize, ConceptReadError> {
        debug_assert!(self.peek().is_some() && !range.is_empty());

        if range.end() == &self.tuple_length {
            self.advance_single()?;
            return Ok(1);
        }

        let current = self.peek().unwrap().clone()?.into_owned();
        let current_range = &current.values()[range.clone()];
        self.iterator.next().unwrap()?;
        let mut count = 1;
        loop {
            let peek = self.iterator.peek();
            match peek {
                None => return Ok(count),
                Some(Ok(tuple)) => {
                    let values = &tuple.values()[range.clone()];
                    if values != current_range {
                        return Ok(count);
                    } else {
                        count += 1;
                        self.iterator.next().unwrap()?;
                    }
                }
                Some(Err(err)) => return Err(err.clone()),
            }
        }
    }

    fn first_unbound_index(&self) -> TupleIndex {
        self.first_unbound
    }

    fn skip_until_changes(&mut self, range: RangeInclusive<usize>) -> Result<(), ConceptReadError> {
        // TODO: this should be optimisable with seek(to peek[index].increment())
        self.count_until_changes(range)?;
        Ok(())
    }

    fn skip_until_value(
        &mut self,
        index: TupleIndex,
        target: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, ConceptReadError> {
        // TODO: this should use seek if index == self.first_unbound()
        loop {
            let peek = self.peek();
            match peek {
                None => return Ok(None),
                Some(Ok(tuple)) => {
                    let value = &tuple.values()[index as usize];
                    match value.partial_cmp(target).unwrap() {
                        Ordering::Less => self.advance_single()?,
                        Ordering::Equal => return Ok(Some(Ordering::Equal)),
                        Ordering::Greater => return Ok(Some(Ordering::Greater)),
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
        self.peek()
            .map(|result| result.as_ref().map(|tuple| &tuple.values()[index as usize]).map_err(|err| err.clone()))
    }
}

impl<Iterator: for<'a> LendingIterator<Item<'a> = TupleResult<'a>>> TupleIteratorAPI for SortedTupleIterator<Iterator> {
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

        if self.last_enumerated == Some(self.last_enumerated_or_counted) {
            let last = self.last_enumerated_or_counted as usize;
            self.skip_until_changes(0..=last)?;
            Ok(1)
        } else if let Some(last_enumerated) = self.last_enumerated {
            let mut count = 1;
            let current = self.peek().unwrap().clone()?.into_owned();
            let enumerated = &current.values()[0..=last_enumerated as usize];
            loop {
                // TODO: this feels inefficient since each skip() call does a copy of the current tuple
                self.skip_until_changes(0..=self.last_enumerated_or_counted as usize)?;
                match self.iterator.peek() {
                    None => return Ok(count),
                    Some(Ok(tuple)) => {
                        let tuple_range = &tuple.values()[0..=last_enumerated as usize];
                        if tuple_range != enumerated {
                            return Ok(count);
                        } else {
                            count += 1;
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
            }
        } else if self.last_enumerated_or_counted as usize == self.tuple_length {
            return Ok(self.iterator.count_as_ref());
        } else {
            let mut count = 1;
            // TODO: this feels inefficient since each skip() call does a copy of the current tuple
            while self.peek().is_some() {
                self.skip_until_changes(0..=self.last_enumerated_or_counted as usize)?;
                count += 1;
            }
            return Ok(count);
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

fn first_unbound(variable_modes: &VariableModes, positions: &TuplePositions) -> TupleIndex {
    for (i, position) in positions.iter().enumerate() {
        if variable_modes.get(*position).unwrap() != &VariableMode::Input {
            return i as TupleIndex;
        }
    }
    panic!("No unbound variable found")
}

fn last_enumerated(variable_modes: &VariableModes, positions: &TuplePositions) -> Option<TupleIndex> {
    (positions.iter().enumerate())
        .filter(|&(_, &position)| {
            matches!(variable_modes.get(position), Some(VariableMode::Input | VariableMode::Output))
        })
        .map(|(i, _)| i as TupleIndex)
        .last()
}

fn last_enumerated_or_counted(variable_modes: &VariableModes, positions: &TuplePositions) -> Option<TupleIndex> {
    (positions.iter().enumerate())
        .filter(|&(_, &position)| {
            matches!(
                variable_modes.get(position),
                Some(VariableMode::Input | VariableMode::Output | VariableMode::Count)
            )
        })
        .map(|(i, _)| i as TupleIndex)
        .last()
}

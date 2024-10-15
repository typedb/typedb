/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, iter::Iterator, ops::Range};

use answer::variable_value::VariableValue;
use compiler::{
    executable::match_::instructions::{VariableMode, VariableModes},
    VariablePosition,
};
use concept::error::ConceptReadError;
use itertools::zip_eq;
use lending_iterator::{LendingIterator, Peekable};

use crate::{
    instruction::{
        constant_executor::ConstantValueIterator,
        has_executor::{
            HasBoundedSortedAttribute, HasUnboundedSortedAttributeMerged, HasUnboundedSortedAttributeSingle,
            HasUnboundedSortedOwner,
        },
        has_reverse_executor::{
            HasReverseBoundedSortedOwner, HasReverseUnboundedSortedAttribute, HasReverseUnboundedSortedOwnerMerged,
            HasReverseUnboundedSortedOwnerSingle,
        },
        isa_executor::{
            IsaBoundedSortedType, IsaUnboundedSortedThingMerged, IsaUnboundedSortedThingSingle,
            IsaUnboundedSortedTypeMerged, IsaUnboundedSortedTypeSingle,
        },
        isa_reverse_executor::{
            IsaReverseBoundedSortedThing, IsaReverseUnboundedSortedThingMerged, IsaReverseUnboundedSortedThingSingle,
            IsaReverseUnboundedSortedTypeMerged, IsaReverseUnboundedSortedTypeSingle,
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
        owns_executor::{OwnsBoundedSortedAttribute, OwnsUnboundedSortedOwner},
        owns_reverse_executor::{OwnsReverseBoundedSortedOwner, OwnsReverseUnboundedSortedAttribute},
        plays_executor::{PlaysBoundedSortedRole, PlaysUnboundedSortedPlayer},
        plays_reverse_executor::{PlaysReverseBoundedSortedPlayer, PlaysReverseUnboundedSortedRole},
        relates_executor::{RelatesBoundedSortedRole, RelatesUnboundedSortedRelation},
        relates_reverse_executor::{RelatesReverseBoundedSortedRelation, RelatesReverseUnboundedSortedRole},
        sub_executor::{SubBoundedSortedSuper, SubUnboundedSortedSub},
        sub_reverse_executor::{SubReverseBoundedSortedSuper, SubReverseUnboundedSortedSub},
        tuple::{Tuple, TupleIndex, TuplePositions, TupleResult},
        type_list_executor::TypeIterator,
    },
    row::Row,
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
    Type(SortedTupleIterator<TypeIterator>),

    SubUnbounded(SortedTupleIterator<SubUnboundedSortedSub>),
    SubBounded(SortedTupleIterator<SubBoundedSortedSuper>),

    SubReverseUnbounded(SortedTupleIterator<SubReverseUnboundedSortedSub>),
    SubReverseBounded(SortedTupleIterator<SubReverseBoundedSortedSuper>),

    OwnsUnbounded(SortedTupleIterator<OwnsUnboundedSortedOwner>),
    OwnsBounded(SortedTupleIterator<OwnsBoundedSortedAttribute>),

    OwnsReverseUnbounded(SortedTupleIterator<OwnsReverseUnboundedSortedAttribute>),
    OwnsReverseBounded(SortedTupleIterator<OwnsReverseBoundedSortedOwner>),

    RelatesUnbounded(SortedTupleIterator<RelatesUnboundedSortedRelation>),
    RelatesBounded(SortedTupleIterator<RelatesBoundedSortedRole>),

    RelatesReverseUnbounded(SortedTupleIterator<RelatesReverseUnboundedSortedRole>),
    RelatesReverseBounded(SortedTupleIterator<RelatesReverseBoundedSortedRelation>),

    PlaysUnbounded(SortedTupleIterator<PlaysUnboundedSortedPlayer>),
    PlaysBounded(SortedTupleIterator<PlaysBoundedSortedRole>),

    PlaysReverseUnbounded(SortedTupleIterator<PlaysReverseUnboundedSortedRole>),
    PlaysReverseBounded(SortedTupleIterator<PlaysReverseBoundedSortedPlayer>),

    Constant(SortedTupleIterator<ConstantValueIterator>),

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
    last_enumerated_or_counted: Option<TupleIndex>,
}

impl<Iterator: for<'a> LendingIterator<Item<'a> = TupleResult<'a>>> SortedTupleIterator<Iterator> {
    pub(crate) fn new(iterator: Iterator, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> Self {
        // assumption: items in tuple are ordered as:
        //      inputs, outputs, counted, checked
        #[cfg(debug_assertions)]
        {
            let mut expected_mode = VariableMode::Input;
            for pos in tuple_positions.positions() {
                let &Some(pos) = pos else { continue };
                match variable_modes.get(pos) {
                    Some(VariableMode::Input) => debug_assert_eq!(expected_mode, VariableMode::Input),
                    Some(VariableMode::Output) => {
                        debug_assert!(matches!(expected_mode, VariableMode::Input | VariableMode::Output));
                        expected_mode = VariableMode::Output;
                    }
                    Some(VariableMode::Count) => {
                        debug_assert!(matches!(
                            expected_mode,
                            VariableMode::Input | VariableMode::Output | VariableMode::Count
                        ));
                        expected_mode = VariableMode::Count;
                    }
                    Some(VariableMode::Check) => expected_mode = VariableMode::Check,
                    None => (),
                }
            }
        }

        let first_unbound = first_unbound(variable_modes, &tuple_positions);
        let last_enumerated = last_enumerated(variable_modes, &tuple_positions);
        let last_enumerated_or_counted = last_enumerated_or_counted(variable_modes, &tuple_positions);
        Self {
            iterator: Peekable::new(iterator),
            tuple_length: tuple_positions.len(),
            positions: tuple_positions,
            first_unbound,
            last_enumerated,
            last_enumerated_or_counted,
        }
    }

    fn first_unbound_index(&self) -> TupleIndex {
        self.first_unbound
    }

    fn count_until_enumerated_changes(&mut self) -> Result<usize, ConceptReadError> {
        let Some(last_enumerated) = self.last_enumerated else {
            unreachable!("this should only be called if the tuple contains enumerated variables")
        };
        let past_enumerated_or_counted_index = self.last_enumerated_or_counted.map_or(0, |i| i as usize + 1);
        let mut count = 1;
        let current = self.peek().unwrap().clone()?.into_owned();
        let enumerated = &current.values()[0..=last_enumerated as usize];
        loop {
            // TODO: this feels inefficient since each skip() call does a copy of the current tuple
            self.skip_until_changes(0..past_enumerated_or_counted_index)?;
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
    }

    fn skip_until_changes(&mut self, range: Range<usize>) -> Result<(), ConceptReadError> {
        // TODO: this should be optimisable with seek(to peek[index].increment())
        debug_assert!(self.peek().is_some());

        if range.end == self.tuple_length {
            self.advance_single()?;
            return Ok(());
        }

        let current = self.peek().unwrap().clone()?.into_owned();
        let current_range = &current.values()[range.clone()];
        self.iterator.next().unwrap()?;
        loop {
            let peek = self.iterator.peek();
            match peek {
                None => return Ok(()),
                Some(Ok(tuple)) => {
                    let values = &tuple.values()[range.clone()];
                    if values != current_range {
                        return Ok(());
                    } else {
                        self.iterator.next().unwrap()?;
                    }
                }
                Some(Err(err)) => return Err(err.clone()),
            }
        }
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

    fn all_counted(&mut self) -> bool {
        self.last_enumerated_or_counted == Some(self.tuple_length as u16)
    }

    fn all_checked(&mut self) -> bool {
        self.last_enumerated_or_counted.is_none()
    }

    fn any_enumerated(&mut self) -> bool {
        self.last_enumerated.is_some()
    }

    fn no_counted(&mut self) -> bool {
        self.last_enumerated == self.last_enumerated_or_counted
    }
}

impl<Iterator: for<'a> LendingIterator<Item<'a> = TupleResult<'a>>> TupleIteratorAPI for SortedTupleIterator<Iterator> {
    fn write_values(&mut self, row: &mut Row<'_>) {
        debug_assert!(self.peek().is_some() && self.peek().unwrap().is_ok());
        // note: can't use self.peek() since it will cause mut and immutable reference to self
        let tuple = self.iterator.peek().unwrap().as_ref().unwrap();

        fn relevant_values<'a, 'b>(
            (&pos, value): (&Option<VariablePosition>, &'a VariableValue<'b>),
        ) -> Option<(VariablePosition, &'a VariableValue<'b>)> {
            Some((pos?, value))
        }

        match tuple {
            Tuple::Single(values) => {
                for (pos, value) in zip_eq(self.positions.as_single(), values).filter_map(relevant_values) {
                    row.set(pos, value.clone().into_owned());
                }
            }
            Tuple::Pair(values) => {
                for (pos, value) in zip_eq(self.positions.as_pair(), values).filter_map(relevant_values) {
                    row.set(pos, value.clone().into_owned());
                }
            }
            Tuple::Triple(values) => {
                for (pos, value) in zip_eq(self.positions.as_triple(), values).filter_map(relevant_values) {
                    row.set(pos, value.clone().into_owned());
                }
            }
            Tuple::Quintuple(values) => {
                for (pos, value) in zip_eq(self.positions.as_quintuple(), values).filter_map(relevant_values) {
                    row.set(pos, value.clone().into_owned());
                }
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

        let past_enumerated_or_counted_index = self.last_enumerated_or_counted.map_or(0, |i| i as usize + 1);

        if self.no_counted() {
            self.skip_until_changes(0..past_enumerated_or_counted_index)?;
            Ok(1)
        } else if self.any_enumerated() {
            self.count_until_enumerated_changes()
        } else if self.all_counted() {
            Ok(self.iterator.count_as_ref())
        } else {
            debug_assert!(self.all_checked());
            let mut count = 1;
            // TODO: this feels inefficient since each skip() call does a copy of the current tuple
            while self.peek().is_some() {
                self.skip_until_changes(0..past_enumerated_or_counted_index)?;
                count += 1;
            }
            Ok(count)
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
        if let Some(position) = position {
            if variable_modes.get(position).unwrap() != &VariableMode::Input {
                return i as TupleIndex;
            }
        }
    }
    panic!("No unbound variable found")
}

fn last_enumerated(variable_modes: &VariableModes, positions: &TuplePositions) -> Option<TupleIndex> {
    (positions.iter().enumerate())
        .filter(|&(_, position)| {
            matches!(position.and_then(|p| variable_modes.get(p)), Some(VariableMode::Input | VariableMode::Output))
        })
        .map(|(i, _)| i as TupleIndex)
        .last()
}

fn last_enumerated_or_counted(variable_modes: &VariableModes, positions: &TuplePositions) -> Option<TupleIndex> {
    (positions.iter().enumerate())
        .filter(|&(_, position)| {
            matches!(
                position.and_then(|p| variable_modes.get(p)),
                Some(VariableMode::Input | VariableMode::Output | VariableMode::Count)
            )
        })
        .map(|(i, _)| i as TupleIndex)
        .last()
}

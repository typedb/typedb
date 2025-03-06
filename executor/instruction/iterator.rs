/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, iter};

use answer::variable_value::VariableValue;
use compiler::{
    executable::match_::instructions::{VariableMode, VariableModes},
    ExecutorVariable, VariablePosition,
};
use concept::error::ConceptReadError;
use itertools::zip_eq;

use crate::{
    instruction::{
        has_executor::{HasUnboundedTupleIteratorMerged, HasUnboundedTupleIteratorSingle},
        has_reverse_executor::{
            HasReverseTupleIteratorChained, HasReverseTupleIteratorSingle, HasReverseUnboundedSortedOwnerMerged,
        },
        iid_executor::IidIterator,
        indexed_relation_executor::{IndexedRelationTupleIteratorMerged, IndexedRelationTupleIteratorSingle},
        is_executor::IsIterator,
        isa_executor::{IsaBoundedSortedType, IsaUnboundedSortedThing},
        isa_reverse_executor::{IsaReverseBoundedSortedThing, IsaReverseUnboundedSortedType},
        links_executor::{LinksTupleIteratorMerged, LinksTupleIteratorSingle},
        owns_executor::{OwnsBoundedSortedAttribute, OwnsUnboundedSortedOwner},
        owns_reverse_executor::{OwnsReverseBoundedSortedOwner, OwnsReverseUnboundedSortedAttribute},
        plays_executor::{PlaysBoundedSortedRole, PlaysUnboundedSortedPlayer},
        plays_reverse_executor::{PlaysReverseBoundedSortedPlayer, PlaysReverseUnboundedSortedRole},
        relates_executor::{RelatesBoundedSortedRole, RelatesUnboundedSortedRelation},
        relates_reverse_executor::{RelatesReverseBoundedSortedRelation, RelatesReverseUnboundedSortedRole},
        sub_executor::{SubBoundedSortedSuper, SubUnboundedSortedSub},
        sub_reverse_executor::{SubReverseBoundedSortedSub, SubReverseUnboundedSortedSuper},
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
    Empty(SortedTupleIterator<iter::Empty<TupleResult<'static>>>),
    Check(SortedTupleIterator<<Option<TupleResult<'static>> as IntoIterator>::IntoIter>),

    Is(SortedTupleIterator<IsIterator>),
    Iid(SortedTupleIterator<IidIterator>),
    Type(SortedTupleIterator<TypeIterator>),

    SubUnbounded(SortedTupleIterator<SubUnboundedSortedSub>),
    SubBounded(SortedTupleIterator<SubBoundedSortedSuper>),

    SubReverseUnbounded(SortedTupleIterator<SubReverseUnboundedSortedSuper>),
    SubReverseBounded(SortedTupleIterator<SubReverseBoundedSortedSub>),

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

    IsaUnbounded(SortedTupleIterator<IsaUnboundedSortedThing>),
    IsaBounded(SortedTupleIterator<IsaBoundedSortedType>),

    IsaReverseUnbounded(SortedTupleIterator<IsaReverseUnboundedSortedType>),
    IsaReverseBounded(SortedTupleIterator<IsaReverseBoundedSortedThing>),

    HasSingle(SortedTupleIterator<HasUnboundedTupleIteratorSingle>),
    HasMerged(SortedTupleIterator<HasUnboundedTupleIteratorMerged>),

    HasReverseSingle(SortedTupleIterator<HasReverseTupleIteratorSingle>),
    HasReverseChained(SortedTupleIterator<HasReverseTupleIteratorChained>),
    HasReverseMerged(SortedTupleIterator<HasReverseUnboundedSortedOwnerMerged>),

    LinksSingle(SortedTupleIterator<LinksTupleIteratorSingle>),
    LinksMerged(SortedTupleIterator<LinksTupleIteratorMerged>),

    LinksReverseSingle(SortedTupleIterator<LinksTupleIteratorSingle>),
    LinksReverseMerged(SortedTupleIterator<LinksTupleIteratorMerged>),

    IndexedRelationsSingle(SortedTupleIterator<IndexedRelationTupleIteratorSingle>),
    IndexedRelationsMerged(SortedTupleIterator<IndexedRelationTupleIteratorMerged>),
}

impl {
    pub(crate) fn write_values(&mut self, row: &mut Row<'_>);
    pub(crate) fn peek(&mut self) -> Option<&Result<Tuple<'_>, Box<ConceptReadError>>>;
    pub(crate) fn advance_past(&mut self) -> Result<usize, Box<ConceptReadError>>;
    fn skip_until_value(
        &mut self,
        index: TupleIndex,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, Box<ConceptReadError>>;
    pub(crate) fn advance_single(&mut self) -> Result<(), Box<ConceptReadError>>;
    pub(crate) fn peek_first_unbound_value(&mut self) -> Option<Result<&VariableValue<'_>, Box<ConceptReadError>>>;
    pub(crate) fn first_unbound_index(&self) -> TupleIndex ;
}
}

impl TupleIterator {
    pub(crate) fn empty() -> Self {
        Self::Empty(SortedTupleIterator::empty())
    }

    pub(crate) fn advance_until_index_is(
        &mut self,
        index: TupleIndex,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, Box<ConceptReadError>> {
        self.skip_until_value(index, value)
    }
}

pub(crate) trait TupleIteratorAPI {
    fn write_values(&mut self, row: &mut Row<'_>);

    fn peek(&mut self) -> Option<&Result<Tuple<'_>, Box<ConceptReadError>>>;

    /// Advance the iterator past the current answer, and return the number duplicate answers were skipped
    fn advance_past(&mut self) -> Result<usize, Box<ConceptReadError>>;

    fn advance_single(&mut self) -> Result<(), Box<ConceptReadError>>;

    fn positions(&self) -> &TuplePositions;
}

pub(crate) struct SortedTupleIterator<It: Iterator<Item = TupleResult<'static>>> {
    iterator: iter::Peekable<iter::Inspect<It, Box<dyn FnMut(&TupleResult<'_>)>>>,
    positions: TuplePositions,
    tuple_length: usize,
    first_unbound: TupleIndex,
    last_enumerated: Option<TupleIndex>,
    last_enumerated_or_counted: Option<TupleIndex>,
}

impl SortedTupleIterator<iter::Empty<TupleResult<'static>>> {
    fn empty() -> Self {
        Self {
            iterator: iter::empty().inspect(Box::new(|_: &TupleResult<'_>| ()) as _).peekable(),
            positions: TuplePositions::Single([None]),
            tuple_length: 0,
            first_unbound: 0,
            last_enumerated: None,
            last_enumerated_or_counted: None,
        }
    }
}

impl<It: Iterator<Item = TupleResult<'static>>> SortedTupleIterator<It> {
    pub(crate) fn new(iterator: It, tuple_positions: TuplePositions, variable_modes: &VariableModes) -> Self {
        // assumption: items in tuple are ordered as:
        //      (sort?), inputs, outputs, counted, checked
        #[cfg(debug_assertions)]
        {
            let mut expected_mode = VariableMode::Input;
            // [0] may have any mode
            for pos in &tuple_positions.positions()[1..] {
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

        let iterator = iterator.inspect({
            let mut prev: Option<Tuple<'static>> = None;
            Box::new(move |item: &TupleResult<'_>| {
                #[cfg(debug_assertions)]
                if let Ok(tuple) = item {
                    if let Some(prev) = &prev {
                        debug_assert!(
                            prev <= tuple,
                            "{prev:?} <= {tuple:?}: sortedness check fail in {}",
                            std::any::type_name::<Self>()
                        );
                    }
                    prev = Some(tuple.clone().into_owned());
                }
            }) as _
        });

        Self {
            iterator: iterator.peekable(),
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

    fn count_until_enumerated_changes(&mut self) -> Result<usize, Box<ConceptReadError>> {
        let Some(last_enumerated) = self.last_enumerated else {
            unreachable!("this should only be called if the tuple contains enumerated variables")
        };
        let past_enumerated_or_counted_index = self.last_enumerated_or_counted.map_or(0, |i| i as usize + 1);
        let mut count = 1;
        let current = self.peek().unwrap().clone()?.into_owned();
        let enumerated = &current.values()[0..=last_enumerated as usize];
        loop {
            // TODO: this feels inefficient since each skip() call does a copy of the current tuple
            self.skip_until_changes(past_enumerated_or_counted_index)?;
            let peek = self.iterator.peek();
            match peek {
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

    fn skip_until_changes(&mut self, change_width: usize) -> Result<(), Box<ConceptReadError>> {
        // TODO: this should be optimisable with seek(to peek[index].increment())
        debug_assert!(self.peek().is_some());

        if change_width == self.tuple_length {
            self.advance_single()?;
            return Ok(());
        }

        let end = usize::max(1, change_width); // at least the sort variable must be checked

        let current = self.peek().unwrap().clone()?.into_owned();
        let current_range = &current.values()[0..end];
        self.iterator.next().unwrap()?;
        loop {
            let peek = self.iterator.peek();
            match peek {
                None => return Ok(()),
                Some(Ok(tuple)) => {
                    let values = &tuple.values()[0..end];
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
    ) -> Result<Option<Ordering>, Box<ConceptReadError>> {
        // TODO: this should use seek if index == self.first_unbound()
        loop {
            match self.peek() {
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

    fn peek_first_unbound_value(&mut self) -> Option<Result<&VariableValue<'_>, Box<ConceptReadError>>> {
        self.peek_current_value_at(self.first_unbound)
    }

    fn peek_current_value_at(
        &mut self,
        index: TupleIndex,
    ) -> Option<Result<&VariableValue<'_>, Box<ConceptReadError>>> {
        self.peek()
            .map(|result| result.as_ref().map(|tuple| &tuple.values()[index as usize]).map_err(|err| err.clone()))
    }

    fn all_counted(&mut self) -> bool {
        self.last_enumerated_or_counted == Some((self.tuple_length - 1) as u16)
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

impl<It: Iterator<Item = TupleResult<'static>>> TupleIteratorAPI for SortedTupleIterator<It> {
    fn write_values(&mut self, row: &mut Row<'_>) {
        debug_assert!(self.peek().is_some() && self.peek().unwrap().is_ok());
        // note: can't use self.peek() since it will cause mut and immutable reference to self
        let tuple = self.iterator.peek().unwrap().as_ref().unwrap();

        fn relevant_values<'a, 'b>(
            (&pos, value): (&Option<ExecutorVariable>, &'a VariableValue<'b>),
        ) -> Option<(VariablePosition, &'a VariableValue<'b>)> {
            Some((pos?.as_position()?, value))
        }

        for (pos, value) in zip_eq(self.positions.positions(), tuple.values()).filter_map(relevant_values) {
            if pos.as_usize() < row.len() {
                // TODO either keep this or used selected varables
                row.set(pos, value.clone().into_owned());
            }
        }
    }

    fn peek(&mut self) -> Option<&Result<Tuple<'_>, Box<ConceptReadError>>> {
        self.iterator.peek()
    }

    fn advance_past(&mut self) -> Result<usize, Box<ConceptReadError>> {
        debug_assert!(self.peek().is_some());

        let past_enumerated_or_counted_index = self.last_enumerated_or_counted.map_or(0, |i| i as usize + 1);

        if self.no_counted() {
            self.skip_until_changes(past_enumerated_or_counted_index)?;
            Ok(1)
        } else if self.any_enumerated() {
            self.count_until_enumerated_changes()
        } else if self.all_counted() {
            Ok(self.iterator.by_ref().count())
        } else {
            let mut count = 1;
            // TODO: this feels inefficient since each skip() call does a copy of the current tuple
            while self.peek().is_some() {
                self.skip_until_changes(past_enumerated_or_counted_index)?;
                count += 1;
            }
            Ok(count)
        }
    }

    fn advance_single(&mut self) -> Result<(), Box<ConceptReadError>> {
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
            if variable_modes.get(position).unwrap() != VariableMode::Input {
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

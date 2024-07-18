/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashSet};

use answer::{variable_value::VariableValue, Thing};
use concept::{
    error::ConceptReadError,
    thing::{
        attribute::AttributeIterator, entity::EntityIterator, has::Has, object::HasAttributeIterator,
        relation::RelationIterator,
    },
};
use lending_iterator::{LendingIterator, Peekable};
use tracing::warn;

use crate::executor::{
    batch::{ImmutableRow, Row},
    instruction::{
        has_executor::{
            HasBoundedSortedAttributeIterator, HasUnboundedSortedAttributeMergedIterator,
            HasUnboundedSortedAttributeSingleIterator, HasUnboundedSortedOwnerIterator, HasVariableModes,
        },
        isa_executor::IsaVariableModes,
        iterator_advance::{
            counting_advance_attribute_iterator, counting_advance_entity_iterator,
            counting_advance_has_bounded_sorted_attribute_iterator,
            counting_advance_has_unbounded_sorted_attribute_merged_iterator,
            counting_advance_has_unbounded_sorted_attribute_single_iterator,
            counting_advance_has_unbounded_sorted_owner_iterator, counting_advance_relation_iterator,
        },
        VariableMode,
    },
    Position,
};

pub(crate) enum HasSortedAttributeIterator {
    // Unbounded()
    UnboundedMerged(Peekable<HasUnboundedSortedAttributeMergedIterator>),
    UnboundedSingle(Peekable<HasUnboundedSortedAttributeSingleIterator>),
    Bounded(Peekable<HasBoundedSortedAttributeIterator>),
}

impl HasSortedAttributeIterator {
    fn peek(&mut self) -> Option<&Result<(Has, u64), ConceptReadError>> {
        match self {
            HasSortedAttributeIterator::UnboundedMerged(iter) => iter.peek(),
            HasSortedAttributeIterator::UnboundedSingle(iter) => iter.peek(),
            HasSortedAttributeIterator::Bounded(iter) => iter.peek(),
        }
    }

    fn peek_sorted_value(&mut self) -> Option<Result<VariableValue<'_>, &ConceptReadError>> {
        match self {
            HasSortedAttributeIterator::UnboundedMerged(iter) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            HasSortedAttributeIterator::UnboundedSingle(iter) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            HasSortedAttributeIterator::Bounded(iter) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
        }
    }

    fn counting_skip_to_sorted_value(
        &mut self,
        value: &VariableValue<'_>,
    ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
        match self {
            HasSortedAttributeIterator::UnboundedMerged(iter) => {
                counting_advance_has_unbounded_sorted_attribute_merged_iterator(iter, value)
            }
            HasSortedAttributeIterator::UnboundedSingle(iter) => {
                counting_advance_has_unbounded_sorted_attribute_single_iterator(iter, value)
            }
            HasSortedAttributeIterator::Bounded(iter) => {
                counting_advance_has_bounded_sorted_attribute_iterator(iter, value)
            }
        }
    }

    fn advance_single(&mut self) -> Result<(), ConceptReadError> {
        match self {
            HasSortedAttributeIterator::UnboundedMerged(iter) => iter.next().unwrap()?,
            HasSortedAttributeIterator::UnboundedSingle(iter) => iter.next().unwrap()?,
            HasSortedAttributeIterator::Bounded(iter) => iter.next().unwrap()?,
        };
        Ok(())
    }

    fn count(&mut self) -> usize {
        match self {
            HasSortedAttributeIterator::UnboundedMerged(iter) => iter.count_as_ref(),
            HasSortedAttributeIterator::UnboundedSingle(iter) => iter.count_as_ref(),
            HasSortedAttributeIterator::Bounded(iter) => iter.count_as_ref(),
        }
    }
}

pub(crate) enum HasSortedOwnerIterator {
    Unbounded(Peekable<HasUnboundedSortedOwnerIterator>),
}

impl HasSortedOwnerIterator {
    fn peek(&mut self) -> Option<&Result<(Has, u64), ConceptReadError>> {
        match self {
            Self::Unbounded(iter) => iter.peek(),
        }
    }

    fn peek_sorted_value(&mut self) -> Option<Result<VariableValue<'_>, &ConceptReadError>> {
        match self {
            Self::Unbounded(iter) => {
                iter.peek().map(|result| result.as_ref().map(|(has, count)| VariableValue::Thing(has.owner().into())))
            }
        }
    }

    fn counting_skip_to_sorted_value(
        &mut self,
        value: &VariableValue<'_>,
    ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
        match self {
            Self::Unbounded(iter) => counting_advance_has_unbounded_sorted_owner_iterator(iter, value),
        }
    }

    fn advance_single(&mut self) -> Result<(), ConceptReadError> {
        match self {
            Self::Unbounded(iter) => iter.next().unwrap()?,
        };
        Ok(())
    }

    fn count(&mut self) -> usize {
        match self {
            HasSortedOwnerIterator::Unbounded(iter) => iter.count_as_ref(),
        }
    }
}

pub(crate) enum InstructionIterator {
    IsaEntitySortedThing(
        Peekable<EntityIterator>,
        ir::pattern::constraint::Isa<Position>,
        IsaVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    IsaRelationSortedThing(
        Peekable<RelationIterator>,
        ir::pattern::constraint::Isa<Position>,
        IsaVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    IsaAttributeSortedThing(
        Peekable<AttributeIterator>,
        ir::pattern::constraint::Isa<Position>,
        IsaVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),

    HasSortedAttribute(
        HasSortedAttributeIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    HasSortedOwner(
        HasSortedOwnerIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
}

impl InstructionIterator {
    pub(crate) fn peek_sorted_value_equals(&mut self, value: &VariableValue<'_>) -> Result<bool, ConceptReadError> {
        Ok(self.peek_sorted_value().transpose().map_err(|err| err.clone())?.is_some_and(|peek| peek == *value))
    }

    pub(crate) fn peek_sorted_value(&mut self) -> Option<Result<VariableValue<'_>, &ConceptReadError>> {
        debug_assert!(self.is_sorted());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => iter
                .peek()
                .map(|result| result.as_ref().map(|entity| VariableValue::Thing(entity.as_reference().into()))),
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => iter
                .peek()
                .map(|result| result.as_ref().map(|relation| VariableValue::Thing(relation.as_reference().into()))),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => iter
                .peek()
                .map(|result| result.as_ref().map(|attribute| VariableValue::Thing(attribute.as_reference().into()))),
            InstructionIterator::HasSortedAttribute(iter, _, _, _) => iter.peek_sorted_value(),
            InstructionIterator::HasSortedOwner(iter, _, _, _) => iter.peek_sorted_value(),
        }
    }

    pub(crate) fn counting_skip_to_sorted_value(
        &mut self,
        value: &VariableValue<'_>,
    ) -> Result<(usize, Option<Ordering>), ConceptReadError> {
        debug_assert!(self.is_sorted());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => counting_advance_entity_iterator(iter, value),
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => {
                counting_advance_relation_iterator(iter, value)
            }
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => {
                counting_advance_attribute_iterator(iter, value)
            }
            InstructionIterator::HasSortedAttribute(iter, _, _, _) => iter.counting_skip_to_sorted_value(value),
            InstructionIterator::HasSortedOwner(iter, _, _, _) => iter.counting_skip_to_sorted_value(value),
        }
    }

    pub(crate) fn advance_single(&mut self) -> Result<(), ConceptReadError> {
        assert!(self.has_value());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::HasSortedAttribute(iter, _, _, _) => return iter.advance_single(),
            InstructionIterator::HasSortedOwner(iter, _, _, _) => return iter.advance_single(),
        }
        Ok(())
    }

    pub(crate) fn count_until_next_answer(&mut self, answer_row: ImmutableRow) -> Result<usize, ConceptReadError> {
        assert!(self.has_value());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => {
                todo!()
            }
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => {
                todo!()
            }
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => {
                todo!()
            }
            InstructionIterator::HasSortedAttribute(iter, has, variable_modes, dedup) => {
                Self::count_until_next_answer_has_sorted_attribute(answer_row, iter, has, *variable_modes, dedup)
            }
            InstructionIterator::HasSortedOwner(iter, has, variable_modes, dedup) => {
                Self::count_until_next_answer_has_sorted_owner(answer_row, iter, has, *variable_modes, dedup)
            }
        }
    }

    fn count_until_next_answer_has_sorted_owner(
        answer_row: ImmutableRow,
        iterator: &mut HasSortedOwnerIterator,
        has: &ir::pattern::constraint::Has<Position>,
        variable_modes: HasVariableModes,
        mut deduplication_set: &mut Option<HashSet<VariableValue<'static>>>,
    ) -> Result<usize, ConceptReadError> {
        match (variable_modes.owner(), variable_modes.attribute()) {
            (VariableMode::UnboundSelect, VariableMode::UnboundSelect)
            | (VariableMode::BoundSelect, VariableMode::UnboundSelect) => {
                iterator.advance_single()?;
                Ok(1)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCount) => {
                let target = answer_row.get(has.owner()).next_possible();
                let (count, _) = iterator.counting_skip_to_sorted_value(&target)?;
                Ok(count)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCheck) => {
                let target = answer_row.get(has.owner()).next_possible();
                // TODO: replace with seek()
                let (_, _) = iterator.counting_skip_to_sorted_value(&target)?;
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundSelect) => {
                iterator.advance_single()?;
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundCount)
            | (VariableMode::BoundSelect, VariableMode::UnboundCount) => Ok(iterator.count()),
            (VariableMode::UnboundCount, VariableMode::UnboundCheck) => {
                let mut count = 1;
                iterator.advance_single()?;
                let mut target = answer_row.get(has.owner()).next_possible();
                // TODO: replace with seek()
                while iterator.counting_skip_to_sorted_value(&target)?.1.is_some() {
                    count += 1;
                    // TODO: incorporate has count
                    let (has, count) = iterator.peek().unwrap().as_ref().unwrap();
                    target = VariableValue::Thing(Thing::from(has.owner()).next_possible());
                }
                Ok(count)
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundSelect) => {
                warn!(
                    "Sorted variable Check and unsorted variable Select is unperformant as it requires deduplicating."
                );
                if deduplication_set.is_none() {
                    *deduplication_set = Some(HashSet::new());
                }
                let dedup = deduplication_set.as_mut().unwrap();
                loop {
                    let peek = iterator.peek();
                    match peek {
                        None => return Ok(1),
                        Some(Ok((has, count))) => {
                            // TODO: handle has count
                            let attribute: Thing<'static> = has.attribute().clone().into_owned().into();
                            let new_element = dedup.insert(VariableValue::Thing(attribute));
                            if new_element {
                                return Ok(1);
                            } else {
                                iterator.advance_single()?;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundCount) => {
                warn!(
                    "Sorted variable Check and unsorted variable Count is unperformant as it requires deduplicating."
                );
                let mut multiplicity = 1;
                let mut dedup = HashSet::new();
                loop {
                    let peek = iterator.peek();
                    match peek {
                        None => return Ok(multiplicity),
                        Some(Ok((has, count))) => {
                            // TODO: handle has count
                            let attribute: Thing<'static> = has.attribute().clone().into_owned().into();
                            let new_element = dedup.insert(VariableValue::Thing(attribute));
                            if new_element {
                                multiplicity += 1;
                            } else {
                                iterator.advance_single()?;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (VariableMode::BoundSelect, VariableMode::UnboundCheck)
            | (VariableMode::UnboundCheck, VariableMode::UnboundCheck) => {
                // TODO: how do end iterator immediately?
                todo!();
                Ok(1)
            }
            (_, VariableMode::BoundSelect) => {
                unreachable!("Should never traverse to a bounded attribute, even as a check. Instead, invert the instruction direction or use a Check instruction.")
            }
        }
    }

    fn count_until_next_answer_has_sorted_attribute(
        answer_row: ImmutableRow,
        iterator: &mut HasSortedAttributeIterator,
        has: &ir::pattern::constraint::Has<Position>,
        variable_modes: HasVariableModes,
        mut deduplication_set: &mut Option<HashSet<VariableValue<'static>>>,
    ) -> Result<usize, ConceptReadError> {
        match (variable_modes.attribute(), variable_modes.owner()) {
            (VariableMode::UnboundSelect, VariableMode::UnboundSelect)
            | (VariableMode::BoundSelect, VariableMode::UnboundSelect) => {
                iterator.advance_single()?;
                Ok(1)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCount) => {
                let target = answer_row.get(has.attribute()).next_possible();
                let (count, _) = iterator.counting_skip_to_sorted_value(&target)?;
                Ok(count)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCheck) => {
                let target = answer_row.get(has.attribute()).next_possible();
                // TODO: replace with seek()
                let (_, _) = iterator.counting_skip_to_sorted_value(&target)?;
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundSelect) => {
                iterator.advance_single()?;
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundCount)
            | (VariableMode::BoundSelect, VariableMode::UnboundCount) => Ok(iterator.count()),
            (VariableMode::UnboundCount, VariableMode::UnboundCheck) => {
                let mut count = 1;
                iterator.advance_single()?;
                let mut target = answer_row.get(has.attribute()).next_possible();
                // TODO: replace with seek()
                while iterator.counting_skip_to_sorted_value(&target)?.1.is_some() {
                    count += 1;
                    // TODO: incorporate has count
                    let (has, count) = iterator.peek().unwrap().as_ref().unwrap();
                    target = VariableValue::Thing(Thing::from(has.attribute()).next_possible());
                }
                Ok(count)
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundSelect) => {
                warn!(
                    "Sorted variable Check and unsorted variable Select is unperformant as it requires deduplicating."
                );
                if deduplication_set.is_none() {
                    *deduplication_set = Some(HashSet::new());
                }
                let dedup = deduplication_set.as_mut().unwrap();
                loop {
                    let peek = iterator.peek();
                    match peek {
                        None => return Ok(1),
                        Some(Ok((has, count))) => {
                            // TODO: handle has count
                            let owner: Thing<'static> = has.owner().clone().into_owned().into();
                            let new_element = dedup.insert(VariableValue::Thing(owner));
                            if new_element {
                                return Ok(1);
                            } else {
                                iterator.advance_single()?;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundCount) => {
                warn!(
                    "Sorted variable Check and unsorted variable Count is unperformant as it requires deduplicating."
                );
                let mut multiplicity = 1;
                let mut dedup = HashSet::new();
                loop {
                    let peek = iterator.peek();
                    match peek {
                        None => return Ok(multiplicity),
                        Some(Ok((has, count))) => {
                            // TODO: handle has count
                            let owner: Thing<'static> = has.owner().clone().into_owned().into();
                            let new_element = dedup.insert(VariableValue::Thing(owner));
                            if new_element {
                                multiplicity += 1;
                            } else {
                                iterator.advance_single()?;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (VariableMode::BoundSelect, VariableMode::UnboundCheck)
            | (VariableMode::UnboundCheck, VariableMode::UnboundCheck) => {
                // TODO: how do end iterator immediately?
                todo!();
                Ok(1)
            }
            (_, VariableMode::BoundSelect) => {
                unreachable!("Should never traverse to a bounded attribute, even as a check. Instead, invert the instruction direction or use a Check instruction.")
            }
        }
    }

    pub(crate) fn write_values(&mut self, row: &mut Row) -> Result<(), ConceptReadError> {
        debug_assert!(self.has_value());
        // TODO: how to handle multiple answers found in an iterator?
        // TODO: when do we copy the selected values from the input Row into the output Row?
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, isa, _, _) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, row)
            }
            InstructionIterator::IsaRelationSortedThing(iter, isa, _, _) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, row)
            }
            InstructionIterator::IsaAttributeSortedThing(iter, isa, _, _) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, row)
            }
            InstructionIterator::HasSortedOwner(iter, has, _, _) => {
                // TODO: incorporate the repetitions/count
                let (has_value, count) = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?;
                Self::write_values_has(has_value, has, row)
            }
            InstructionIterator::HasSortedAttribute(iter, has, _, _) => {
                // TODO: incorporate the repetitions/count
                let (has_value, count) = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?;
                Self::write_values_has(has_value, has, row)
            }
        }
    }

    fn write_values_isa(
        thing: Thing<'_>,
        isa_constraint: &ir::pattern::constraint::Isa<Position>,
        row: &mut Row,
    ) -> Result<(), ConceptReadError> {
        row.set(isa_constraint.type_(), VariableValue::Type(thing.type_()));
        row.set(isa_constraint.thing(), VariableValue::Thing(thing.into_owned()));
        Ok(())
    }

    fn write_values_has(
        has: &Has<'_>,
        has_constraint: &ir::pattern::constraint::Has<Position>,
        row: &mut Row,
    ) -> Result<(), ConceptReadError> {
        row.set(has_constraint.owner(), VariableValue::Thing(has.owner().into_owned().into()));
        row.set(has_constraint.attribute(), VariableValue::Thing(has.attribute().into_owned().into()));
        Ok(())
    }

    pub(crate) fn has_value(&mut self) -> bool {
        self.peek_sorted_value().is_some()
    }

    const fn is_sorted(&self) -> bool {
        match self {
            InstructionIterator::IsaEntitySortedThing(_, _, _, _)
            | InstructionIterator::IsaRelationSortedThing(_, _, _, _)
            | InstructionIterator::IsaAttributeSortedThing(_, _, _, _)
            | InstructionIterator::HasSortedAttribute(_, _, _, _)
            | InstructionIterator::HasSortedOwner(_, _, _, _) => true,
        }
    }
}

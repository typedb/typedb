/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap};
use std::collections::HashSet;

pub use tracing::{error, info, trace, warn};

use answer::{Thing, variable::Variable, variable_value::VariableValue};
use concept::{
    error::ConceptReadError,
    thing::{entity::EntityIterator, has::Has, relation::RelationIterator, thing_manager::ThingManager},
};
use concept::thing::attribute::AttributeIterator;
use concept::thing::ThingAPI;
use concept::type_::TypeAPI;
use ir::inference::type_inference::TypeAnnotations;
use lending_iterator::{LendingIterator, Peekable};
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        instruction::{
            comparison_executor::ComparisonIteratorExecutor,
            comparison_reverse_executor::ComparisonReverseIteratorExecutor,
            function_call_binding_executor::FunctionCallBindingIteratorExecutor,
            has_executor::{
                HasBoundedSortedAttributeIterator, HasIteratorExecutor, HasUnboundedSortedAttributeMergedIterator,
                HasUnboundedSortedAttributeSingleIterator, HasUnboundedSortedOwnerIterator,
            },
            has_reverse_executor::HasReverseIteratorExecutor,
            isa_executor::IsaExecutor,
            role_player_executor::RolePlayerIteratorExecutor,
            role_player_reverse_executor::RolePlayerReverseIteratorExecutor,
        },
        pattern_executor::{ImmutableRow, Row},
        Position,
    },
    planner::pattern_plan::Instruction,
};
use crate::executor::instruction::has_executor::HasVariableModes;
use crate::executor::instruction::isa_executor::IsaVariableModes;

mod comparison_executor;
mod comparison_reverse_executor;
mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
mod role_player_executor;
mod role_player_reverse_executor;
mod isa_executor;

pub(crate) enum IteratorExecutor {
    Isa(IsaExecutor),

    Has(HasIteratorExecutor),
    HasReverse(HasReverseIteratorExecutor),

    RolePlayer(RolePlayerIteratorExecutor),
    RolePlayerReverse(RolePlayerReverseIteratorExecutor),

    // RolePlayerIndex(RolePlayerIndexExecutor),

    FunctionCallBinding(FunctionCallBindingIteratorExecutor),

    Comparison(ComparisonIteratorExecutor),
    ComparisonReverse(ComparisonReverseIteratorExecutor),
}

impl IteratorExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        instruction: Instruction,
        selected_variables: &Vec<Variable>,
        named_variables: &HashMap<Variable, String>,
        variable_positions: &HashMap<Variable, Position>,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        sort_by: Option<Variable>,
    ) -> Result<Self, ConceptReadError> {
        match instruction {
            Instruction::Isa(isa, bounds) => {
                let thing = isa.thing();
                let provider = IsaExecutor::new(
                    isa.clone(),
                    bounds,
                    selected_variables,
                    named_variables,
                    variable_positions,
                    sort_by,
                    type_annotations.constraint_annotations(isa.into()).unwrap().get_left_right().left_to_right(),
                    type_annotations.variable_annotations(thing).unwrap().clone(),
                );
                Ok(Self::Isa(provider))
            }
            Instruction::Has(has, bounds) => {
                let has_attribute = has.attribute();
                let executor = HasIteratorExecutor::new(
                    has.clone(),
                    bounds,
                    selected_variables,
                    named_variables,
                    variable_positions,
                    sort_by,
                    type_annotations.constraint_annotations(has.into()).unwrap().get_left_right().left_to_right(),
                    type_annotations.variable_annotations(has_attribute).unwrap().clone(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::Has(executor))
            }
            Instruction::HasReverse(has, mode) => {
                todo!()
                // Ok(Self::HasReverse(HasReverseExecutor::new(has.into_ids(variable_to_position), mode)))
            }
            Instruction::RolePlayer(rp, mode) => {
                todo!()
                // Ok(Self::RolePlayer(RolePlayerExecutor::new(rp.into_ids(variable_to_position), mode)))
            }
            Instruction::RolePlayerReverse(rp, mode) => {
                todo!()
                // Ok(Self::RolePlayerReverse(RolePlayerReverseExecutor::new(rp.into_ids(variable_to_position), mode)))
            }
            Instruction::FunctionCallBinding(function_call) => {
                todo!()
            }
            Instruction::ComparisonGenerator(comparison) => {
                todo!()
            }
            Instruction::ComparisonGeneratorReverse(comparison) => {
                todo!()
            }
            Instruction::ComparisonCheck(comparison) => {
                todo!()
            }
            Instruction::ExpressionBinding(expression_binding) => {
                todo!()
            }
        }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<InstructionIterator, ConceptReadError> {
        match self {
            IteratorExecutor::Isa(executor) => executor.get_iterator(snapshot, thing_manager, row),
            IteratorExecutor::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            IteratorExecutor::HasReverse(executor) => todo!(),
            IteratorExecutor::RolePlayer(executor) => todo!(),
            IteratorExecutor::RolePlayerReverse(executor) => todo!(),
            IteratorExecutor::FunctionCallBinding(executor) => todo!(),
            IteratorExecutor::Comparison(executor) => todo!(),
            IteratorExecutor::ComparisonReverse(executor) => todo!(),
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

    HasUnboundedSortedOwner(
        HasUnboundedSortedOwnerIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    HasUnboundedSortedAttributeMerged(
        HasUnboundedSortedAttributeMergedIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    // TODO: perhaps we can merge with HasBoundedSortedTo?
    HasUnboundedSortedAttributeSingle(
        HasUnboundedSortedAttributeSingleIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
        Option<HashSet<VariableValue<'static>>>,
    ),
    HasBoundedSortedAttribute(
        HasBoundedSortedAttributeIterator,
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
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|entity| VariableValue::Thing(entity.as_reference().into()))
            }),
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|relation| VariableValue::Thing(relation.as_reference().into()))
            }),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|attribute| VariableValue::Thing(attribute.as_reference().into()))
            }),
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::from(has.owner())))
            }),
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
        }
    }

    pub(crate) fn counting_skip_to_sorted<'a, T: Ord, Iterator, Mapper>(
        iterator: &mut Peekable<Iterator>,
        mapper: Mapper,
        target: &'a VariableValue<'a>,
    ) -> Result<Option<(Ordering, usize)>, ConceptReadError>
        where
            Mapper: Fn(&T) -> Result<VariableValue<'_>, ConceptReadError>,
            Iterator: for<'b> LendingIterator<Item<'b>=T> + 'static,
    {
        let mut count = 0;
        loop {
            let peek = iterator.peek();
            match peek {
                None => return Ok(None),
                Some(value) => {
                    let mapped = mapper(value)?;
                    let cmp = mapped.partial_cmp(&target).unwrap();
                    match cmp {
                        Ordering::Less => {}
                        Ordering::Equal => return Ok(Some((Ordering::Equal, count))),
                        Ordering::Greater => return Ok(Some((Ordering::Greater, count))),
                    }
                }
            }
            // match peek {
            //     None => return Ok(None),
            //     Some(Ok(peek_value)) => {
            //         let mapped = mapper(peek_value);
            //         let cmp = mapped.partial_cmp(&target).unwrap();
            //         match cmp {
            //             Ordering::Less => {}
            //             Ordering::Equal => return Ok(Some((Ordering::Equal, count))),
            //             Ordering::Greater => return Ok(Some((Ordering::Greater, count))),
            //         }
            //     }
            //     Some(Err(err)) => return Err(err.clone()),
            // }
            let _ = iterator.next();
            // TODO: incorporate edge count
            count += 1;
        }
    }

    pub(crate) fn counting_skip_to_sorted_value(
        &mut self,
        value: &VariableValue<'_>,
    ) -> Result<Option<(Ordering, usize)>, ConceptReadError> {
        debug_assert!(self.is_sorted());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|entity| VariableValue::Thing(entity.as_reference().into())),
                value,
            ),
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|relation| VariableValue::Thing(relation.as_reference().into())),
                value,
            ),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|attribute| VariableValue::Thing(attribute.as_reference().into())),
                value,
            ),
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|has| VariableValue::Thing(has.0.owner().into())),
                value,
            ),
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|has| VariableValue::Thing(has.0.attribute().into())),
                value,
            ),
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _, _) => Self::counting_skip_to_sorted(
                iter,
                |res| res.map(|has| VariableValue::Thing(has.0.attribute().into())),
                value,
            ),
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _, _) => {
                Self::counting_skip_to_sorted(
                    iter,
                    |res| res.map(|has| VariableValue::Thing(has.0.attribute().clone().into())),
                    value,
                )
            }
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
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _, _) => {
                iter.next().unwrap()?;
            }
        };
        Ok(())
    }

    pub(crate) fn counting_advance_past(&mut self, answer_row: ImmutableRow) -> Result<usize, ConceptReadError> {
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
            InstructionIterator::HasUnboundedSortedOwner(iter, has, variable_modes, dedup) => {
                self.counting_advance_has_sorted_owner(answer_row, iter, has, *variable_modes, dedup)
            }
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, has, variable_modes, dedup) => {
                todo!()
            }
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, has, variable_modes, dedup) => {
                todo!()
            }
            InstructionIterator::HasBoundedSortedAttribute(iter, has, variable_modes, dedup) => {
                self.counting_advance_has_sorted_owner(answer_row, iter, has, *variable_modes, dedup)
            }
        }
    }

    fn counting_advance_has_sorted_owner<'a, HasIterator>(
        &mut self,
        answer_row: ImmutableRow,
        iterator: &'a mut Peekable<HasIterator>,
        has: &ir::pattern::constraint::Has<Position>,
        variable_modes: HasVariableModes,
        mut deduplication_set: &mut Option<HashSet<VariableValue<'static>>>,
    ) -> Result<usize, ConceptReadError>
        where
            HasIterator: for<'b> LendingIterator<Item<'b>=Result<(Has<'b>, u64), ConceptReadError>>
    {
        match (variable_modes.owner(), variable_modes.attribute()) {
            (VariableMode::UnboundSelect, VariableMode::UnboundSelect)
            | (VariableMode::BoundSelect, VariableMode::UnboundSelect) => {
                iterator.next().unwrap()?;
                Ok(1)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCount) => {
                let target = answer_row.get(has.owner()).next_possible();
                let (_, count) = Self::counting_skip_to_sorted(
                    iterator,
                    |res| res.map(|(has, _)| VariableValue::Thing(has.owner().into())),
                    &target,
                )?.unwrap();
                Ok(count)
            }
            (VariableMode::UnboundSelect, VariableMode::UnboundCheck) => {
                let target = answer_row.get(has.owner()).next_possible();
                // TODO: replace with seek()
                let (_, count) = Self::counting_skip_to_sorted(
                    iterator,
                    |res| res.map(|(has, _)| VariableValue::Thing(has.owner().into())),
                    &target,
                )?.unwrap();
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundSelect) => {
                iterator.next().unwrap()?;
                Ok(1)
            }
            (VariableMode::UnboundCount, VariableMode::UnboundCount)
            | (VariableMode::BoundSelect, VariableMode::UnboundCount) => {
                Ok(iterator.count())
            }
            (VariableMode::UnboundCount, VariableMode::UnboundCheck) => {
                let mut count = 1;
                iterator.next().unwrap()?;
                let mut target = answer_row.get(has.owner()).next_possible();
                // TODO: replace with seek()
                while Self::counting_skip_to_sorted(
                    iterator,
                    |res| res.map(|(has, _)| VariableValue::Thing(has.owner().into())),
                    &target,
                )?.is_some() {
                    count += 1;
                    // TODO: incorporate has count
                    let (has, count) = iterator.peek().unwrap().as_ref().unwrap();
                    target = VariableValue::Thing(Thing::from(has.owner()).next_possible());
                }
                Ok(count)
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundSelect) => {
                warn!("Sorted variable Check and unsorted variable Select is unperformant as it requires deduplicating.");
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
                                iterator.next().unwrap()?;
                            }
                        }
                        Some(Err(err)) => return Err(err.clone()),
                    }
                }
            }
            (VariableMode::UnboundCheck, VariableMode::UnboundCount) => {
                warn!("Sorted variable Check and unsorted variable Count is unperformant as it requires deduplicating.");
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
                                iterator.next().unwrap()?;
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
                unreachable!("Should never traverse to a bounded attribute, even as a check. Instead, invert the instruction direction or use a Check.")
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
            InstructionIterator::HasUnboundedSortedOwner(iter, has, _, _) => {
                Self::write_values_has(iter, has, row)
            }
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, has, _, _) => {
                Self::write_values_has(iter, has, row)
            }
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, has, _, _) => {
                Self::write_values_has(iter, has, row)
            }
            InstructionIterator::HasBoundedSortedAttribute(iter, has, _, _) => {
                Self::write_values_has(iter, has, row)
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

    fn write_values_has<'a, HasIterator: for<'b> LendingIterator<Item<'b>=Result<(Has<'b>, u64), ConceptReadError>>>(
        iterator: &'a mut Peekable<HasIterator>,
        has_constraint: &ir::pattern::constraint::Has<Position>,
        row: &mut Row,
    ) -> Result<(), ConceptReadError> {
        let (has_value, count) = iterator.peek().unwrap().as_ref().map_err(|err| err.clone())?;
        // TODO: incorporate the repetitions/count
        row.set(has_constraint.owner(), VariableValue::Thing(has_value.owner().into_owned().into()));
        row.set(has_constraint.attribute(), VariableValue::Thing(has_value.attribute().into_owned().into()));
        Ok(())
    }

    pub(crate) fn has_value(&mut self) -> bool {
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::IsaRelationSortedThing(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _, _) => iter.peek().is_some(),
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _, _) => iter.peek().is_some(),
        }
    }

    const fn is_sorted(&self) -> bool {
        match self {
            InstructionIterator::IsaEntitySortedThing(_, _, _, _)
            | InstructionIterator::IsaRelationSortedThing(_, _, _, _)
            | InstructionIterator::IsaAttributeSortedThing(_, _, _, _)
            | InstructionIterator::HasUnboundedSortedOwner(_, _, _, _)
            | InstructionIterator::HasUnboundedSortedAttributeMerged(_, _, _, _)
            | InstructionIterator::HasUnboundedSortedAttributeSingle(_, _, _, _)
            | InstructionIterator::HasBoundedSortedAttribute(_, _, _, _) => true,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum VariableMode {
    BoundSelect,
    UnboundSelect,
    UnboundCount,
    UnboundCheck,
}

impl VariableMode {}

impl VariableMode {
    pub(crate) const fn new(is_bound: bool, is_selected: bool, is_named: bool) -> VariableMode {
        match (is_bound, is_selected, is_named) {
            (true, _, _) => Self::BoundSelect,
            (false, true, _) => Self::UnboundSelect,
            (false, false, true) => Self::UnboundCount,
            (false, false, false) => Self::UnboundCheck,
        }
    }

    pub(crate) fn is_bound(&self) -> bool {
        matches!(self, Self::BoundSelect)
    }

    pub(crate) fn is_unbound(&self) -> bool {
        !self.is_bound()
    }
}

// enum CheckExecutor {
//     Has(HasCheckExecutor),
//     HasReverse(HasReverseCheckExecutor),
//
//     RolePlayer(RolePlayerCheckExecutor),
//     RolePlayerReverse(RolePlayerReverseCheckExecutor),
//
//     // RolePlayerIndex(RolePlayerIndexExecutor),
//
//     Comparison(ComparisonCheckExecutor),
// }

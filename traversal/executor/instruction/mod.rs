/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, collections::HashMap};

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
    ),
    IsaRelationSortedThing(
        Peekable<RelationIterator>,
        ir::pattern::constraint::Isa<Position>,
        IsaVariableModes,
    ),
    IsaAttributeSortedThing(
        Peekable<AttributeIterator>,
        ir::pattern::constraint::Isa<Position>,
        IsaVariableModes,
    ),

    HasUnboundedSortedOwner(
        HasUnboundedSortedOwnerIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
    ),
    HasUnboundedSortedAttributeMerged(
        HasUnboundedSortedAttributeMergedIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
    ),
    // TODO: perhaps we can merge with HasBoundedSortedTo?
    HasUnboundedSortedAttributeSingle(
        HasUnboundedSortedAttributeSingleIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
    ),
    HasBoundedSortedAttribute(
        HasBoundedSortedAttributeIterator,
        ir::pattern::constraint::Has<Position>,
        HasVariableModes,
    ),
}

impl InstructionIterator {
    pub(crate) fn peek_sorted_value_equals(&mut self, value: &VariableValue<'_>) -> Result<bool, ConceptReadError> {
        Ok(self.peek_sorted_value().transpose().map_err(|err| err.clone())?.is_some_and(|peek| peek == *value))
    }

    pub(crate) fn peek_sorted_value(&mut self) -> Option<Result<VariableValue<'_>, &ConceptReadError>> {
        debug_assert!(self.is_sorted());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|entity| VariableValue::Thing(entity.as_reference().into()))
            }),
            InstructionIterator::IsaRelationSortedThing(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|relation| VariableValue::Thing(relation.as_reference().into()))
            }),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|attribute| VariableValue::Thing(attribute.as_reference().into()))
            }),
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::from(has.owner())))
            }),
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _) => iter.peek().map(|result| {
                result.as_ref().map(|(has, count)| VariableValue::Thing(Thing::Attribute(has.attribute())))
            }),
        }
    }

    pub(crate) fn skip_to_sorted_value(
        &mut self,
        value: &VariableValue<'_>,
    ) -> Result<Option<Ordering>, ConceptReadError> {
        debug_assert!(self.is_sorted());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(peek_value.as_reference().into()).partial_cmp(value).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::IsaRelationSortedThing(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(peek_value.as_reference().into()).partial_cmp(value).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::IsaAttributeSortedThing(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(peek_value.as_reference().into()).partial_cmp(value).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(Thing::from(peek_value.0.owner())).partial_cmp(value).unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(Thing::Attribute(peek_value.0.attribute()))
                            .partial_cmp(value)
                            .unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(Thing::Attribute(peek_value.0.attribute()))
                            .partial_cmp(value)
                            .unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
                let _ = iter.next();
            },
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _) => loop {
                let peek = iter.peek();
                match peek {
                    None => return Ok(None),
                    Some(Ok(peek_value)) => {
                        let cmp = VariableValue::Thing(Thing::Attribute(peek_value.0.attribute()))
                            .partial_cmp(value)
                            .unwrap();
                        match cmp {
                            Ordering::Less => {}
                            Ordering::Equal => return Ok(Some(Ordering::Equal)),
                            Ordering::Greater => return Ok(Some(Ordering::Greater)),
                        }
                    }
                    Some(Err(err)) => return Err(err.clone()),
                }
            },
        }
    }

    pub(crate) fn advance(&mut self) -> Result<(), ConceptReadError> {
        assert!(self.has_value());
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _) => {
                iter.next().transpose()?;
            }
            InstructionIterator::IsaRelationSortedThing(iter, _, _) => {
                iter.next().transpose()?;
            }
            InstructionIterator::IsaAttributeSortedThing(iter, _, _) => {
                iter.next().transpose()?;
            }
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _) => {
                // TODO: how to handle multiple answers found in an iterator, eg (_, count > 1)?
                iter.next().transpose()?;
            }
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _) => {
                // TODO: how to handle multiple answers found in an iterator, eg (_, count > 1)?
                iter.next().transpose()?;
            }
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _) => {
                // TODO: how to handle multiple answers found in an iterator, eg (_, count > 1)?
                iter.next().transpose()?;
            }
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _) => {
                // TODO: how to handle multiple answers found in an iterator, eg (_, count > 1)?
                iter.next().transpose()?;
            }
        };
        Ok(())
    }

    pub(crate) fn write_values(&mut self, row: &mut Row) -> Result<(), ConceptReadError> {
        debug_assert!(self.has_value());
        // TODO: how to handle multiple answers found in an iterator?
        // TODO: when do we copy the selected values from the input Row into the output Row?
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, isa, isa_variable_modes) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, *isa_variable_modes, row)
            }
            InstructionIterator::IsaRelationSortedThing(iter, isa, isa_variable_modes) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, *isa_variable_modes, row)
            }
            InstructionIterator::IsaAttributeSortedThing(iter, isa, isa_variable_modes) => {
                let thing: Thing<'_> = iter.peek().unwrap().as_ref().map_err(|err| err.clone())?.clone().into();
                Self::write_values_isa(thing, isa, *isa_variable_modes, row)
            }
            InstructionIterator::HasUnboundedSortedOwner(iter, has, has_variable_modes) => {
                Self::write_values_has(iter, has, *has_variable_modes, row)
            }
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, has, has_variable_modes) => {
                Self::write_values_has(iter, has, *has_variable_modes, row)
            }
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, has, has_variable_modes) => {
                Self::write_values_has(iter, has, *has_variable_modes, row)
            }
            InstructionIterator::HasBoundedSortedAttribute(iter, has, has_variable_modes) => {
                Self::write_values_has(iter, has, *has_variable_modes, row)
            }
        }
    }

    fn write_values_isa(
        thing: Thing<'_>,
        isa_constraint: &ir::pattern::constraint::Isa<Position>,
        modes: IsaVariableModes,
        row: &mut Row,
    ) -> Result<(), ConceptReadError> {
        match modes.type_() {
            VariableMode::BoundSelect
            | VariableMode::UnboundSelect => {
                row.set(isa_constraint.thing(), VariableValue::Type(thing.type_()));
            }
            VariableMode::UnboundCount
            | VariableMode::UnboundCheck
            | VariableMode::BoundUnselect => {
                debug_assert!(*row.get(isa_constraint.type_()) == VariableValue::Empty)
            }
        }
        match modes.thing() {
            VariableMode::BoundSelect
            | VariableMode::UnboundSelect => {
                row.set(isa_constraint.thing(), VariableValue::Thing(thing.into_owned()))
            }
            VariableMode::UnboundCount
            | VariableMode::UnboundCheck
            | VariableMode::BoundUnselect => {
                debug_assert!(*row.get(isa_constraint.thing()) == VariableValue::Empty)
            }
        }
        Ok(())
    }

    fn write_values_has<'a, HasIterator: for<'b> LendingIterator<Item<'b>=Result<(Has<'b>, u64), ConceptReadError>>>(
        iterator: &'a mut Peekable<HasIterator>,
        has_constraint: &ir::pattern::constraint::Has<Position>,
        modes: HasVariableModes,
        row: &mut Row,
    ) -> Result<(), ConceptReadError> {
        let (has_value, count) = iterator.peek().unwrap().as_ref().map_err(|err| err.clone())?;
        // TODO: incorporate the repetitions/count
        match modes.owner() {
            VariableMode::BoundSelect => {
                row.set(has_constraint.owner(), VariableValue::Thing(has_value.owner().into_owned().into()));
            }
            VariableMode::UnboundSelect => {
                row.set(has_constraint.owner(), VariableValue::Thing(has_value.owner().clone().into_owned().into()));
            }
            VariableMode::UnboundCount
            | VariableMode::UnboundCheck
            | VariableMode::BoundUnselect => {
                debug_assert!(*row.get(has_constraint.owner()) == VariableValue::Empty)
            }
        };
        match modes.attribute() {
            VariableMode::BoundSelect => {
                row.set(has_constraint.attribute(), VariableValue::Thing(has_value.attribute().into_owned().into()));
            }
            VariableMode::UnboundSelect => {
                row.set(has_constraint.attribute(), VariableValue::Thing(has_value.attribute().clone().into_owned().into()));
            }
            VariableMode::UnboundCount
            | VariableMode::UnboundCheck
            | VariableMode::BoundUnselect => {
                // don't write counts or checks since they are not selected
                debug_assert!(*row.get(has_constraint.attribute()) == VariableValue::Empty)
            }
        };
        Ok(())
    }

    pub(crate) fn write_values_and_advance_optimised(&mut self, row: &mut Row) -> Result<(), &ConceptReadError> {
        debug_assert!(self.has_value());
        match self {
            InstructionIterator::IsaEntitySortedThing(_, _, _) => {}
            InstructionIterator::IsaRelationSortedThing(_, _, _) => {}
            InstructionIterator::IsaAttributeSortedThing(_, _, _) => {}
            InstructionIterator::HasUnboundedSortedOwner(iter, has, has_variable_modes) => {}
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, has, has_variable_modes) => {}
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, has, has_variable_modes) => {}
            InstructionIterator::HasBoundedSortedAttribute(iter, has, has_variable_modes) => {}
        }
        todo!()
    }

    pub(crate) fn has_value(&mut self) -> bool {
        match self {
            InstructionIterator::IsaEntitySortedThing(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::IsaRelationSortedThing(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::IsaAttributeSortedThing(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedOwner(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedAttributeMerged(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::HasUnboundedSortedAttributeSingle(iter, _, _) => iter.peek().is_some(),
            InstructionIterator::HasBoundedSortedAttribute(iter, _, _) => iter.peek().is_some(),
        }
    }

    const fn is_sorted(&self) -> bool {
        match self {
            InstructionIterator::IsaEntitySortedThing(_, _, _)
            | InstructionIterator::IsaRelationSortedThing(_, _, _)
            | InstructionIterator::IsaAttributeSortedThing(_, _, _)
            | InstructionIterator::HasUnboundedSortedOwner(_, _, _)
            | InstructionIterator::HasUnboundedSortedAttributeMerged(_, _, _)
            | InstructionIterator::HasUnboundedSortedAttributeSingle(_, _, _)
            | InstructionIterator::HasBoundedSortedAttribute(_, _, _) => true,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum VariableMode {
    BoundUnselect,
    BoundSelect,
    UnboundSelect,
    UnboundCount,
    UnboundCheck,
}

impl VariableMode {}

impl VariableMode {
    pub(crate) const fn new(is_bound: bool, is_selected: bool, is_named: bool) -> VariableMode {
        match (is_bound, is_selected, is_named) {
            (true, false, _) => Self::BoundUnselect,
            (true, true, _) => Self::BoundSelect,
            (false, true, _) => Self::UnboundSelect,
            (false, false, true) => Self::UnboundCount,
            (false, false, false) => Self::UnboundCheck,
        }
    }

    pub(crate) fn is_bound(&self) -> bool {
        matches!(self, Self::BoundSelect) || matches!(self, Self::BoundUnselect)
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

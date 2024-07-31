/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::{
    inference::type_annotations::TypeAnnotations,
};
use compiler::instruction::constraint::instructions::ConstraintInstruction;
use compiler::planner::pattern_plan::InstructionAPI;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pattern::constraint::Constraint;
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    instruction::{
        comparison_executor::ComparisonIteratorExecutor,
        comparison_reverse_executor::ComparisonReverseIteratorExecutor,
        function_call_binding_executor::FunctionCallBindingIteratorExecutor, has_executor::HasExecutor,
        has_reverse_executor::HasReverseExecutor, isa_reverse_executor::IsaReverseExecutor, iterator::TupleIterator,
        role_player_executor::RolePlayerIteratorExecutor,
        role_player_reverse_executor::RolePlayerReverseIteratorExecutor, InstructionExecutor::HasReverse,
    },
    VariablePosition,
};

mod comparison_executor;
mod comparison_reverse_executor;
mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
mod isa_reverse_executor;
pub(crate) mod iterator;
mod role_player_executor;
mod role_player_reverse_executor;
pub(crate) mod tuple;

pub(crate) enum InstructionExecutor {
    Isa(IsaReverseExecutor),

    Has(HasExecutor),
    HasReverse(HasReverseExecutor),

    RolePlayer(RolePlayerIteratorExecutor),
    RolePlayerReverse(RolePlayerReverseIteratorExecutor),

    // RolePlayerIndex(RolePlayerIndexExecutor),
    FunctionCallBinding(FunctionCallBindingIteratorExecutor),

    Comparison(ComparisonIteratorExecutor),
    ComparisonReverse(ComparisonReverseIteratorExecutor),
}

impl InstructionExecutor {
    pub(crate) fn new<Snapshot: ReadableSnapshot>(
        instruction: ConstraintInstruction,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>,
        positions: &HashMap<Variable, VariablePosition>,
        type_annotations: &TypeAnnotations,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        sort_by: Option<Variable>,
    ) -> Result<Self, ConceptReadError> {
        let variable_modes = VariableModes::new_for(&instruction, positions, selected, named);
        let sort_by_position = sort_by.map(|var| *positions.get(&var).unwrap());
        match instruction {
            ConstraintInstruction::Isa(isa, _) => todo!(),
            ConstraintInstruction::IsaReverse(isa, _) => {
                let thing = isa.thing();
                let provider = IsaReverseExecutor::new(
                    isa.clone().into_ids(positions),
                    variable_modes,
                    sort_by_position,
                    type_annotations.constraint_annotations_of(isa.into()).unwrap().get_left_right().left_to_right(),
                    type_annotations.variable_annotations_of(thing).unwrap().clone(),
                );
                Ok(Self::Isa(provider))
            }
            ConstraintInstruction::Has(has, _) => {
                let has_attribute = has.attribute();
                let executor = HasExecutor::new(
                    has.clone().into_ids(positions),
                    variable_modes,
                    sort_by_position,
                    type_annotations.constraint_annotations_of(has.into()).unwrap().get_left_right().left_to_right(),
                    type_annotations.variable_annotations_of(has_attribute).unwrap().clone(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::Has(executor))
            }
            ConstraintInstruction::HasReverse(has, _) => {
                let has_owner = has.owner();
                let executor = HasReverseExecutor::new(
                    has.clone().into_ids(positions),
                    variable_modes,
                    sort_by_position,
                    type_annotations.constraint_annotations_of(has.into()).unwrap().get_left_right().right_to_left(),
                    type_annotations.variable_annotations_of(has_owner).unwrap().clone(),
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::HasReverse(executor))
            }
            ConstraintInstruction::RolePlayer(rp, _) => {
                todo!()
                // Ok(Self::RolePlayer(RolePlayerExecutor::new(rp.into_ids(variable_to_position), mode)))
            }
            ConstraintInstruction::RolePlayerReverse(rp, mode) => {
                todo!()
                // Ok(Self::RolePlayerReverse(RolePlayerReverseExecutor::new(rp.into_ids(variable_to_position), mode)))
            }
            ConstraintInstruction::FunctionCallBinding(function_call) => {
                todo!()
            }
            ConstraintInstruction::ComparisonGenerator(comparison) => {
                todo!()
            }
            ConstraintInstruction::ComparisonGeneratorReverse(comparison) => {
                todo!()
            }
            ConstraintInstruction::ComparisonCheck(comparison) => {
                todo!()
            }
            ConstraintInstruction::ExpressionBinding(expression_binding) => {
                todo!()
            }
        }
    }

    pub(crate) fn get_iterator<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self {
            InstructionExecutor::Isa(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::HasReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::RolePlayer(executor) => todo!(),
            InstructionExecutor::RolePlayerReverse(executor) => todo!(),
            InstructionExecutor::FunctionCallBinding(executor) => todo!(),
            InstructionExecutor::Comparison(executor) => todo!(),
            InstructionExecutor::ComparisonReverse(executor) => todo!(),
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

pub(crate) struct VariableModes {
    modes: HashMap<VariablePosition, VariableMode>,
}

impl VariableModes {
    fn new() -> Self {
        VariableModes { modes: HashMap::new() }
    }

    pub(crate) fn new_for(
        instruction: &ConstraintInstruction,
        variable_positions: &HashMap<Variable, VariablePosition>,
        selected: &[Variable],
        named: &HashMap<Variable, String>,
    ) -> Self {
        let constraint = instruction.constraint();
        let mut modes = Self::new();
        constraint.ids_foreach(|id, _| {
            let as_position = *variable_positions.get(&id).unwrap();
            let var_mode =
                VariableMode::new(instruction.is_input_variable(id), selected.contains(&id), named.contains_key(&id));
            modes.insert(as_position, var_mode)
        });
        modes
    }

    fn insert(&mut self, variable_position: VariablePosition, mode: VariableMode) {
        let existing = self.modes.insert(variable_position, mode);
        debug_assert!(existing.is_none())
    }

    pub(crate) fn get(&self, variable_position: VariablePosition) -> Option<&VariableMode> {
        self.modes.get(&variable_position)
    }

    pub(crate) fn fully_bound(&self) -> bool {
        self.modes.values().all(|mode| mode.is_bound())
    }

    pub(crate) fn fully_unbound(&self) -> bool {
        self.modes.values().all(|mode| mode.is_unbound())
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum BinaryIterateMode {
    Unbound,
    // [x, y] in standard order
    UnboundInverted,
    // [x, y] in [y, x] sort order
    BoundFrom, // [X, y], where X is bound
}

impl BinaryIterateMode {
    pub(crate) fn new(
        constraint: impl Into<Constraint<VariablePosition>>,
        in_reverse_direction: bool,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> BinaryIterateMode {
        let constraint = constraint.into();
        debug_assert!(constraint.ids_count() == 2);
        debug_assert!(!var_modes.fully_bound());

        let default_sort_variable_for_direction =
            if in_reverse_direction { constraint.right_id() } else { constraint.left_id() };

        if var_modes.fully_unbound() {
            match sort_by {
                None => {
                    // arbitrarily pick from sorted
                    BinaryIterateMode::Unbound
                }
                Some(variable) => {
                    if default_sort_variable_for_direction == variable {
                        BinaryIterateMode::Unbound
                    } else {
                        BinaryIterateMode::UnboundInverted
                    }
                }
            }
        } else {
            BinaryIterateMode::BoundFrom
        }
    }

    pub(crate) fn is_inverted(&self) -> bool {
        matches!(self, Self::UnboundInverted)
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

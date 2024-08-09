/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::{
    inference::type_annotations::TypeAnnotations, instruction::constraint::instructions::ConstraintInstruction,
    planner::pattern_plan::InstructionAPI,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::ImmutableRow,
    instruction::{
        comparison_executor::ComparisonIteratorExecutor,
        comparison_reverse_executor::ComparisonReverseIteratorExecutor,
        function_call_binding_executor::FunctionCallBindingIteratorExecutor, has_executor::HasExecutor,
        has_reverse_executor::HasReverseExecutor, isa_reverse_executor::IsaReverseExecutor, iterator::TupleIterator,
        role_player_executor::RolePlayerExecutor, role_player_reverse_executor::RolePlayerReverseExecutor,
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
    IsaReverse(IsaReverseExecutor),

    Has(HasExecutor),
    HasReverse(HasReverseExecutor),

    RolePlayer(RolePlayerExecutor),
    RolePlayerReverse(RolePlayerReverseExecutor),

    // RolePlayerIndex(RolePlayerIndexExecutor),
    FunctionCallBinding(FunctionCallBindingIteratorExecutor),

    Comparison(ComparisonIteratorExecutor),
    ComparisonReverse(ComparisonReverseIteratorExecutor),
}

impl InstructionExecutor {
    pub(crate) fn new(
        instruction: ConstraintInstruction,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>,
        positions: &HashMap<Variable, VariablePosition>,
        type_annotations: &TypeAnnotations,
        snapshot: &impl ReadableSnapshot,
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
                    type_annotations.constraint_annotations_of(isa.into()).unwrap().as_left_right().right_to_left(),
                    type_annotations.variable_annotations_of(thing).unwrap().clone(),
                );
                Ok(Self::IsaReverse(provider))
            }
            ConstraintInstruction::Has(has) => {
                let executor =
                    HasExecutor::new(has.map(positions), variable_modes, sort_by_position, snapshot, thing_manager)?;
                Ok(Self::Has(executor))
            }
            ConstraintInstruction::HasReverse(has_reverse) => {
                let executor = HasReverseExecutor::new(
                    has_reverse.map(positions),
                    variable_modes,
                    sort_by_position,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::HasReverse(executor))
            }
            ConstraintInstruction::RolePlayer(role_player) => {
                let executor = RolePlayerExecutor::new(
                    role_player.map(positions),
                    variable_modes,
                    sort_by_position,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::RolePlayer(executor))
            }
            ConstraintInstruction::RolePlayerReverse(role_player_reverse) => {
                let executor = RolePlayerReverseExecutor::new(
                    role_player_reverse.map(positions),
                    variable_modes,
                    sort_by_position,
                    snapshot,
                    thing_manager,
                )?;
                Ok(Self::RolePlayerReverse(executor))
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

    pub(crate) fn get_iterator(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        row: ImmutableRow<'_>,
    ) -> Result<TupleIterator, ConceptReadError> {
        match self {
            InstructionExecutor::IsaReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::HasReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::RolePlayer(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::RolePlayerReverse(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::FunctionCallBinding(executor) => todo!(),
            InstructionExecutor::Comparison(executor) => todo!(),
            InstructionExecutor::ComparisonReverse(executor) => todo!(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum VariableMode {
    Input,
    Output,
    Count,
    Check,
}

impl VariableMode {
    pub(crate) const fn new(is_input: bool, is_selected: bool, is_named: bool) -> VariableMode {
        if is_input {
            Self::Input
        } else if is_selected {
            Self::Output
        } else if is_named {
            Self::Count
        } else {
            Self::Check
        }
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

    pub(crate) fn all_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode == &VariableMode::Input)
    }

    pub(crate) fn none_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode != &VariableMode::Input)
    }

    fn len(&self) -> usize {
        self.modes.len()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum BinaryIterateMode {
    // [x, y] in standard order, sorted by x, then y
    Unbound,
    // [x, y] in [y, x] sort order
    UnboundInverted,
    // [X, y], where X is bound
    BoundFrom,
}

impl BinaryIterateMode {
    pub(crate) fn new(
        from_var: VariablePosition,
        to_var: VariablePosition,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> BinaryIterateMode {
        debug_assert!(var_modes.len() == 2);
        debug_assert!(!var_modes.all_inputs());

        let is_from_bound = var_modes.get(from_var) == Some(&VariableMode::Input);
        debug_assert!(var_modes.get(to_var) != Some(&VariableMode::Input));

        if is_from_bound {
            Self::BoundFrom
        } else if sort_by == Some(to_var) {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
    }

    pub(crate) fn is_inverted(&self) -> bool {
        self == &Self::UnboundInverted
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum TernaryIterateMode {
    // [x, y, z] = standard sort order
    Unbound,
    // [y, x, z] sort order
    UnboundInverted,
    // [X, y, z] sort order
    BoundFrom,
    // [X, Y, z]
    BoundFromBoundTo,
}

impl TernaryIterateMode {
    pub(crate) fn new(
        from_var: VariablePosition,
        to_var: VariablePosition,
        var_modes: &VariableModes,
        sort_by: Option<VariablePosition>,
    ) -> TernaryIterateMode {
        debug_assert!(var_modes.len() == 3);
        debug_assert!(!var_modes.all_inputs());
        let is_from_bound = var_modes.get(from_var) == Some(&VariableMode::Input);
        let is_to_bound = var_modes.get(to_var) == Some(&VariableMode::Input);

        if is_to_bound {
            assert!(is_from_bound);
            Self::BoundFromBoundTo
        } else if is_from_bound {
            Self::BoundFrom
        } else if sort_by == Some(to_var) {
            Self::UnboundInverted
        } else {
            Self::Unbound
        }
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

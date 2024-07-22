/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

pub use tracing::{error, info, trace, warn};

use answer::variable::Variable;
use concept::{
    error::ConceptReadError,
    thing::{thing_manager::ThingManager, ThingAPI},
    type_::TypeAPI,
};
use ir::inference::type_inference::TypeAnnotations;
use ir::pattern::constraint::Constraint;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        batch::ImmutableRow,
        instruction::{
            comparison_executor::ComparisonIteratorExecutor,
            comparison_reverse_executor::ComparisonReverseIteratorExecutor,
            function_call_binding_executor::FunctionCallBindingIteratorExecutor, has_executor::HasExecutor,
            has_reverse_executor::HasReverseIteratorExecutor, isa_executor::IsaExecutor,
            role_player_executor::RolePlayerIteratorExecutor,
            role_player_reverse_executor::RolePlayerReverseIteratorExecutor,
        },
        Position,
    },
    planner::pattern_plan::Instruction,
};
use crate::executor::instruction::iterator::TupleIterator;
use crate::planner::pattern_plan::IterateBounds;

mod comparison_executor;
mod comparison_reverse_executor;
mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
mod isa_executor;
pub(crate) mod iterator;
mod role_player_executor;
mod role_player_reverse_executor;
pub(crate) mod tuple;

pub(crate) enum InstructionExecutor {
    Isa(IsaExecutor),

    Has(HasExecutor),
    HasReverse(HasReverseIteratorExecutor),

    RolePlayer(RolePlayerIteratorExecutor),
    RolePlayerReverse(RolePlayerReverseIteratorExecutor),

    // RolePlayerIndex(RolePlayerIndexExecutor),
    FunctionCallBinding(FunctionCallBindingIteratorExecutor),

    Comparison(ComparisonIteratorExecutor),
    ComparisonReverse(ComparisonReverseIteratorExecutor),
}

impl InstructionExecutor {
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
                let executor = HasExecutor::new(
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
    ) -> Result<TupleIterator, ConceptReadError> {
        match self {
            InstructionExecutor::Isa(executor) => todo!(), // executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::Has(executor) => executor.get_iterator(snapshot, thing_manager, row),
            InstructionExecutor::HasReverse(executor) => todo!(),
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
    modes: HashMap<Position, VariableMode>,
}

impl VariableModes {
    fn new() -> Self {
        VariableModes { modes: HashMap::new() }
    }

    pub(crate) fn new_from(
        constraint: impl Into<Constraint<Variable>>,
        variable_positions: &HashMap<Variable, Position>,
        bounds: &IterateBounds<Variable>,
        selected: &Vec<Variable>,
        named: &HashMap<Variable, String>
    ) -> Self {
        let mut modes = Self::new();
        constraint.into().ids_foreach(|id, _| {
            let as_position = *variable_positions.get(&id).unwrap();
            modes.insert(as_position, VariableMode::new(bounds.contains(id), selected.contains(&id), named.contains_key(&id)))
        });
        modes
    }

    pub(crate) fn insert(&mut self, variable_position: Position, mode: VariableMode) {
        let existing = self.modes.insert(variable_position, mode);
        debug_assert!(existing.is_none())
    }

    pub(crate) fn get(&self, variable_position: Position) -> Option<&VariableMode> {
        self.modes.get(&variable_position)
    }

    pub(crate) fn is_fully_bound(&self) -> bool {
        self.modes.values().all(|mode| mode.is_bound())
    }

    pub(crate) fn is_fully_unbound(&self) -> bool {
        self.modes.values().all(|mode| mode.is_unbound())
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

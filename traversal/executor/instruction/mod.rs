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
    thing::thing_manager::ThingManager,
};
use concept::thing::ThingAPI;
use concept::type_::TypeAPI;
use ir::inference::type_inference::TypeAnnotations;
use iterator::InstructionIterator;
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{
    executor::{
        instruction::{
            comparison_executor::ComparisonIteratorExecutor,
            comparison_reverse_executor::ComparisonReverseIteratorExecutor,
            function_call_binding_executor::FunctionCallBindingIteratorExecutor,
            has_executor::HasIteratorExecutor,
            has_reverse_executor::HasReverseIteratorExecutor,
            isa_executor::IsaExecutor,
            role_player_executor::RolePlayerIteratorExecutor,
            role_player_reverse_executor::RolePlayerReverseIteratorExecutor,
        },
        pattern_executor::ImmutableRow,
        Position,
    },
    planner::pattern_plan::Instruction,
};

mod comparison_executor;
mod comparison_reverse_executor;
mod function_call_binding_executor;
mod has_executor;
mod has_reverse_executor;
mod role_player_executor;
mod role_player_reverse_executor;
mod isa_executor;
mod iterator_advance;
pub(crate) mod iterator;

pub(crate) enum InstructionExecutor {
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
            InstructionExecutor::Isa(executor) => executor.get_iterator(snapshot, thing_manager, row),
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

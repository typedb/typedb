/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::HashMap;
use itertools::Itertools;
use answer::variable::Variable;
use ir::pattern::constraint::{Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Isa, RolePlayer};
use ir::pattern::IrID;
use crate::planner::pattern_plan::InstructionAPI;

#[derive(Debug, Clone)]
pub enum ConstraintInstruction {
    // type -> thing
    Isa(Isa<Variable>, Inputs<Variable>),
    // thing -> type
    IsaReverse(Isa<Variable>, Inputs<Variable>),

    // owner -> attribute
    Has(Has<Variable>, Inputs<Variable>),
    // attribute -> owner
    HasReverse(Has<Variable>, Inputs<Variable>),

    // relation -> player
    RolePlayer(RolePlayer<Variable>, Inputs<Variable>),
    // player -> relation
    RolePlayerReverse(RolePlayer<Variable>, Inputs<Variable>),

    // $x --> $y
    // RolePlayerIndex(IR, IterateBounds)
    FunctionCallBinding(FunctionCallBinding<Variable>),

    // lhs derived from rhs. We need to decide if lhs will always be sorted
    ComparisonGenerator(Comparison<Variable>),
    // rhs derived from lhs
    ComparisonGeneratorReverse(Comparison<Variable>),
    // lhs and rhs are known
    ComparisonCheck(Comparison<Variable>),

    // vars = <expr>
    ExpressionBinding(ExpressionBinding<Variable>),
}

impl ConstraintInstruction {
    pub fn is_input_variable(&self, var: Variable) -> bool {
        let mut found = false;
        self.input_variables_foreach(|v| {
            if v == var {
                found = true;
            }
        });
        found
    }

    pub(crate) fn input_variables_foreach(&self, mut apply: impl FnMut(Variable)) {
        match self {
            ConstraintInstruction::Isa(_, inputs) => inputs.inputs().iter().cloned().for_each(apply),
            ConstraintInstruction::IsaReverse(_, inputs) => inputs.inputs().iter().cloned().for_each(apply),
            ConstraintInstruction::Has(_, inputs) | ConstraintInstruction::HasReverse(_, inputs) => {
                inputs.inputs().iter().cloned().for_each(apply)
            }
            ConstraintInstruction::RolePlayer(_, inputs) | ConstraintInstruction::RolePlayerReverse(_, inputs) => {
                inputs.inputs().iter().cloned().for_each(apply)
            }
            ConstraintInstruction::ComparisonCheck(_) => {}
            ConstraintInstruction::FunctionCallBinding(call) => call.function_call().argument_ids().for_each(apply),
            ConstraintInstruction::ComparisonGenerator(comparison) => apply(comparison.rhs()),
            ConstraintInstruction::ComparisonGeneratorReverse(comparison) => apply(comparison.lhs()),
            ConstraintInstruction::ExpressionBinding(binding) => binding.expression().variables().for_each(apply),
        }
    }

    pub(crate) fn new_variables_foreach(&self, mut apply: impl FnMut(Variable)) {
        match self {
            ConstraintInstruction::Isa(isa, inputs) | ConstraintInstruction::IsaReverse(isa, inputs) => isa.ids_foreach(|var, _| {
                if !inputs.inputs().iter().contains(&var) {
                    apply(var)
                }
            }),
            ConstraintInstruction::Has(has, inputs) | ConstraintInstruction::HasReverse(has, inputs) => has.ids_foreach(|var, _| {
                if !inputs.inputs().iter().contains(&var) {
                    apply(var)
                }
            }),
            ConstraintInstruction::RolePlayer(rp, inputs) | ConstraintInstruction::RolePlayerReverse(rp, inputs) => {
                rp.ids_foreach(|var, _| {
                    if !inputs.inputs().iter().contains(&var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::FunctionCallBinding(call) => call.ids_assigned().for_each(apply),
            ConstraintInstruction::ComparisonGenerator(comparison) => apply(comparison.lhs()),
            ConstraintInstruction::ComparisonGeneratorReverse(comparison) => apply(comparison.rhs()),
            ConstraintInstruction::ComparisonCheck(comparison) => {
                apply(comparison.lhs());
                apply(comparison.rhs())
            }
            ConstraintInstruction::ExpressionBinding(binding) => binding.ids_assigned().for_each(apply),
        }
    }
}

impl InstructionAPI for ConstraintInstruction {
    fn constraint(&self) -> Constraint<Variable> {
        match self {
            ConstraintInstruction::Isa(isa, _) | ConstraintInstruction::IsaReverse(isa, _) => isa.clone().into(),
            ConstraintInstruction::Has(has, _) | ConstraintInstruction::HasReverse(has, _) => has.clone().into(),
            ConstraintInstruction::RolePlayer(rp, _) | ConstraintInstruction::RolePlayerReverse(rp, _) => rp.clone().into(),
            ConstraintInstruction::FunctionCallBinding(call) => call.clone().into(),
            | ConstraintInstruction::ComparisonGenerator(cmp)
            | ConstraintInstruction::ComparisonGeneratorReverse(cmp)
            | ConstraintInstruction::ComparisonCheck(cmp) => cmp.clone().into(),
            ConstraintInstruction::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Inputs<ID: IrID> {
    None([ID; 0]),
    Single([ID; 1]),
    Dual([ID; 2]),
}

impl<ID: IrID> Inputs<ID> {
    pub(crate) fn contains(&self, id: ID) -> bool {
        self.inputs().contains(&id)
    }

    fn inputs(&self) -> &[ID] {
        match self {
            Inputs::None(ids) => ids,
            Inputs::Single(ids) => ids,
            Inputs::Dual(ids) => ids,
        }
    }

    pub fn into_ids<T: IrID>(self, mapping: &HashMap<ID, T>) -> Inputs<T> {
        match self {
            Inputs::None(_) => Inputs::None([]),
            Inputs::Single([var]) => Inputs::Single([*mapping.get(&var).unwrap()]),
            Inputs::Dual([var_1, var_2]) => {
                Inputs::Dual([*mapping.get(&var_1).unwrap(), *mapping.get(&var_2).unwrap()])
            }
        }
    }
}

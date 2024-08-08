/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::pattern::{
    constraint::{Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Isa, RolePlayer},
    IrID,
};
use itertools::Itertools;

use crate::{
    inference::type_annotations::{LeftRightFilteredAnnotations, TypeAnnotations},
    planner::pattern_plan::InstructionAPI,
};

#[derive(Debug, Clone)]
pub struct RolePlayerInstruction<ID> {
    pub constraint: RolePlayer<ID>,
    inputs: Inputs<ID>,
    edge_annotations: LeftRightFilteredAnnotations,
    end_type_annotations: Arc<HashSet<Type>>,
}

impl RolePlayerInstruction<Variable> {
    pub fn new(constraint: RolePlayer<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations = type_annotations
            .constraint_annotations_of(constraint.clone().into())
            .unwrap()
            .as_left_right_filtered()
            .clone();
        let end_type_annotations = type_annotations.variable_annotations_of(constraint.player()).unwrap().clone();
        Self { constraint, inputs, edge_annotations, end_type_annotations }
    }
}

impl<ID> RolePlayerInstruction<ID> {
    pub fn edge_types(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.edge_annotations.left_to_right()
    }

    pub fn end_types(&self) -> Arc<HashSet<Type>> {
        self.end_type_annotations.clone()
    }

    pub fn filter_types(&self) -> Arc<BTreeMap<Type, HashSet<Type>>> {
        self.edge_annotations.filters_on_right()
    }
}

impl<ID: IrID> RolePlayerInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> RolePlayerInstruction<T> {
        let Self { constraint, inputs, edge_annotations: type_annotations, end_type_annotations } = self;
        RolePlayerInstruction {
            constraint: constraint.map(mapping),
            inputs: inputs.map(mapping),
            edge_annotations: type_annotations,
            end_type_annotations,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintInstruction {
    // thing -> type
    Isa(Isa<Variable>, Inputs<Variable>),
    // type -> thing
    IsaReverse(Isa<Variable>, Inputs<Variable>),

    // owner -> attribute
    Has(Has<Variable>, Inputs<Variable>),
    // attribute -> owner
    HasReverse(Has<Variable>, Inputs<Variable>),

    // relation -> player
    RolePlayer(RolePlayerInstruction<Variable>),
    // player -> relation
    RolePlayerReverse(RolePlayer<Variable>, Inputs<Variable>),

    // $x --> $y
    // RolePlayerIndex(IR, IterateBounds)
    FunctionCallBinding(FunctionCallBinding<Variable>),

    // rhs derived from lhs. We need to decide if rhs will always be sorted
    ComparisonGenerator(Comparison<Variable>),
    // lhs derived from rhs
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
            | ConstraintInstruction::Isa(_, inputs)
            | ConstraintInstruction::IsaReverse(_, inputs)
            | ConstraintInstruction::Has(_, inputs)
            | ConstraintInstruction::HasReverse(_, inputs)
            | ConstraintInstruction::RolePlayer(RolePlayerInstruction { inputs, .. })
            | ConstraintInstruction::RolePlayerReverse(_, inputs) => inputs.iter().cloned().for_each(apply),
            ConstraintInstruction::ComparisonCheck(_) => {}
            ConstraintInstruction::FunctionCallBinding(call) => call.function_call().argument_ids().for_each(apply),
            ConstraintInstruction::ComparisonGenerator(comparison) => apply(comparison.rhs()),
            ConstraintInstruction::ComparisonGeneratorReverse(comparison) => apply(comparison.lhs()),
            ConstraintInstruction::ExpressionBinding(binding) => binding.expression().variables().for_each(apply),
        }
    }

    pub(crate) fn new_variables_foreach(&self, mut apply: impl FnMut(Variable)) {
        match self {
            ConstraintInstruction::Isa(isa, inputs) | ConstraintInstruction::IsaReverse(isa, inputs) => isa
                .ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                }),
            ConstraintInstruction::Has(has, inputs) | ConstraintInstruction::HasReverse(has, inputs) => has
                .ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                }),
            ConstraintInstruction::RolePlayer(RolePlayerInstruction { constraint, inputs, .. })
            | ConstraintInstruction::RolePlayerReverse(constraint, inputs) => constraint.ids_foreach(|var, _| {
                if !inputs.iter().contains(&var) {
                    apply(var)
                }
            }),
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
            ConstraintInstruction::RolePlayer(RolePlayerInstruction { constraint: rp, .. })
            | ConstraintInstruction::RolePlayerReverse(rp, _) => rp.clone().into(),
            ConstraintInstruction::FunctionCallBinding(call) => call.clone().into(),
            | ConstraintInstruction::ComparisonGenerator(cmp)
            | ConstraintInstruction::ComparisonGeneratorReverse(cmp)
            | ConstraintInstruction::ComparisonCheck(cmp) => cmp.clone().into(),
            ConstraintInstruction::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Inputs<ID> {
    None([ID; 0]),
    Single([ID; 1]),
    Dual([ID; 2]),
}

impl<ID: IrID> Inputs<ID> {
    pub(crate) fn contains(&self, id: ID) -> bool {
        self.deref().contains(&id)
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Inputs<T> {
        match self {
            Inputs::None(_) => Inputs::None([]),
            Inputs::Single([var]) => Inputs::Single([mapping[&var]]),
            Inputs::Dual([var_1, var_2]) => Inputs::Dual([mapping[&var_1], mapping[&var_2]]),
        }
    }
}

impl<ID: IrID> Deref for Inputs<ID> {
    type Target = [ID];

    fn deref(&self) -> &Self::Target {
        match self {
            Inputs::None(ids) => ids,
            Inputs::Single(ids) => ids,
            Inputs::Dual(ids) => ids,
        }
    }
}

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
    inference::type_annotations::{LeftRightAnnotations, LeftRightFilteredAnnotations, TypeAnnotations},
    planner::pattern_plan::InstructionAPI,
};

#[derive(Debug, Clone)]
pub enum ConstraintInstruction {
    // thing -> type
    Isa(Isa<Variable>, Inputs<Variable>),
    // type -> thing
    IsaReverse(IsaReverseInstruction<Variable>),

    // owner -> attribute
    Has(HasInstruction<Variable>),
    // attribute -> owner
    HasReverse(HasReverseInstruction<Variable>),

    // relation -> player
    RolePlayer(RolePlayerInstruction<Variable>),
    // player -> relation
    RolePlayerReverse(RolePlayerReverseInstruction<Variable>),

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
            | ConstraintInstruction::IsaReverse(IsaReverseInstruction { inputs, .. })
            | ConstraintInstruction::Has(HasInstruction { inputs, .. })
            | ConstraintInstruction::HasReverse(HasReverseInstruction { inputs, .. })
            | ConstraintInstruction::RolePlayer(RolePlayerInstruction { inputs, .. })
            | ConstraintInstruction::RolePlayerReverse(RolePlayerReverseInstruction { inputs, .. }) => {
                inputs.iter().cloned().for_each(apply)
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
            ConstraintInstruction::Isa(isa, inputs)
            | ConstraintInstruction::IsaReverse(IsaReverseInstruction { constraint: isa, inputs, .. }) => isa
                .ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                }),
            ConstraintInstruction::Has(HasInstruction { constraint: has, inputs, .. })
            | ConstraintInstruction::HasReverse(HasReverseInstruction { constraint: has, inputs, .. }) => has
                .ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                }),
            ConstraintInstruction::RolePlayer(RolePlayerInstruction { constraint: role_player, inputs, .. })
            | ConstraintInstruction::RolePlayerReverse(RolePlayerReverseInstruction {
                constraint: role_player,
                inputs,
                ..
            }) => role_player.ids_foreach(|var, _| {
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
            Self::Isa(isa, _) | Self::IsaReverse(IsaReverseInstruction { constraint: isa, .. }) => isa.clone().into(),
            Self::Has(HasInstruction { constraint: has, .. })
            | Self::HasReverse(HasReverseInstruction { constraint: has, .. }) => has.clone().into(),
            Self::RolePlayer(RolePlayerInstruction { constraint: rp, .. })
            | Self::RolePlayerReverse(RolePlayerReverseInstruction { constraint: rp, .. }) => rp.clone().into(),
            Self::FunctionCallBinding(call) => call.clone().into(),
            Self::ComparisonGenerator(cmp) | Self::ComparisonGeneratorReverse(cmp) | Self::ComparisonCheck(cmp) => {
                cmp.clone().into()
            }
            Self::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IsaReverseInstruction<ID> {
    pub constraint: Isa<ID>,
    pub inputs: Inputs<ID>,
    type_annotations: Arc<HashSet<Type>>,
}

impl IsaReverseInstruction<Variable> {
    pub fn new(constraint: Isa<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let type_annotations = type_annotations.variable_annotations_of(constraint.thing()).unwrap().clone();
        Self { constraint, inputs, type_annotations }
    }
}

impl<ID> IsaReverseInstruction<ID> {
    pub fn types(&self) -> Arc<HashSet<Type>> {
        self.type_annotations.clone()
    }
}

impl<ID: IrID> IsaReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IsaReverseInstruction<T> {
        let Self { constraint, inputs, type_annotations } = self;
        IsaReverseInstruction { constraint: constraint.map(mapping), inputs: inputs.map(mapping), type_annotations }
    }
}

#[derive(Debug, Clone)]
pub struct HasInstruction<ID> {
    pub constraint: Has<ID>,
    inputs: Inputs<ID>,
    edge_annotations: LeftRightAnnotations,
    end_type_annotations: Arc<HashSet<Type>>,
}

impl HasInstruction<Variable> {
    pub fn new(constraint: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(constraint.clone().into()).unwrap().as_left_right().clone();
        let end_type_annotations = type_annotations.variable_annotations_of(constraint.attribute()).unwrap().clone();
        Self { constraint, inputs, edge_annotations, end_type_annotations }
    }
}

impl<ID> HasInstruction<ID> {
    pub fn edge_types(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.edge_annotations.left_to_right()
    }

    pub fn end_types(&self) -> Arc<HashSet<Type>> {
        self.end_type_annotations.clone()
    }
}

impl<ID: IrID> HasInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasInstruction<T> {
        let Self { constraint, inputs, edge_annotations: type_annotations, end_type_annotations } = self;
        HasInstruction {
            constraint: constraint.map(mapping),
            inputs: inputs.map(mapping),
            edge_annotations: type_annotations,
            end_type_annotations,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HasReverseInstruction<ID> {
    pub constraint: Has<ID>,
    pub inputs: Inputs<ID>,
    edge_annotations: LeftRightAnnotations,
    end_type_annotations: Arc<HashSet<Type>>,
}

impl HasReverseInstruction<Variable> {
    pub fn new(constraint: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(constraint.clone().into()).unwrap().as_left_right().clone();
        let end_type_annotations = type_annotations.variable_annotations_of(constraint.owner()).unwrap().clone();
        Self { constraint, inputs, edge_annotations, end_type_annotations }
    }
}

impl<ID> HasReverseInstruction<ID> {
    pub fn edge_types(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.edge_annotations.right_to_left()
    }

    pub fn end_types(&self) -> Arc<HashSet<Type>> {
        self.end_type_annotations.clone()
    }
}

impl<ID: IrID> HasReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasReverseInstruction<T> {
        let Self { constraint, inputs, edge_annotations: type_annotations, end_type_annotations } = self;
        HasReverseInstruction {
            constraint: constraint.map(mapping),
            inputs: inputs.map(mapping),
            edge_annotations: type_annotations,
            end_type_annotations,
        }
    }
}

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
pub struct RolePlayerReverseInstruction<ID> {
    pub constraint: RolePlayer<ID>,
    pub inputs: Inputs<ID>,
    edge_annotations: LeftRightFilteredAnnotations,
    end_type_annotations: Arc<HashSet<Type>>,
}

impl RolePlayerReverseInstruction<Variable> {
    pub fn new(constraint: RolePlayer<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations = type_annotations
            .constraint_annotations_of(constraint.clone().into())
            .unwrap()
            .as_left_right_filtered()
            .clone();
        let end_type_annotations = type_annotations.variable_annotations_of(constraint.relation()).unwrap().clone();
        Self { constraint, inputs, edge_annotations, end_type_annotations }
    }
}

impl<ID> RolePlayerReverseInstruction<ID> {
    pub fn edge_types(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.edge_annotations.right_to_left()
    }

    pub fn end_types(&self) -> Arc<HashSet<Type>> {
        self.end_type_annotations.clone()
    }

    pub fn filter_types(&self) -> Arc<BTreeMap<Type, HashSet<Type>>> {
        self.edge_annotations.filters_on_left()
    }
}

impl<ID: IrID> RolePlayerReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> RolePlayerReverseInstruction<T> {
        let Self { constraint, inputs, edge_annotations: type_annotations, end_type_annotations } = self;
        RolePlayerReverseInstruction {
            constraint: constraint.map(mapping),
            inputs: inputs.map(mapping),
            edge_annotations: type_annotations,
            end_type_annotations,
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

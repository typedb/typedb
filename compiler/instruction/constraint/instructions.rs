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
    constraint::{Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Isa, Links},
    IrID,
};
use itertools::Itertools;

use crate::{inference::type_annotations::TypeAnnotations, planner::pattern_plan::InstructionAPI};

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
    Links(LinksInstruction<Variable>),
    // player -> relation
    LinksReverse(LinksReverseInstruction<Variable>),

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
            | ConstraintInstruction::Links(LinksInstruction { inputs, .. })
            | ConstraintInstruction::LinksReverse(LinksReverseInstruction { inputs, .. }) => {
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
            | ConstraintInstruction::IsaReverse(IsaReverseInstruction { isa, inputs, .. }) => {
                isa.ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::Has(HasInstruction { has, inputs, .. })
            | ConstraintInstruction::HasReverse(HasReverseInstruction { has, inputs, .. }) => {
                has.ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::Links(LinksInstruction { links, inputs, .. })
            | ConstraintInstruction::LinksReverse(LinksReverseInstruction { links, inputs, .. }) => {
                links.ids_foreach(|var, _| {
                    if !inputs.iter().contains(&var) {
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
            Self::Isa(isa, _) | Self::IsaReverse(IsaReverseInstruction { isa, .. }) => isa.clone().into(),
            Self::Has(HasInstruction { has, .. }) | Self::HasReverse(HasReverseInstruction { has, .. }) => {
                has.clone().into()
            }
            Self::Links(LinksInstruction { links, .. }) | Self::LinksReverse(LinksReverseInstruction { links, .. }) => {
                links.clone().into()
            }
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
    pub isa: Isa<ID>,
    pub inputs: Inputs<ID>,
    types: Arc<HashSet<Type>>,
}

impl IsaReverseInstruction<Variable> {
    pub fn new(constraint: Isa<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let types = type_annotations.variable_annotations_of(constraint.thing()).unwrap().clone();
        Self { isa: constraint, inputs, types }
    }
}

impl<ID> IsaReverseInstruction<ID> {
    pub fn types(&self) -> &Arc<HashSet<Type>> {
        &self.types
    }
}

impl<ID: IrID> IsaReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IsaReverseInstruction<T> {
        let Self { isa: constraint, inputs, types } = self;
        IsaReverseInstruction { isa: constraint.map(mapping), inputs: inputs.map(mapping), types }
    }
}

#[derive(Debug, Clone)]
pub struct HasInstruction<ID> {
    pub has: Has<ID>,
    inputs: Inputs<ID>,
    owner_to_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<HashSet<Type>>,
}

impl HasInstruction<Variable> {
    pub fn new(constraint: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(constraint.clone().into()).unwrap().as_left_right();
        let owner_to_attribute_types = edge_annotations.left_to_right();
        let attribute_types = type_annotations.variable_annotations_of(constraint.attribute()).unwrap().clone();
        Self { has: constraint, inputs, owner_to_attribute_types, attribute_types }
    }
}

impl<ID> HasInstruction<ID> {
    pub fn owner_to_attribute_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.owner_to_attribute_types
    }

    pub fn attribute_types(&self) -> &Arc<HashSet<Type>> {
        &self.attribute_types
    }
}

impl<ID: IrID> HasInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasInstruction<T> {
        let Self { has: constraint, inputs, owner_to_attribute_types, attribute_types } = self;
        HasInstruction {
            has: constraint.map(mapping),
            inputs: inputs.map(mapping),
            owner_to_attribute_types,
            attribute_types,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HasReverseInstruction<ID> {
    pub has: Has<ID>,
    pub inputs: Inputs<ID>,
    attribute_to_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<HashSet<Type>>,
}

impl HasReverseInstruction<Variable> {
    pub fn new(has: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations = &type_annotations.constraint_annotations_of(has.clone().into()).unwrap().as_left_right();
        let attribute_to_owner_types = edge_annotations.right_to_left().clone();
        let owner_types = type_annotations.variable_annotations_of(has.owner()).unwrap().clone();
        Self { has, inputs, attribute_to_owner_types, owner_types }
    }
}

impl<ID> HasReverseInstruction<ID> {
    pub fn attribute_to_owner_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.attribute_to_owner_types
    }

    pub fn owner_types(&self) -> &Arc<HashSet<Type>> {
        &self.owner_types
    }
}

impl<ID: IrID> HasReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasReverseInstruction<T> {
        let Self { has: constraint, inputs, attribute_to_owner_types, owner_types } = self;
        HasReverseInstruction {
            has: constraint.map(mapping),
            inputs: inputs.map(mapping),
            attribute_to_owner_types,
            owner_types,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LinksInstruction<ID> {
    pub links: Links<ID>,
    inputs: Inputs<ID>,
    relation_to_player_types: Arc<BTreeMap<Type, Vec<Type>>>,
    player_to_role_types: Arc<BTreeMap<Type, HashSet<Type>>>,
    player_types: Arc<HashSet<Type>>,
}

impl LinksInstruction<Variable> {
    pub fn new(links: Links<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_left_right_filtered();
        let player_to_role_types = edge_annotations.filters_on_right();
        let relation_to_player_types = edge_annotations.left_to_right();
        let player_types = type_annotations.variable_annotations_of(links.player()).unwrap().clone();
        Self { links, inputs, relation_to_player_types, player_types, player_to_role_types }
    }
}

impl<ID> LinksInstruction<ID> {
    pub fn relation_to_player_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.relation_to_player_types
    }

    pub fn player_types(&self) -> &Arc<HashSet<Type>> {
        &self.player_types
    }

    pub fn relation_to_role_types(&self) -> &Arc<BTreeMap<Type, HashSet<Type>>> {
        &self.player_to_role_types
    }
}

impl<ID: IrID> LinksInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> LinksInstruction<T> {
        let Self { links, inputs, relation_to_player_types, player_types, player_to_role_types } = self;
        LinksInstruction {
            links: links.map(mapping),
            inputs: inputs.map(mapping),
            relation_to_player_types,
            player_types,
            player_to_role_types,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LinksReverseInstruction<ID> {
    pub links: Links<ID>,
    pub inputs: Inputs<ID>,
    player_to_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_to_role_types: Arc<BTreeMap<Type, HashSet<Type>>>,
    relation_types: Arc<HashSet<Type>>,
}

impl LinksReverseInstruction<Variable> {
    pub fn new(links: Links<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_left_right_filtered().clone();
        let relation_to_role_types = edge_annotations.filters_on_right();
        let player_to_relation_types = edge_annotations.right_to_left();
        let relation_types = type_annotations.variable_annotations_of(links.relation()).unwrap().clone();
        Self { links, inputs, player_to_relation_types, relation_types, relation_to_role_types }
    }
}

impl<ID> LinksReverseInstruction<ID> {
    pub fn player_to_relation_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.player_to_relation_types
    }

    pub fn relation_types(&self) -> &Arc<HashSet<Type>> {
        &self.relation_types
    }

    pub fn relation_to_role_types(&self) -> &Arc<BTreeMap<Type, HashSet<Type>>> {
        &self.relation_to_role_types
    }
}

impl<ID: IrID> LinksReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> LinksReverseInstruction<T> {
        let Self { links, inputs, player_to_relation_types, relation_to_role_types, relation_types } = self;
        LinksReverseInstruction {
            links: links.map(mapping),
            inputs: inputs.map(mapping),
            player_to_relation_types,
            relation_to_role_types,
            relation_types,
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

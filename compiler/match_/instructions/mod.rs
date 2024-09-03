/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, ops::Deref};

use ir::pattern::{
    constraint::{Comparator, Comparison, Constraint, ExpressionBinding, FunctionCallBinding},
    IrID,
};

use crate::match_::planner::pattern_plan::InstructionAPI;

pub mod thing;
pub mod type_;

#[derive(Debug, Clone)]
pub enum ConstraintInstruction<ID> {
    // sub -> super
    Sub(type_::SubInstruction<ID>),

    // thing -> type
    Isa(thing::IsaInstruction<ID>),
    // type -> thing
    IsaReverse(thing::IsaReverseInstruction<ID>),

    // owner -> attribute
    Has(thing::HasInstruction<ID>),
    // attribute -> owner
    HasReverse(thing::HasReverseInstruction<ID>),

    // relation -> player
    Links(thing::LinksInstruction<ID>),
    // player -> relation
    LinksReverse(thing::LinksReverseInstruction<ID>),

    // $x --> $y
    // RolePlayerIndex(IR, IterateBounds)
    FunctionCallBinding(FunctionCallBinding<ID>),

    // rhs derived from lhs. We need to decide if rhs will always be sorted
    ComparisonGenerator(Comparison<ID>),
    // lhs derived from rhs
    ComparisonGeneratorReverse(Comparison<ID>),
    // lhs and rhs are known
    ComparisonCheck(Comparison<ID>),

    // vars = <expr>
    ExpressionBinding(ExpressionBinding<ID>),
}

impl<ID: IrID> ConstraintInstruction<ID> {
    pub fn is_input_variable(&self, var: ID) -> bool {
        let mut found = false;
        self.input_variables_foreach(|v| {
            if v == var {
                found = true;
            }
        });
        found
    }

    pub(crate) fn input_variables_foreach(&self, mut apply: impl FnMut(ID)) {
        match self {
            | ConstraintInstruction::Sub(type_::SubInstruction { inputs, .. })
            | ConstraintInstruction::Isa(thing::IsaInstruction { inputs, .. })
            | ConstraintInstruction::IsaReverse(thing::IsaReverseInstruction { inputs, .. })
            | ConstraintInstruction::Has(thing::HasInstruction { inputs, .. })
            | ConstraintInstruction::HasReverse(thing::HasReverseInstruction { inputs, .. })
            | ConstraintInstruction::Links(thing::LinksInstruction { inputs, .. })
            | ConstraintInstruction::LinksReverse(thing::LinksReverseInstruction { inputs, .. }) => {
                inputs.iter().cloned().for_each(apply)
            }
            ConstraintInstruction::ComparisonCheck(_) => {}
            ConstraintInstruction::FunctionCallBinding(call) => call.function_call().argument_ids().for_each(apply),
            ConstraintInstruction::ComparisonGenerator(comparison) => apply(comparison.rhs()),
            ConstraintInstruction::ComparisonGeneratorReverse(comparison) => apply(comparison.lhs()),
            ConstraintInstruction::ExpressionBinding(binding) => binding.expression().variables().for_each(apply),
        }
    }

    pub(crate) fn new_variables_foreach(&self, mut apply: impl FnMut(ID)) {
        match self {
            ConstraintInstruction::Sub(type_::SubInstruction { sub, inputs, .. })
            /*| ConstraintInstruction::IsaReverse(thing::IsaReverseInstruction { isa, inputs, .. })*/ => {
                sub.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::Isa(thing::IsaInstruction { isa, inputs, .. })
            | ConstraintInstruction::IsaReverse(thing::IsaReverseInstruction { isa, inputs, .. }) => {
                isa.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::Has(thing::HasInstruction { has, inputs, .. })
            | ConstraintInstruction::HasReverse(thing::HasReverseInstruction { has, inputs, .. }) => {
                has.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            ConstraintInstruction::Links(thing::LinksInstruction { links, inputs, .. })
            | ConstraintInstruction::LinksReverse(thing::LinksReverseInstruction { links, inputs, .. }) => links
                .ids_foreach(|var, _| {
                    if !inputs.contains(var) {
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

    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        match self {
            Self::Sub(inner) => inner.add_check(check),
            Self::Isa(inner) => inner.add_check(check),
            Self::IsaReverse(inner) => inner.add_check(check),
            Self::Has(inner) => inner.add_check(check),
            Self::HasReverse(inner) => inner.add_check(check),
            Self::Links(inner) => inner.add_check(check),
            Self::LinksReverse(inner) => inner.add_check(check),
            Self::FunctionCallBinding(_) => todo!(),
            Self::ComparisonGenerator(_) => todo!(),
            Self::ComparisonGeneratorReverse(_) => todo!(),
            Self::ComparisonCheck(_) => todo!(),
            Self::ExpressionBinding(_) => todo!(),
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ConstraintInstruction<T> {
        match self {
            Self::Sub(inner) => ConstraintInstruction::Sub(inner.map(mapping)),
            Self::Isa(inner) => ConstraintInstruction::Isa(inner.map(mapping)),
            Self::IsaReverse(inner) => ConstraintInstruction::IsaReverse(inner.map(mapping)),
            Self::Has(inner) => ConstraintInstruction::Has(inner.map(mapping)),
            Self::HasReverse(inner) => ConstraintInstruction::HasReverse(inner.map(mapping)),
            Self::Links(inner) => ConstraintInstruction::Links(inner.map(mapping)),
            Self::LinksReverse(inner) => ConstraintInstruction::LinksReverse(inner.map(mapping)),
            Self::FunctionCallBinding(_) => todo!(),
            Self::ComparisonGenerator(_) => todo!(),
            Self::ComparisonGeneratorReverse(_) => todo!(),
            Self::ComparisonCheck(_) => todo!(),
            Self::ExpressionBinding(_) => todo!(),
        }
    }
}

impl<ID: Copy> InstructionAPI<ID> for ConstraintInstruction<ID> {
    fn constraint(&self) -> Constraint<ID> {
        match self {
            Self::Sub(type_::SubInstruction { sub, .. }) => sub.clone().into(),
            Self::Isa(thing::IsaInstruction { isa, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, .. }) => isa.clone().into(),
            Self::Has(thing::HasInstruction { has, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, .. }) => has.clone().into(),
            Self::Links(thing::LinksInstruction { links, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, .. }) => links.clone().into(),
            Self::FunctionCallBinding(call) => call.clone().into(),
            Self::ComparisonGenerator(cmp) | Self::ComparisonGeneratorReverse(cmp) | Self::ComparisonCheck(cmp) => {
                cmp.clone().into()
            }
            Self::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CheckInstruction<ID> {
    Comparison { lhs: ID, rhs: ID, comparator: Comparator },
    Has { owner: ID, attribute: ID },
    Links { relation: ID, player: ID, role: ID },
}

impl<ID: IrID> CheckInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> CheckInstruction<T> {
        match self {
            Self::Comparison { lhs, rhs, comparator } => {
                CheckInstruction::Comparison { lhs: mapping[&lhs], rhs: mapping[&rhs], comparator }
            }
            Self::Has { owner, attribute } => {
                CheckInstruction::Has { owner: mapping[&owner], attribute: mapping[&attribute] }
            }
            Self::Links { relation, player, role } => {
                CheckInstruction::Links { relation: mapping[&relation], player: mapping[&player], role: mapping[&role] }
            }
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

impl<ID> Deref for Inputs<ID> {
    type Target = [ID];

    fn deref(&self) -> &Self::Target {
        match self {
            Inputs::None(ids) => ids,
            Inputs::Single(ids) => ids,
            Inputs::Dual(ids) => ids,
        }
    }
}

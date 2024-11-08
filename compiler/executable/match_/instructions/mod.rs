/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(clippy::large_enum_variant)]

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use answer::{variable::Variable, Type};
use ir::pattern::{
    constraint::{Comparator, Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Is, IsaKind, SubKind},
    IrID, ParameterID, Vertex,
};
use itertools::Itertools;

use crate::{
    annotation::type_annotations::TypeAnnotations, executable::match_::planner::match_executable::InstructionAPI,
    ExecutorVariable, VariablePosition,
};

pub mod thing;
pub mod type_;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum VariableMode {
    Input,
    Output,
    Count,
    Check,
}

impl VariableMode {
    const fn new(is_input: bool, is_selected: bool, is_named: bool) -> VariableMode {
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

#[derive(Clone, Debug)]
pub struct VariableModes {
    modes: HashMap<ExecutorVariable, VariableMode>,
}

impl VariableModes {
    fn new() -> Self {
        VariableModes { modes: HashMap::new() }
    }

    pub(crate) fn new_for(
        instruction: &ConstraintInstruction<ExecutorVariable>,
        selected: &[VariablePosition],
        named: &HashSet<ExecutorVariable>,
    ) -> Self {
        let mut modes = Self::new();
        instruction.used_variables_foreach(|var| {
            let var_mode = match var {
                ExecutorVariable::Internal(_) => {
                    VariableMode::new(instruction.is_input_variable(var), false, named.contains(&var))
                }
                ExecutorVariable::RowPosition(pos) => {
                    VariableMode::new(instruction.is_input_variable(var), selected.contains(&pos), named.contains(&var))
                }
            };
            modes.insert(var, var_mode);
        });
        modes
    }

    fn insert(&mut self, variable_position: ExecutorVariable, mode: VariableMode) {
        let existing = self.modes.insert(variable_position, mode);
        debug_assert!(existing.is_none())
    }

    pub fn get(&self, variable_position: ExecutorVariable) -> Option<VariableMode> {
        self.modes.get(&variable_position).copied()
    }

    pub fn all_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode == &VariableMode::Input)
    }

    pub fn none_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode != &VariableMode::Input)
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintInstruction<ID> {
    Is(IsInstruction<ID>),

    TypeList(type_::TypeListInstruction<ID>),

    // sub -> super
    Sub(type_::SubInstruction<ID>),
    // super -> sub
    SubReverse(type_::SubReverseInstruction<ID>),

    // owner -> attribute
    Owns(type_::OwnsInstruction<ID>),
    // attribute -> owner
    OwnsReverse(type_::OwnsReverseInstruction<ID>),

    // relation -> role_type
    Relates(type_::RelatesInstruction<ID>),
    // role_type -> relation
    RelatesReverse(type_::RelatesReverseInstruction<ID>),

    // player -> role_type
    Plays(type_::PlaysInstruction<ID>),
    // role_type -> player
    PlaysReverse(type_::PlaysReverseInstruction<ID>),

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

    pub fn is_new_variable(&self, var: ID) -> bool {
        let mut found = false;
        self.new_variables_foreach(|v| {
            if v == var {
                found = true;
            }
        });
        found
    }

    pub fn used_variables_foreach(&self, mut apply: impl FnMut(ID)) {
        match self {
            Self::Is(IsInstruction { is, .. }) => is.ids_foreach(|var, _| apply(var)),
            &Self::TypeList(type_::TypeListInstruction { type_var, .. }) => apply(type_var),
            Self::Sub(type_::SubInstruction { sub, .. })
            | Self::SubReverse(type_::SubReverseInstruction { sub, .. }) => sub.ids_foreach(|var, _| apply(var)),
            Self::Owns(type_::OwnsInstruction { owns, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { owns, .. }) => owns.ids_foreach(|var, _| apply(var)),
            Self::Relates(type_::RelatesInstruction { relates, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { relates, .. }) => {
                relates.ids_foreach(|var, _| apply(var))
            }
            Self::Plays(type_::PlaysInstruction { plays, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { plays, .. }) => {
                plays.ids_foreach(|var, _| apply(var))
            }
            Self::Isa(thing::IsaInstruction { isa, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, .. }) => isa.ids_foreach(|var, _| apply(var)),
            Self::Has(thing::HasInstruction { has, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, .. }) => has.ids_foreach(|var, _| apply(var)),
            Self::Links(thing::LinksInstruction { links, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, .. }) => {
                links.ids_foreach(|var, _| apply(var))
            }
            Self::FunctionCallBinding(call) => call.ids_assigned().for_each(apply),
            Self::ComparisonCheck(comparison) => {
                comparison.lhs().as_variable().map(&mut apply);
                comparison.rhs().as_variable().map(apply);
            }
            Self::ExpressionBinding(binding) => binding.ids_assigned().for_each(apply),
        }
    }

    pub(crate) fn input_variables_foreach(&self, apply: impl FnMut(ID)) {
        match self {
            Self::TypeList(_) => (),
            | Self::Is(IsInstruction { inputs, .. })
            | Self::Sub(type_::SubInstruction { inputs, .. })
            | Self::SubReverse(type_::SubReverseInstruction { inputs, .. })
            | Self::Owns(type_::OwnsInstruction { inputs, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { inputs, .. })
            | Self::Relates(type_::RelatesInstruction { inputs, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { inputs, .. })
            | Self::Plays(type_::PlaysInstruction { inputs, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { inputs, .. })
            | Self::Isa(thing::IsaInstruction { inputs, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { inputs, .. })
            | Self::Has(thing::HasInstruction { inputs, .. })
            | Self::HasReverse(thing::HasReverseInstruction { inputs, .. })
            | Self::Links(thing::LinksInstruction { inputs, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { inputs, .. }) => {
                inputs.iter().cloned().for_each(apply)
            }
            Self::ComparisonCheck(_) => (),
            Self::FunctionCallBinding(call) => call.function_call().argument_ids().for_each(apply),
            Self::ExpressionBinding(binding) => binding.expression().variables().for_each(apply),
        }
    }

    pub(crate) fn new_variables_foreach(&self, mut apply: impl FnMut(ID)) {
        match self {
            &Self::TypeList(type_::TypeListInstruction { type_var, .. }) => apply(type_var),
            Self::Is(IsInstruction { is, inputs, .. }) => is.ids_foreach(|var, _| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Sub(type_::SubInstruction { sub, inputs, .. })
            | Self::SubReverse(type_::SubReverseInstruction { sub, inputs, .. }) => sub.ids_foreach(|var, _| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Owns(type_::OwnsInstruction { owns, inputs, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { owns, inputs, .. }) => owns.ids_foreach(|var, _| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Relates(type_::RelatesInstruction { relates, inputs, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { relates, inputs, .. }) => {
                relates.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            Self::Plays(type_::PlaysInstruction { plays, inputs, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { plays, inputs, .. }) => {
                plays.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            Self::Isa(thing::IsaInstruction { isa, inputs, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, inputs, .. }) => isa.ids_foreach(|var, _| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Has(thing::HasInstruction { has, inputs, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, inputs, .. }) => has.ids_foreach(|var, _| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Links(thing::LinksInstruction { links, inputs, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, inputs, .. }) => {
                links.ids_foreach(|var, _| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            Self::FunctionCallBinding(call) => call.ids_assigned().for_each(apply),
            Self::ComparisonCheck(comparison) => {
                comparison.lhs().as_variable().map(&mut apply);
                comparison.rhs().as_variable().map(apply);
            }
            Self::ExpressionBinding(binding) => binding.ids_assigned().for_each(apply),
        }
    }

    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        match self {
            Self::Is(inner) => inner.add_check(check),
            Self::TypeList(inner) => inner.add_check(check),
            Self::Sub(inner) => inner.add_check(check),
            Self::SubReverse(inner) => inner.add_check(check),
            Self::Owns(inner) => inner.add_check(check),
            Self::OwnsReverse(inner) => inner.add_check(check),
            Self::Relates(inner) => inner.add_check(check),
            Self::RelatesReverse(inner) => inner.add_check(check),
            Self::Plays(inner) => inner.add_check(check),
            Self::PlaysReverse(inner) => inner.add_check(check),
            Self::Isa(inner) => inner.add_check(check),
            Self::IsaReverse(inner) => inner.add_check(check),
            Self::Has(inner) => inner.add_check(check),
            Self::HasReverse(inner) => inner.add_check(check),
            Self::Links(inner) => inner.add_check(check),
            Self::LinksReverse(inner) => inner.add_check(check),
            Self::FunctionCallBinding(_) => todo!(),
            Self::ComparisonCheck(_) => todo!(),
            Self::ExpressionBinding(_) => todo!(),
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ConstraintInstruction<T> {
        match self {
            Self::Is(inner) => ConstraintInstruction::Is(inner.map(mapping)),
            Self::TypeList(inner) => ConstraintInstruction::TypeList(inner.map(mapping)),
            Self::Sub(inner) => ConstraintInstruction::Sub(inner.map(mapping)),
            Self::SubReverse(inner) => ConstraintInstruction::SubReverse(inner.map(mapping)),
            Self::Owns(inner) => ConstraintInstruction::Owns(inner.map(mapping)),
            Self::OwnsReverse(inner) => ConstraintInstruction::OwnsReverse(inner.map(mapping)),
            Self::Relates(inner) => ConstraintInstruction::Relates(inner.map(mapping)),
            Self::RelatesReverse(inner) => ConstraintInstruction::RelatesReverse(inner.map(mapping)),
            Self::Plays(inner) => ConstraintInstruction::Plays(inner.map(mapping)),
            Self::PlaysReverse(inner) => ConstraintInstruction::PlaysReverse(inner.map(mapping)),
            Self::Isa(inner) => ConstraintInstruction::Isa(inner.map(mapping)),
            Self::IsaReverse(inner) => ConstraintInstruction::IsaReverse(inner.map(mapping)),
            Self::Has(inner) => ConstraintInstruction::Has(inner.map(mapping)),
            Self::HasReverse(inner) => ConstraintInstruction::HasReverse(inner.map(mapping)),
            Self::Links(inner) => ConstraintInstruction::Links(inner.map(mapping)),
            Self::LinksReverse(inner) => ConstraintInstruction::LinksReverse(inner.map(mapping)),
            Self::FunctionCallBinding(_) => todo!(),
            Self::ComparisonCheck(_) => todo!(),
            Self::ExpressionBinding(_) => todo!(),
        }
    }
}

impl<ID: IrID + Copy> InstructionAPI<ID> for ConstraintInstruction<ID> {
    fn constraint(&self) -> Constraint<ID> {
        match self {
            Self::Is(IsInstruction { is, .. }) => is.clone().into(),
            Self::TypeList(_) => todo!(), // TODO underlying constraint?
            Self::Sub(type_::SubInstruction { sub, .. })
            | Self::SubReverse(type_::SubReverseInstruction { sub, .. }) => sub.clone().into(),
            Self::Owns(type_::OwnsInstruction { owns, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { owns, .. }) => owns.clone().into(),
            Self::Relates(type_::RelatesInstruction { relates, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { relates, .. }) => relates.clone().into(),
            Self::Plays(type_::PlaysInstruction { plays, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { plays, .. }) => plays.clone().into(),
            Self::Isa(thing::IsaInstruction { isa, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, .. }) => isa.clone().into(),
            Self::Has(thing::HasInstruction { has, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, .. }) => has.clone().into(),
            Self::Links(thing::LinksInstruction { links, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, .. }) => links.clone().into(),
            Self::FunctionCallBinding(call) => call.clone().into(),
            Self::ComparisonCheck(cmp) => cmp.clone().into(),
            Self::ExpressionBinding(binding) => binding.clone().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IsInstruction<ID> {
    pub is: Is<ID>,
    pub inputs: Inputs<ID>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl IsInstruction<Variable> {
    pub(crate) fn new(is: Is<Variable>, inputs: Inputs<Variable>) -> Self {
        Self { is, inputs, checks: Vec::new() }
    }
}

impl<ID> IsInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }
}

impl<ID: IrID> IsInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IsInstruction<T> {
        let Self { is, inputs, checks } = self;
        IsInstruction {
            is: is.map(mapping),
            inputs: inputs.map(mapping),
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CheckVertex<ID> {
    Variable(ID),
    Type(Type),
    Parameter(ParameterID),
}

impl CheckVertex<ExecutorVariable> {
    pub(crate) fn resolve(vertex: Vertex<ExecutorVariable>, type_annotations: &TypeAnnotations) -> Self {
        match vertex {
            Vertex::Variable(var) => Self::Variable(var),
            Vertex::Parameter(param) => Self::Parameter(param),
            Vertex::Label(label) => Self::Type(
                type_annotations
                    .vertex_annotations_of(&Vertex::Label(label))
                    .unwrap()
                    .iter()
                    .exactly_one()
                    .unwrap()
                    .clone(),
            ),
        }
    }
}

impl<ID: IrID> CheckVertex<ID> {
    /// Returns `true` if the check vertex is [`Variable`].
    ///
    /// [`Variable`]: CheckVertex::Variable
    #[must_use]
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::Variable(..))
    }

    pub fn as_variable(&self) -> Option<ID> {
        if let &Self::Variable(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the check vertex is [`Type`].
    ///
    /// [`Type`]: CheckVertex::Type
    #[must_use]
    pub fn is_type(&self) -> bool {
        matches!(self, Self::Type(..))
    }

    pub fn as_type(&self) -> Option<&Type> {
        if let Self::Type(v) = self {
            Some(v)
        } else {
            None
        }
    }

    /// Returns `true` if the check vertex is [`Parameter`].
    ///
    /// [`Parameter`]: CheckVertex::Parameter
    #[must_use]
    pub fn is_parameter(&self) -> bool {
        matches!(self, Self::Parameter(..))
    }

    pub fn as_parameter(&self) -> Option<ParameterID> {
        if let &Self::Parameter(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> CheckVertex<T> {
        match self {
            Self::Variable(var) => CheckVertex::Variable(mapping[&var]),
            Self::Type(type_) => CheckVertex::Type(type_),
            Self::Parameter(param) => CheckVertex::Parameter(param),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CheckInstruction<ID> {
    TypeList { type_var: ID, types: Vec<Type> },

    Sub { sub_kind: SubKind, subtype: CheckVertex<ID>, supertype: CheckVertex<ID> },
    Owns { owner: CheckVertex<ID>, attribute: CheckVertex<ID> },
    Relates { relation: CheckVertex<ID>, role_type: CheckVertex<ID> },
    Plays { player: CheckVertex<ID>, role_type: CheckVertex<ID> },

    Isa { isa_kind: IsaKind, type_: CheckVertex<ID>, thing: CheckVertex<ID> },
    Has { owner: CheckVertex<ID>, attribute: CheckVertex<ID> },
    Links { relation: CheckVertex<ID>, player: CheckVertex<ID>, role: CheckVertex<ID> },

    Is { lhs: ID, rhs: ID },
    Comparison { lhs: CheckVertex<ID>, rhs: CheckVertex<ID>, comparator: Comparator },
}

impl<ID: IrID> CheckInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> CheckInstruction<T> {
        match self {
            Self::TypeList { type_var: var, types } => CheckInstruction::TypeList { type_var: mapping[&var], types },
            Self::Sub { sub_kind: kind, subtype, supertype } => CheckInstruction::Sub {
                sub_kind: kind,
                subtype: subtype.map(mapping),
                supertype: supertype.map(mapping),
            },
            Self::Owns { owner, attribute } => {
                CheckInstruction::Owns { owner: owner.map(mapping), attribute: attribute.map(mapping) }
            }
            Self::Relates { relation, role_type } => {
                CheckInstruction::Relates { relation: relation.map(mapping), role_type: role_type.map(mapping) }
            }
            Self::Plays { player, role_type } => {
                CheckInstruction::Plays { player: player.map(mapping), role_type: role_type.map(mapping) }
            }
            Self::Isa { isa_kind: kind, type_, thing } => {
                CheckInstruction::Isa { isa_kind: kind, type_: type_.map(mapping), thing: thing.map(mapping) }
            }
            Self::Has { owner, attribute } => {
                CheckInstruction::Has { owner: owner.map(mapping), attribute: attribute.map(mapping) }
            }
            Self::Links { relation, player, role } => CheckInstruction::Links {
                relation: relation.map(mapping),
                player: player.map(mapping),
                role: role.map(mapping),
            },
            Self::Is { lhs, rhs } => CheckInstruction::Is { lhs: mapping[&lhs], rhs: mapping[&rhs] },
            Self::Comparison { lhs, rhs, comparator } => {
                CheckInstruction::Comparison { lhs: lhs.map(mapping), rhs: rhs.map(mapping), comparator }
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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(clippy::large_enum_variant)]

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    ops::Deref,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::pattern::{
    constraint::{Comparator, Is, IsaKind, SubKind},
    IrID, ParameterID, Vertex,
};
use itertools::Itertools;

use crate::{annotation::type_annotations::TypeAnnotations, ExecutorVariable, VariablePosition};

pub mod thing;
pub mod type_;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
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

impl fmt::Display for VariableMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VariableMode::Input => write!(f, "input"),
            VariableMode::Output => write!(f, "output"),
            VariableMode::Count => write!(f, "count"),
            VariableMode::Check => write!(f, "check"),
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
        debug_assert!(existing.is_none() || existing == Some(mode))
    }

    pub fn get(&self, variable_position: ExecutorVariable) -> Option<VariableMode> {
        self.modes.get(&variable_position).copied()
    }

    pub fn len(&self) -> usize {
        self.modes.len()
    }

    pub fn all_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode == &VariableMode::Input)
    }

    pub fn none_inputs(&self) -> bool {
        self.modes.values().all(|mode| mode != &VariableMode::Input)
    }

    pub fn make_var_mapped(&self, mapping: &HashMap<ExecutorVariable, Variable>) -> VarMappedVariableModes {
        let mut var_mapped_modes = HashMap::new();
        for (var, mode) in &self.modes {
            var_mapped_modes.insert(mapping[var], *mode);
        }
        VarMappedVariableModes { modes: var_mapped_modes }
    }
}

impl fmt::Display for VariableModes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (key, group) in &self.modes.iter().sorted_by_key(|(_, value)| *value).group_by(|(_, value)| *value) {
            write!(f, "{key}s=")?;
            for (var, _) in group {
                write!(f, "{}, ", var)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct VarMappedVariableModes {
    modes: HashMap<Variable, VariableMode>,
}

impl fmt::Display for VarMappedVariableModes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (key, group) in &self.modes.iter().sorted_by_key(|(_, value)| *value).group_by(|(_, value)| *value) {
            write!(f, "{key}s=")?;
            for (var, _) in group {
                write!(f, "{}, ", var)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintInstruction<ID> {
    Is(IsInstruction<ID>),

    Iid(thing::IidInstruction<ID>),

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
    IndexedRelation(thing::IndexedRelationInstruction<ID>),
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
            Self::Is(IsInstruction { is, .. }) => is.ids_foreach(apply),
            Self::Iid(thing::IidInstruction { iid, .. }) => iid.ids_foreach(apply),
            &Self::TypeList(type_::TypeListInstruction { type_var, .. }) => apply(type_var),
            Self::Sub(type_::SubInstruction { sub, .. })
            | Self::SubReverse(type_::SubReverseInstruction { sub, .. }) => sub.ids_foreach(apply),
            Self::Owns(type_::OwnsInstruction { owns, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { owns, .. }) => owns.ids_foreach(apply),
            Self::Relates(type_::RelatesInstruction { relates, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { relates, .. }) => relates.ids_foreach(apply),
            Self::Plays(type_::PlaysInstruction { plays, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { plays, .. }) => plays.ids_foreach(apply),
            Self::Isa(thing::IsaInstruction { isa, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, .. }) => isa.ids_foreach(apply),
            Self::Has(thing::HasInstruction { has, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, .. }) => has.ids_foreach(apply),
            Self::Links(thing::LinksInstruction { links, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, .. }) => links.ids_foreach(apply),
            Self::IndexedRelation(thing::IndexedRelationInstruction {
                player_start,
                player_end,
                relation,
                role_end,
                role_start,
                ..
            }) => {
                apply(*player_start);
                apply(*player_end);
                apply(*relation);
                apply(*role_end);
                apply(*role_start);
            }
        }
    }

    pub(crate) fn input_variables_foreach(&self, apply: impl FnMut(ID)) {
        match self {
            Self::Iid(_) => (),
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
            | Self::IndexedRelation(thing::IndexedRelationInstruction { inputs, .. }) => {
                inputs.iter().cloned().for_each(apply)
            }
        }
    }

    pub(crate) fn new_variables_foreach(&self, mut apply: impl FnMut(ID)) {
        match self {
            Self::Is(IsInstruction { is, inputs, .. }) => is.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Iid(thing::IidInstruction { iid, .. }) => iid.ids_foreach(apply),
            &Self::TypeList(type_::TypeListInstruction { type_var, .. }) => apply(type_var),
            Self::Sub(type_::SubInstruction { sub, inputs, .. })
            | Self::SubReverse(type_::SubReverseInstruction { sub, inputs, .. }) => sub.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Owns(type_::OwnsInstruction { owns, inputs, .. })
            | Self::OwnsReverse(type_::OwnsReverseInstruction { owns, inputs, .. }) => owns.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Relates(type_::RelatesInstruction { relates, inputs, .. })
            | Self::RelatesReverse(type_::RelatesReverseInstruction { relates, inputs, .. }) => {
                relates.ids_foreach(|var| {
                    if !inputs.contains(var) {
                        apply(var)
                    }
                })
            }
            Self::Plays(type_::PlaysInstruction { plays, inputs, .. })
            | Self::PlaysReverse(type_::PlaysReverseInstruction { plays, inputs, .. }) => plays.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Isa(thing::IsaInstruction { isa, inputs, .. })
            | Self::IsaReverse(thing::IsaReverseInstruction { isa, inputs, .. }) => isa.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Has(thing::HasInstruction { has, inputs, .. })
            | Self::HasReverse(thing::HasReverseInstruction { has, inputs, .. }) => has.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::Links(thing::LinksInstruction { links, inputs, .. })
            | Self::LinksReverse(thing::LinksReverseInstruction { links, inputs, .. }) => links.ids_foreach(|var| {
                if !inputs.contains(var) {
                    apply(var)
                }
            }),
            Self::IndexedRelation(thing::IndexedRelationInstruction {
                player_start,
                player_end,
                relation,
                role_start,
                role_end,
                inputs,
                ..
            }) => {
                if !inputs.contains(*player_start) {
                    apply(*player_start)
                }
                if !inputs.contains(*player_end) {
                    apply(*player_end)
                }
                if !inputs.contains(*relation) {
                    apply(*relation)
                }
                if !inputs.contains(*role_start) {
                    apply(*role_start)
                }
                if !inputs.contains(*role_end) {
                    apply(*role_end)
                }
            }
        }
    }

    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        match self {
            Self::Is(inner) => inner.add_check(check),
            Self::Iid(inner) => inner.add_check(check),
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
            Self::IndexedRelation(inner) => inner.add_check(check),
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> ConstraintInstruction<T> {
        match self {
            Self::Is(inner) => ConstraintInstruction::Is(inner.map(mapping)),
            Self::Iid(inner) => ConstraintInstruction::Iid(inner.map(mapping)),
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
            Self::IndexedRelation(inner) => ConstraintInstruction::IndexedRelation(inner.map(mapping)),
        }
    }
}

impl<ID: IrID> fmt::Display for ConstraintInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintInstruction::Is(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Iid(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::TypeList(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Sub(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::SubReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Owns(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::OwnsReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Relates(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::RelatesReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Plays(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::PlaysReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Isa(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::IsaReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Has(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::HasReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::Links(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::LinksReverse(instruction) => write!(f, "{instruction}"),
            ConstraintInstruction::IndexedRelation(instruction) => write!(f, "{instruction}"),
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

impl<ID: IrID> fmt::Display for IsInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} filter {}", &self.is, DisplayVec::new(&self.checks))
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
                *type_annotations.vertex_annotations_of(&Vertex::Label(label)).unwrap().iter().exactly_one().unwrap(),
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

impl<ID: IrID> fmt::Display for CheckVertex<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckVertex::Variable(var) => write!(f, "{var}"),
            CheckVertex::Type(type_) => write!(f, "{type_}"),
            CheckVertex::Parameter(param) => write!(f, "{param}"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CheckInstruction<ID> {
    TypeList {
        type_var: ID,
        types: Arc<BTreeSet<Type>>,
    },
    Iid {
        var: ID,
        iid: ParameterID,
    },

    Sub {
        sub_kind: SubKind,
        subtype: CheckVertex<ID>,
        supertype: CheckVertex<ID>,
    },
    Owns {
        owner: CheckVertex<ID>,
        attribute: CheckVertex<ID>,
    },
    Relates {
        relation: CheckVertex<ID>,
        role_type: CheckVertex<ID>,
    },
    Plays {
        player: CheckVertex<ID>,
        role_type: CheckVertex<ID>,
    },

    Isa {
        isa_kind: IsaKind,
        type_: CheckVertex<ID>,
        thing: CheckVertex<ID>,
    },
    Has {
        owner: CheckVertex<ID>,
        attribute: CheckVertex<ID>,
    },
    Links {
        relation: CheckVertex<ID>,
        player: CheckVertex<ID>,
        role: CheckVertex<ID>,
    },
    IndexedRelation {
        start_player: CheckVertex<ID>,
        end_player: CheckVertex<ID>,
        relation: CheckVertex<ID>,
        start_role: CheckVertex<ID>,
        end_role: CheckVertex<ID>,
    },

    Is {
        lhs: ID,
        rhs: ID,
    },
    LinksDeduplication {
        role1: ID,
        player1: ID,
        role2: ID,
        player2: ID,
    },
    Comparison {
        lhs: CheckVertex<ID>,
        rhs: CheckVertex<ID>,
        comparator: Comparator,
    },
    Unsatisfiable,
}

impl<ID: IrID> CheckInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> CheckInstruction<T> {
        match self {
            Self::TypeList { type_var, types } => CheckInstruction::TypeList { type_var: mapping[&type_var], types },
            Self::Iid { var, iid } => CheckInstruction::Iid { var: mapping[&var], iid },
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
            Self::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                CheckInstruction::IndexedRelation {
                    relation: relation.map(mapping),
                    start_player: start_player.map(mapping),
                    end_player: end_player.map(mapping),
                    start_role: start_role.map(mapping),
                    end_role: end_role.map(mapping),
                }
            }
            Self::Is { lhs, rhs } => CheckInstruction::Is { lhs: mapping[&lhs], rhs: mapping[&rhs] },
            Self::LinksDeduplication { role1, player1, role2, player2 } => CheckInstruction::LinksDeduplication {
                role1: mapping[&role1],
                player1: mapping[&player1],
                role2: mapping[&role2],
                player2: mapping[&player2],
            },
            Self::Comparison { lhs, rhs, comparator } => {
                CheckInstruction::Comparison { lhs: lhs.map(mapping), rhs: rhs.map(mapping), comparator }
            }
            Self::Unsatisfiable => CheckInstruction::Unsatisfiable,
        }
    }
}

impl<ID: IrID> fmt::Display for CheckInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Check[")?;
        match self {
            Self::TypeList { type_var, types } => {
                write!(f, "{type_var} type (")?;
                for type_ in types.as_ref() {
                    write!(f, "{type_}, ")?;
                }
                write!(f, ")")?;
            }
            Self::Iid { var, iid } => {
                write!(f, "{var} {} {iid}", typeql::token::Keyword::IID)?;
            }
            Self::Sub { sub_kind, subtype, supertype } => {
                write!(f, "{subtype} {}{} {supertype}", typeql::token::Keyword::Sub, sub_kind)?;
            }
            Self::Owns { owner, attribute } => {
                write!(f, "{owner} {} {attribute}", typeql::token::Keyword::Owns)?;
            }
            Self::Relates { relation, role_type } => {
                write!(f, "{relation} {} {role_type}", typeql::token::Keyword::Relates)?;
            }
            Self::Plays { player, role_type } => {
                write!(f, "{player} {} {role_type}", typeql::token::Keyword::Plays)?;
            }
            Self::Isa { isa_kind, type_, thing } => {
                write!(f, "{thing} {}{} {type_}", typeql::token::Keyword::Isa, isa_kind)?;
            }
            Self::Has { owner, attribute } => {
                write!(f, "{owner} {} {attribute}", typeql::token::Keyword::Has)?;
            }
            Self::Links { relation, player, role } => {
                write!(f, "{relation} {} ({role}:{player})", typeql::token::Keyword::Links)?;
            }
            Self::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                write!(
                    f,
                    "{start_player} indexed_relation(role {start_role}->{relation}->role {end_role}) {end_player}",
                )?;
            }
            Self::Is { lhs, rhs } => {
                write!(f, "{lhs} {} {rhs}", typeql::token::Keyword::Is)?;
            }
            Self::LinksDeduplication { role1, player1, role2, player2 } => {
                write!(f, "({role1},{player1}) __links_deduplication__ ({role2},{player2})")?;
            }
            Self::Comparison { lhs, rhs, comparator } => {
                write!(f, "{lhs} {comparator} {rhs}")?;
            }
            Self::Unsatisfiable => {
                write!(f, "fail")?;
            }
        }
        write!(f, "] ")
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Inputs<ID> {
    None([ID; 0]),
    Single([ID; 1]),
    Dual([ID; 2]),
    Triple([ID; 3]),
    Quadruple([ID; 4]),
    Quintuple([ID; 5]),
}

impl<ID: IrID> Inputs<ID> {
    pub(crate) fn build_from(inputs: &[ID]) -> Self {
        match inputs.len() {
            0 => Self::None([]),
            1 => Self::Single([inputs[0]]),
            2 => Self::Dual([inputs[0], inputs[1]]),
            3 => Self::Triple([inputs[0], inputs[1], inputs[2]]),
            4 => Self::Quadruple([inputs[0], inputs[1], inputs[2], inputs[3]]),
            5 => Self::Quintuple([inputs[0], inputs[1], inputs[2], inputs[3], inputs[4]]),
            _ => panic!("Inputs longer than 5 provided."),
        }
    }

    pub(crate) fn contains(&self, id: ID) -> bool {
        self.deref().contains(&id)
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Inputs<T> {
        match self {
            Inputs::None(_) => Inputs::None([]),
            Inputs::Single([var]) => Inputs::Single([mapping[&var]]),
            Inputs::Dual([var_1, var_2]) => Inputs::Dual([mapping[&var_1], mapping[&var_2]]),
            Inputs::Triple([var_1, var_2, var_3]) => {
                Inputs::Triple([mapping[&var_1], mapping[&var_2], mapping[&var_3]])
            }
            Inputs::Quadruple([var_1, var_2, var_3, var_4]) => {
                Inputs::Quadruple([mapping[&var_1], mapping[&var_2], mapping[&var_3], mapping[&var_4]])
            }
            Inputs::Quintuple([var_1, var_2, var_3, var_4, var_5]) => {
                Inputs::Quintuple([mapping[&var_1], mapping[&var_2], mapping[&var_3], mapping[&var_4], mapping[&var_5]])
            }
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
            Inputs::Triple(ids) => ids,
            Inputs::Quadruple(ids) => ids,
            Inputs::Quintuple(ids) => ids,
        }
    }
}

struct DisplayVec<'a, T: fmt::Display> {
    vec: &'a Vec<T>,
}

impl<'a, T: fmt::Display> DisplayVec<'a, T> {
    fn new(vec: &'a Vec<T>) -> Self {
        Self { vec }
    }
}

impl<T: fmt::Display> fmt::Display for DisplayVec<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, element) in self.vec.iter().enumerate() {
            if i != self.vec.len() - 1 {
                write!(f, "{}, ", element)?;
            } else {
                write!(f, "{}", element)?;
            }
        }
        write!(f, "]")
    }
}

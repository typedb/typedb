/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::pattern::{
    constraint::{As, Owns, Plays, Relates, Sub},
    IrID, Vertex,
};

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::instructions::{CheckInstruction, Inputs},
};

#[derive(Debug, Clone)]
pub struct TypeListInstruction<ID> {
    pub type_var: ID,
    types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl TypeListInstruction<Variable> {
    pub(crate) fn new(type_var: Variable, type_annotations: &TypeAnnotations) -> Self {
        let types = type_annotations.vertex_annotations_of(&Vertex::Variable(type_var)).unwrap().clone();
        Self { type_var, types, checks: Vec::new() }
    }
}

impl<ID> TypeListInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn types(&self) -> &BTreeSet<Type> {
        &self.types
    }
}

impl<ID: IrID> TypeListInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> TypeListInstruction<T> {
        let Self { type_var, types, checks } = self;
        TypeListInstruction {
            type_var: mapping[&type_var],
            types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubInstruction<ID> {
    pub sub: Sub<ID>,
    pub inputs: Inputs<ID>,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl SubInstruction<Variable> {
    pub fn new(sub: Sub<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let supertypes = type_annotations.vertex_annotations_of(sub.supertype()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(sub.clone().into()).unwrap().as_left_right();
        let sub_to_supertypes = edge_annotations.left_to_right();
        Self { sub, inputs, sub_to_supertypes, supertypes, checks: Vec::new() }
    }
}

impl<ID> SubInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn sub_to_supertypes(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.sub_to_supertypes
    }

    pub fn supertypes(&self) -> &Arc<BTreeSet<Type>> {
        &self.supertypes
    }
}

impl<ID: IrID> SubInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> SubInstruction<T> {
        let Self { sub, inputs, sub_to_supertypes, supertypes, checks } = self;
        SubInstruction {
            sub: sub.map(mapping),
            inputs: inputs.map(mapping),
            sub_to_supertypes,
            supertypes,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubReverseInstruction<ID> {
    pub sub: Sub<ID>,
    pub inputs: Inputs<ID>,
    super_to_subtypes: Arc<BTreeMap<Type, Vec<Type>>>,
    subtypes: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl SubReverseInstruction<Variable> {
    pub fn new(sub: Sub<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let subtypes = type_annotations.vertex_annotations_of(sub.subtype()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(sub.clone().into()).unwrap().as_left_right();
        let super_to_subtypes = edge_annotations.right_to_left();
        Self { sub, inputs, super_to_subtypes, subtypes, checks: Vec::new() }
    }
}

impl<ID> SubReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn super_to_subtypes(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.super_to_subtypes
    }

    pub fn subtypes(&self) -> &Arc<BTreeSet<Type>> {
        &self.subtypes
    }
}

impl<ID: IrID> SubReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> SubReverseInstruction<T> {
        let Self { sub, inputs, super_to_subtypes, subtypes, checks } = self;
        SubReverseInstruction {
            sub: sub.map(mapping),
            inputs: inputs.map(mapping),
            super_to_subtypes,
            subtypes,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnsInstruction<ID> {
    pub owns: Owns<ID>,
    pub inputs: Inputs<ID>,
    owner_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl OwnsInstruction<Variable> {
    pub fn new(owns: Owns<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let attribute_types = type_annotations.vertex_annotations_of(owns.attribute()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(owns.clone().into()).unwrap().as_left_right();
        let owner_attribute_types = edge_annotations.left_to_right();
        Self { owns, inputs, owner_attribute_types, attribute_types, checks: Vec::new() }
    }
}

impl<ID> OwnsInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn owner_attribute_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.owner_attribute_types
    }

    pub fn attribute_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.attribute_types
    }
}

impl<ID: IrID> OwnsInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> OwnsInstruction<T> {
        let Self { owns, inputs, owner_attribute_types, attribute_types, checks } = self;
        OwnsInstruction {
            owns: owns.map(mapping),
            inputs: inputs.map(mapping),
            owner_attribute_types,
            attribute_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnsReverseInstruction<ID> {
    pub owns: Owns<ID>,
    pub inputs: Inputs<ID>,
    attribute_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl OwnsReverseInstruction<Variable> {
    pub fn new(owns: Owns<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let owner_types = type_annotations.vertex_annotations_of(owns.owner()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(owns.clone().into()).unwrap().as_left_right();
        let attribute_owner_types = edge_annotations.right_to_left();
        Self { owns, inputs, attribute_owner_types, owner_types, checks: Vec::new() }
    }
}

impl<ID> OwnsReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn attribute_owner_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.attribute_owner_types
    }

    pub fn owner_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.owner_types
    }
}

impl<ID: IrID> OwnsReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> OwnsReverseInstruction<T> {
        let Self { owns, inputs, attribute_owner_types, owner_types, checks } = self;
        OwnsReverseInstruction {
            owns: owns.map(mapping),
            inputs: inputs.map(mapping),
            attribute_owner_types,
            owner_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelatesInstruction<ID> {
    pub relates: Relates<ID>,
    pub inputs: Inputs<ID>,
    relation_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl RelatesInstruction<Variable> {
    pub fn new(relates: Relates<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let role_types = type_annotations.vertex_annotations_of(relates.role_type()).unwrap().clone();
        let edge_annotations =
            type_annotations.constraint_annotations_of(relates.clone().into()).unwrap().as_left_right();
        let relation_role_types = edge_annotations.left_to_right();
        Self { relates, inputs, relation_role_types, role_types, checks: Vec::new() }
    }
}

impl<ID> RelatesInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn relation_role_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.relation_role_types
    }

    pub fn role_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.role_types
    }
}

impl<ID: IrID> RelatesInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> RelatesInstruction<T> {
        let Self { relates, inputs, relation_role_types, role_types, checks } = self;
        RelatesInstruction {
            relates: relates.map(mapping),
            inputs: inputs.map(mapping),
            relation_role_types,
            role_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelatesReverseInstruction<ID> {
    pub relates: Relates<ID>,
    pub inputs: Inputs<ID>,
    role_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl RelatesReverseInstruction<Variable> {
    pub fn new(relates: Relates<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let relation_types = type_annotations.vertex_annotations_of(relates.relation()).unwrap().clone();
        let edge_annotations =
            type_annotations.constraint_annotations_of(relates.clone().into()).unwrap().as_left_right();
        let role_type_relation_types = edge_annotations.right_to_left();
        Self { relates, inputs, role_relation_types: role_type_relation_types, relation_types, checks: Vec::new() }
    }
}

impl<ID> RelatesReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn role_relation_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.role_relation_types
    }

    pub fn relation_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.relation_types
    }
}

impl<ID: IrID> RelatesReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> RelatesReverseInstruction<T> {
        let Self { relates, inputs, role_relation_types: role_type_relation_types, relation_types, checks } = self;
        RelatesReverseInstruction {
            relates: relates.map(mapping),
            inputs: inputs.map(mapping),
            role_relation_types: role_type_relation_types,
            relation_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlaysInstruction<ID> {
    pub plays: Plays<ID>,
    pub inputs: Inputs<ID>,
    player_role_types: Arc<BTreeMap<Type, Vec<Type>>>,
    role_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl PlaysInstruction<Variable> {
    pub fn new(plays: Plays<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let role_types = type_annotations.vertex_annotations_of(plays.role_type()).unwrap().clone();
        let edge_annotations =
            type_annotations.constraint_annotations_of(plays.clone().into()).unwrap().as_left_right();
        let player_role_types = edge_annotations.left_to_right();
        Self { plays, inputs, player_role_types, role_types, checks: Vec::new() }
    }
}

impl<ID> PlaysInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn player_role_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.player_role_types
    }

    pub fn role_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.role_types
    }
}

impl<ID: IrID> PlaysInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> PlaysInstruction<T> {
        let Self { plays, inputs, player_role_types, role_types, checks } = self;
        PlaysInstruction {
            plays: plays.map(mapping),
            inputs: inputs.map(mapping),
            player_role_types,
            role_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlaysReverseInstruction<ID> {
    pub plays: Plays<ID>,
    pub inputs: Inputs<ID>,
    role_player_types: Arc<BTreeMap<Type, Vec<Type>>>,
    player_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl PlaysReverseInstruction<Variable> {
    pub fn new(plays: Plays<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let player_types = type_annotations.vertex_annotations_of(plays.player()).unwrap().clone();
        let edge_annotations =
            type_annotations.constraint_annotations_of(plays.clone().into()).unwrap().as_left_right();
        let role_type_player_types = edge_annotations.right_to_left();
        Self { plays, inputs, role_player_types: role_type_player_types, player_types, checks: Vec::new() }
    }
}

impl<ID> PlaysReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn role_player_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.role_player_types
    }

    pub fn player_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.player_types
    }
}

impl<ID: IrID> PlaysReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> PlaysReverseInstruction<T> {
        let Self { plays, inputs, role_player_types: role_type_player_types, player_types, checks } = self;
        PlaysReverseInstruction {
            plays: plays.map(mapping),
            inputs: inputs.map(mapping),
            role_player_types: role_type_player_types,
            player_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AsInstruction<ID> {
    pub as_: As<ID>,
    pub inputs: Inputs<ID>,
    specialising_to_specialised: Arc<BTreeMap<Type, Vec<Type>>>,
    specialised: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl AsInstruction<Variable> {
    pub fn new(as_: As<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let specialised = type_annotations.vertex_annotations_of(as_.specialised()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(as_.clone().into()).unwrap().as_left_right();
        let specialising_to_specialised = edge_annotations.left_to_right();
        Self { as_, inputs, specialising_to_specialised, specialised, checks: Vec::new() }
    }
}

impl<ID> AsInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn specialising_to_specialised(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.specialising_to_specialised
    }

    pub fn specialised(&self) -> &Arc<BTreeSet<Type>> {
        &self.specialised
    }
}

impl<ID: IrID> AsInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> AsInstruction<T> {
        let Self { as_, inputs, specialising_to_specialised, specialised, checks } = self;
        AsInstruction {
            as_: as_.map(mapping),
            inputs: inputs.map(mapping),
            specialising_to_specialised,
            specialised,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AsReverseInstruction<ID> {
    pub as_: As<ID>,
    pub inputs: Inputs<ID>,
    specialised_to_specialising: Arc<BTreeMap<Type, Vec<Type>>>,
    specialising: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl AsReverseInstruction<Variable> {
    pub fn new(as_: As<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let specialising = type_annotations.vertex_annotations_of(as_.specialising()).unwrap().clone();
        let edge_annotations = type_annotations.constraint_annotations_of(as_.clone().into()).unwrap().as_left_right();
        let specialised_to_specialising = edge_annotations.right_to_left();
        Self { as_, inputs, specialised_to_specialising, specialising, checks: Vec::new() }
    }
}

impl<ID> AsReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn specialised_to_specialising(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.specialised_to_specialising
    }

    pub fn specialising(&self) -> &Arc<BTreeSet<Type>> {
        &self.specialising
    }
}

impl<ID: IrID> AsReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> AsReverseInstruction<T> {
        let Self { as_, inputs, specialised_to_specialising, specialising, checks } = self;
        AsReverseInstruction {
            as_: as_.map(mapping),
            inputs: inputs.map(mapping),
            specialised_to_specialising,
            specialising,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use ir::pattern::{
    constraint::{Owns, Sub},
    IrID,
};

use crate::match_::{
    inference::type_annotations::TypeAnnotations,
    instructions::{CheckInstruction, Inputs},
};

#[derive(Debug, Clone)]
pub struct LabelInstruction<ID> {
    pub type_var: ID,
    types: Arc<HashSet<Type>>,
}

impl LabelInstruction<Variable> {
    pub(crate) fn new(type_var: Variable, type_annotations: &TypeAnnotations) -> Self {
        let types = type_annotations.variable_annotations_of(type_var).unwrap().clone();
        Self { type_var, types }
    }
}

impl<ID> LabelInstruction<ID> {
    pub fn types(&self) -> &HashSet<Type> {
        &self.types
    }
}

impl<ID: IrID> LabelInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> LabelInstruction<T> {
        let Self { type_var, types } = self;
        LabelInstruction { type_var: mapping[&type_var], types }
    }
}

#[derive(Debug, Clone)]
pub struct SubInstruction<ID> {
    pub sub: Sub<ID>,
    pub inputs: Inputs<ID>,
    sub_to_supertypes: Arc<BTreeMap<Type, Vec<Type>>>,
    supertypes: Arc<HashSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl SubInstruction<Variable> {
    pub fn new(sub: Sub<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let supertypes = type_annotations.variable_annotations_of(sub.supertype()).unwrap().clone();
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

    pub fn supertypes(&self) -> &Arc<HashSet<Type>> {
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
    subtypes: Arc<HashSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl SubReverseInstruction<Variable> {
    pub fn new(sub: Sub<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let subtypes = type_annotations.variable_annotations_of(sub.subtype()).unwrap().clone();
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

    pub fn subtypes(&self) -> &Arc<HashSet<Type>> {
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
    attribute_types: Arc<HashSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl OwnsInstruction<Variable> {
    pub fn new(owns: Owns<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let attribute_types = type_annotations.variable_annotations_of(owns.attribute()).unwrap().clone();
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

    pub fn attribute_types(&self) -> &Arc<HashSet<Type>> {
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
    owner_types: Arc<HashSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl OwnsReverseInstruction<Variable> {
    pub fn new(owns: Owns<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let owner_types = type_annotations.variable_annotations_of(owns.owner()).unwrap().clone();
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

    pub fn owner_types(&self) -> &Arc<HashSet<Type>> {
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

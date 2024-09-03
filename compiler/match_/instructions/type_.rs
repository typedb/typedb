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
use ir::pattern::{constraint::Sub, IrID};

use crate::match_::{
    inference::type_annotations::TypeAnnotations,
    instructions::{CheckInstruction, Inputs},
};

#[derive(Debug, Clone)]
pub struct TypeInstruction<ID> {
    pub type_var: ID,
    types: Arc<HashSet<Type>>,
}

impl TypeInstruction<Variable> {
    pub(crate) fn new(type_var: Variable, type_annotations: &TypeAnnotations) -> Self {
        let types = type_annotations.variable_annotations_of(type_var).unwrap().clone();
        Self { type_var, types }
    }
}

impl<ID> TypeInstruction<ID> {
    pub fn types(&self) -> &HashSet<Type> {
        &self.types
    }
}

impl<ID: IrID> TypeInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> TypeInstruction<T> {
        let Self { type_var, types } = self;
        TypeInstruction { type_var: mapping[&type_var], types }
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

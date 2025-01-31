/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hasher},
    mem,
};

use answer::variable::Variable;
use encoding::value::label::Label;
use structural_equality::{ordered_hash_combine, StructuralEquality};
use typeql::common::Span;

use crate::{
    pattern::ParameterID,
    pipeline::function::Function,
    translation::{pipeline::TranslatedStage, TranslationContext},
};

#[derive(Debug, Clone)]
pub enum FetchSome {
    SingleVar(Variable),
    SingleAttribute(FetchSingleAttribute),
    SingleFunction(Function),

    // note: all source_spans are contained in FetchObject
    Object(Box<FetchObject>),

    ListFunction(Function),
    ListSubFetch(FetchListSubFetch),
    ListAttributesAsList(FetchListAttributeAsList),
    ListAttributesFromList(FetchListAttributeFromList),
}

impl FetchSome {
    pub(crate) fn record_variables_recursive(&self, vars: &mut HashSet<Variable>) {
        match self {
            Self::SingleVar(variable) => {
                vars.insert(*variable);
            }
            Self::SingleAttribute(FetchSingleAttribute { variable, .. }) => {
                vars.insert(*variable);
            }
            Self::SingleFunction(function) | Self::ListFunction(function) => {
                vars.extend(function.arguments.iter().cloned());
            }
            Self::Object(object) => {
                object.record_variables_recursive(vars);
            }
            Self::ListSubFetch(FetchListSubFetch { stages, fetch, .. }) => {
                stages.iter().for_each(|stage| vars.extend(stage.variables()));
                fetch.record_variables_recursive(vars);
            }
            Self::ListAttributesAsList(FetchListAttributeAsList { variable, .. }) => {
                vars.insert(*variable);
            }
            Self::ListAttributesFromList(FetchListAttributeFromList { variable, .. }) => {
                vars.insert(*variable);
            }
        };
    }
}

impl StructuralEquality for FetchSome {
    fn hash(&self) -> u64 {
        ordered_hash_combine(
            mem::discriminant(self).hash(),
            match self {
                FetchSome::SingleVar(var) => var.hash(),
                FetchSome::SingleAttribute(fetch) => fetch.hash(),
                FetchSome::SingleFunction(function) => function.hash(),
                FetchSome::Object(object) => object.hash(),
                FetchSome::ListFunction(function) => function.hash(),
                FetchSome::ListSubFetch(fetch) => fetch.hash(),
                FetchSome::ListAttributesAsList(fetch) => fetch.hash(),
                FetchSome::ListAttributesFromList(fetch) => fetch.hash(),
            },
        )
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SingleVar(inner), Self::SingleVar(other_inner)) => inner.equals(other_inner),
            (Self::SingleAttribute(inner), Self::SingleAttribute(other_inner)) => inner.equals(other_inner),
            (Self::SingleFunction(inner), Self::SingleFunction(other_inner)) => inner.equals(other_inner),
            (Self::Object(inner), Self::Object(other_inner)) => inner.equals(other_inner),
            (Self::ListFunction(inner), Self::ListFunction(other_inner)) => inner.equals(other_inner),
            (Self::ListSubFetch(inner), Self::ListSubFetch(other_inner)) => inner.equals(other_inner),
            (Self::ListAttributesAsList(inner), Self::ListAttributesAsList(other_inner)) => inner.equals(other_inner),
            (Self::ListAttributesFromList(inner), Self::ListAttributesFromList(other_inner)) => {
                inner.equals(other_inner)
            }

            (Self::SingleVar(_), _)
            | (Self::SingleAttribute(_), _)
            | (Self::SingleFunction(_), _)
            | (Self::Object(_), _)
            | (Self::ListFunction(_), _)
            | (Self::ListSubFetch(_), _)
            | (Self::ListAttributesAsList(_), _)
            | (Self::ListAttributesFromList(_), _) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchSingleAttribute {
    pub variable: Variable,
    pub attribute: Label,
}

impl StructuralEquality for FetchSingleAttribute {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.variable.hash_into(&mut hasher);
        self.attribute.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variable.equals(&other.variable) && self.attribute.equals(&other.attribute)
    }
}

#[derive(Debug, Clone)]
pub enum FetchObject {
    Entries(HashMap<ParameterID, FetchSome>, HashMap<ParameterID, Option<Span>>),
    Attributes(Variable, Option<Span>),
}

impl FetchObject {
    pub(crate) fn record_variables_recursive(&self, vars: &mut HashSet<Variable>) {
        match self {
            FetchObject::Entries(entries, _) => {
                entries.iter().for_each(|(_, some)| some.record_variables_recursive(vars));
            }
            FetchObject::Attributes(var, _) => {
                vars.insert(*var);
            }
        }
    }
}

impl StructuralEquality for FetchObject {
    fn hash(&self) -> u64 {
        ordered_hash_combine(
            mem::discriminant(self).hash(),
            match self {
                FetchObject::Entries(entries, _) => StructuralEquality::hash(entries),
                FetchObject::Attributes(variable, _) => variable.hash(),
            },
        )
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Entries(entries, _), Self::Entries(other_entries, _)) => entries.equals(other_entries),
            (Self::Attributes(var, _), Self::Attributes(other_var, _)) => var.equals(other_var),
            (Self::Entries(_, _), _) | (Self::Attributes(_, _), _) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchListSubFetch {
    pub context: TranslationContext,
    pub input_variables: HashSet<Variable>,
    pub stages: Vec<TranslatedStage>,
    pub fetch: FetchObject,
}

impl StructuralEquality for FetchListSubFetch {
    fn hash(&self) -> u64 {
        ordered_hash_combine(self.stages.hash(), self.fetch.hash())
    }

    fn equals(&self, other: &Self) -> bool {
        self.stages.equals(&other.stages) && self.fetch.equals(&other.fetch)
    }
}

#[derive(Debug, Clone)]
pub struct FetchListAttributeAsList {
    pub variable: Variable,
    pub attribute: Label,
}

impl StructuralEquality for FetchListAttributeAsList {
    fn hash(&self) -> u64 {
        ordered_hash_combine(self.variable.hash(), self.attribute.hash())
    }

    fn equals(&self, other: &Self) -> bool {
        self.variable.equals(&other.variable) && self.attribute.equals(&other.attribute)
    }
}

#[derive(Debug, Clone)]
pub struct FetchListAttributeFromList {
    pub variable: Variable,
    pub attribute: Label,
}

impl StructuralEquality for FetchListAttributeFromList {
    fn hash(&self) -> u64 {
        ordered_hash_combine(self.variable.hash(), self.attribute.hash())
    }

    fn equals(&self, other: &Self) -> bool {
        self.variable.equals(&other.variable) && self.attribute.equals(&other.attribute)
    }
}

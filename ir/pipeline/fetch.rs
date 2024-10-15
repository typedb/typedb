/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use answer::variable::Variable;

use crate::{
    pattern::ParameterID
    ,
    translation::{pipeline::TranslatedStage, TranslationContext},
};
use crate::pipeline::function::Function;

#[derive(Debug, Clone)]
pub enum FetchSome {
    SingleVar(Variable),
    SingleAttribute(FetchSingleAttribute),
    SingleFunction(Function),

    Object(Box<FetchObject>),

    ListFunction(Function),
    ListSubFetch(FetchListSubFetch),
    ListAttributesAsList(FetchListAttributeAsList),
    ListAttributesFromList(FetchListAttributeFromList),
}

impl FetchSome {
    pub(crate) fn record_variables_recursive(&self, vars: &mut HashSet<Variable>) {
        match self {
            Self::SingleVar(variable) => { vars.insert(*variable); }
            Self::SingleAttribute(FetchSingleAttribute { variable, ..}) => { vars.insert(*variable); }
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
            Self::ListAttributesAsList(FetchListAttributeAsList { variable, .. }) => { vars.insert(*variable); }
            Self::ListAttributesFromList(FetchListAttributeFromList { variable, .. }) => { vars.insert(*variable); }
        };
    }
}

#[derive(Debug, Clone)]
pub struct FetchSingleAttribute {
    pub variable: Variable,
    pub attribute: String,
}

#[derive(Debug, Clone)]
pub enum FetchObject {
    Entries(HashMap<ParameterID, FetchSome>),
    Attributes(Variable),
}

impl FetchObject {
   pub(crate) fn record_variables_recursive(&self, vars: &mut HashSet<Variable>) {
       match self {
           FetchObject::Entries(entries) => {
               entries.iter().for_each(|(_, some)| some.record_variables_recursive(vars));
           }
           FetchObject::Attributes(var) => {
               vars.insert(*var);
           },
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

#[derive(Debug, Clone)]
pub struct FetchListAttributeAsList {
    pub variable: Variable,
    pub attribute: String,
}

#[derive(Debug, Clone)]
pub struct FetchListAttributeFromList {
    pub variable: Variable,
    pub attribute: String,
}

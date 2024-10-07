/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::pattern::ParameterID;
use crate::program::function::FunctionBody;
use crate::translation::pipeline::TranslatedStage;

#[derive(Debug, Clone)]
pub enum FetchSome {
    SingleVar(FetchSingleVar),
    SingleAttribute(FetchSingleAttribute),
    SingleInlineFunction(FunctionBody),

    Object(Box<FetchObject>),

    ListFunction(FunctionBody),
    ListSubFetch(FetchListSubFetch),
    ListAttributesAsList(FetchListAttributeAsList),
    ListAttributesFromList(FetchListAttributeFromList),
}

#[derive(Debug, Clone)]
pub struct FetchSingleVar {
    pub(crate) variable: Variable,
}

#[derive(Debug, Clone)]
pub struct FetchSingleAttribute {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

#[derive(Debug, Clone)]
pub enum FetchObject {
    Entries(FetchObjectEntries),
    Attributes(FetchObjectAttributes),
}

#[derive(Debug, Clone)]
pub struct FetchObjectEntries {
    pub(crate) entries: HashMap<ParameterID, FetchSome>,
}

#[derive(Debug, Clone)]
pub struct FetchObjectAttributes {
    pub(crate) variable: Variable,
}

#[derive(Debug, Clone)]
pub struct FetchListSubFetch {
    pub(crate) stages: Vec<TranslatedStage>,
}

#[derive(Debug, Clone)]
pub struct FetchListAttributeAsList {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

#[derive(Debug, Clone)]
pub struct FetchListAttributeFromList {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

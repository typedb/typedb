/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::{
    pattern::ParameterID
    ,
    translation::{pipeline::TranslatedStage, TranslationContext},
};
use crate::pipeline::function::Function;

#[derive(Debug, Clone)]
pub enum FetchSome {
    SingleVar(FetchSingleVar),
    SingleAttribute(FetchSingleAttribute),
    SingleFunction(Function),

    Object(Box<FetchObject>),

    ListFunction(Function),
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
    pub variable: Variable,
    pub attribute: String,
}

#[derive(Debug, Clone)]
pub enum FetchObject {
    Entries(FetchObjectEntries),
    Attributes(FetchObjectAttributes),
}

#[derive(Debug, Clone)]
pub struct FetchObjectEntries {
    pub entries: HashMap<ParameterID, FetchSome>,
}

#[derive(Debug, Clone)]
pub struct FetchObjectAttributes {
    pub(crate) variable: Variable,
}

#[derive(Debug, Clone)]
pub struct FetchListSubFetch {
    pub context: TranslationContext,
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

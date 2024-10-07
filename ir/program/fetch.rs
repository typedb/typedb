/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::pattern::ParameterID;
use crate::program::function::FunctionBody;
use crate::program::function_signature::FunctionID;
use crate::translation::pipeline::TranslatedStage;

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

pub struct FetchSingleVar {
    pub(crate) variable: Variable,
}

pub struct FetchSingleAttribute {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

pub enum FetchObject {
    Static(FetchObjectStatic),
    Attributes(FetchObjectAttributes),
}

pub struct FetchObjectStatic {
    pub(crate) object: HashMap<ParameterID, FetchSome>,
}

pub struct FetchObjectAttributes {
    pub(crate) variable: Variable,
}

pub struct FetchListSubFetch {
    pub(crate) stages: Vec<TranslatedStage>,
}

pub struct FetchListAttributeAsList {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

pub struct FetchListAttributeFromList {
    pub(crate) variable: Variable,
    pub(crate) attribute: String,
}

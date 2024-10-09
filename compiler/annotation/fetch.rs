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
use encoding::value::value_type::ValueType;
use ir::{
    pattern::{ParameterID, Vertex},
    program::fetch::{
        FetchListAttributeAsList, FetchListAttributeFromList, FetchObjectAttributes, FetchSingleAttribute,
        FetchSingleVar,
    },
};

use crate::annotation::{function::AnnotatedAnonymousFunction, pipeline::AnnotatedStage};

#[derive(Debug, Clone)]
pub struct AnnotatedFetch {
    input_type_annotations: BTreeMap<Vertex<Variable>, Arc<BTreeSet<Type>>>,
    input_value_annotations: BTreeMap<Variable, ValueType>,

    object: AnnotatedFetchObject,
}

#[derive(Debug, Clone)]
pub enum AnnotatedFetchSome {
    SingleVar(FetchSingleVar),
    SingleAttribute(FetchSingleAttribute),
    SingleFunction(AnnotatedAnonymousFunction),

    Object(Box<AnnotatedFetchObject>),

    ListFunction(AnnotatedAnonymousFunction),
    ListSubFetch(AnnotatedFetchListSubFetch),
    ListAttributesAsList(FetchListAttributeAsList),
    ListAttributesFromList(FetchListAttributeFromList),
}

#[derive(Debug, Clone)]
pub enum AnnotatedFetchObject {
    Entries(AnnotatedFetchObjectEntries),
    Attributes(FetchObjectAttributes),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFetchObjectEntries {
    pub(crate) entries: HashMap<ParameterID, AnnotatedFetchSome>,
}

#[derive(Debug, Clone)]
pub struct AnnotatedFetchListSubFetch {
    stages: Vec<AnnotatedStage>,
}

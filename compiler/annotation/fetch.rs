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
use concept::type_::type_manager::TypeManager;
use encoding::value::value_type::ValueType;
use ir::{
    pattern::ParameterID,
    pipeline::fetch::{
        FetchListAttributeAsList, FetchListAttributeFromList, FetchListSubFetch, FetchObject, FetchObjectAttributes,
        FetchObjectEntries, FetchSingleAttribute, FetchSingleVar, FetchSome,
    },
};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    function::{
        annotate_anonymous_function, AnnotatedAnonymousFunction, AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions,
    },
    pipeline::AnnotatedStage,
    AnnotationError,
};

#[derive(Debug, Clone)]
pub struct AnnotatedFetch {
    input_type_annotations: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
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

pub(crate) fn annotate_fetch(
    fetch: FetchObject,
    input_type_annotations: BTreeMap<Variable, Arc<BTreeSet<Type>>>,
    input_value_annotations: BTreeMap<Variable, ValueType>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFetch, AnnotationError> {
    let object = annotate_object(fetch, snapshot, type_manager, indexed_annotated_functions, local_functions)?;
    Ok(AnnotatedFetch { input_type_annotations, input_value_annotations, object })
}

fn annotate_object(
    object: FetchObject,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFetchObject, AnnotationError> {
    match object {
        FetchObject::Entries(entries) => {
            let annotated_entries = annotated_object_entries(
                entries,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                local_functions,
            )?;
            Ok(AnnotatedFetchObject::Entries(annotated_entries))
        }
        FetchObject::Attributes(attributes) => Ok(AnnotatedFetchObject::Attributes(attributes)),
    }
}

fn annotated_object_entries(
    entries: FetchObjectEntries,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFetchObjectEntries, AnnotationError> {
    let mut annotated_entries = HashMap::new();
    for (key, value) in entries.entries.into_iter() {
        let annotated_value =
            annotate_some(value, snapshot, type_manager, indexed_annotated_functions, local_functions)?;
        annotated_entries.insert(key, annotated_value);
    }
    Ok(AnnotatedFetchObjectEntries { entries: annotated_entries })
}

fn annotate_some(
    some: FetchSome,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFetchSome, AnnotationError> {
    match some {
        FetchSome::SingleVar(var) => Ok(AnnotatedFetchSome::SingleVar(var)),
        FetchSome::SingleAttribute(attr) => Ok(AnnotatedFetchSome::SingleAttribute(attr)),
        FetchSome::SingleFunction(function) => {
            let annotated_function = annotate_anonymous_function(
                &function,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                local_functions,
            )
            .map_err(|err| AnnotationError::FetchBlockFunctionInferenceError { typedb_source: err })?;
            Ok(AnnotatedFetchSome::SingleFunction(annotated_function))
        }
        FetchSome::Object(object) => {
            let object =
                annotate_object(*object, snapshot, type_manager, indexed_annotated_functions, local_functions)?;
            Ok(AnnotatedFetchSome::Object(Box::new(object)))
        }
        FetchSome::ListFunction(function) => {
            let annotated_function = annotate_anonymous_function(
                &function,
                snapshot,
                type_manager,
                indexed_annotated_functions,
                local_functions,
            )
            .map_err(|err| AnnotationError::FetchBlockFunctionInferenceError { typedb_source: err })?;
            Ok(AnnotatedFetchSome::ListFunction(annotated_function))
        }
        FetchSome::ListSubFetch(sub_fetch) => Ok(AnnotatedFetchSome::ListSubFetch(annotate_sub_fetch(sub_fetch))),
        FetchSome::ListAttributesAsList(fetch) => Ok(AnnotatedFetchSome::ListAttributesAsList(fetch)),
        FetchSome::ListAttributesFromList(fetch) => Ok(AnnotatedFetchSome::ListAttributesFromList(fetch)),
    }
}

fn annotate_sub_fetch(sub_fetch: FetchListSubFetch) -> AnnotatedFetchListSubFetch {
    todo!()
}

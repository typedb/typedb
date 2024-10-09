/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeSet;

use answer::Type;
use concept::type_::type_manager::TypeManager;
use encoding::{graph::definition::definition_key::DefinitionKey, value::value_type::ValueType};
use ir::program::{function::Function, function_signature::FunctionIDAPI};
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{pipeline::AnnotatedStage, FunctionTypeInferenceError};

#[derive(Debug, Clone)]
pub enum FunctionParameterAnnotation {
    Concept(BTreeSet<Type>),
    Value(ValueType),
}

#[derive(Debug, Clone)]
pub struct AnnotatedFunction {
    stages: Vec<AnnotatedStage>,
    return_annotations: Vec<BTreeSet<FunctionParameterAnnotation>>,
}

#[derive(Debug, Clone)]
pub struct AnnotatedAnonymousFunction {
    stages: Vec<AnnotatedStage>,
    return_annotations: Vec<BTreeSet<FunctionParameterAnnotation>>,
}

/// Indexed by Function ID
#[derive(Debug)]
pub struct IndexedAnnotatedFunctions {
    functions: Vec<AnnotatedFunction>,
}

impl IndexedAnnotatedFunctions {
    pub fn new(functions: Vec<AnnotatedFunction>) -> Self {
        Self { functions }
    }

    pub fn empty() -> Self {
        Self { functions: Vec::new() }
    }
}

pub trait AnnotatedFunctions {
    type ID;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction>;

    fn is_empty(&self) -> bool;
}

impl AnnotatedFunctions for IndexedAnnotatedFunctions {
    type ID = DefinitionKey<'static>;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.functions.get(id.as_usize())
    }

    fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

// May hold IR & Annotations for either uncommitted Schema functions or Preamble functions
// For schema functions, The index does not correspond to function_id.as_usize().
pub struct AnnotatedUnindexedFunctions {
    unindexed_functions: Vec<AnnotatedFunction>,
}

impl AnnotatedUnindexedFunctions {
    pub fn new(functions: Vec<AnnotatedFunction>) -> Self {
        Self { unindexed_functions: functions }
    }

    pub fn empty() -> Self {
        Self { unindexed_functions: Vec::new() }
    }

    pub fn iter_functions(&self) -> impl Iterator<Item = &AnnotatedFunction> {
        self.unindexed_functions.iter()
    }
}

impl AnnotatedFunctions for AnnotatedUnindexedFunctions {
    type ID = usize;

    fn get_function(&self, id: Self::ID) -> Option<&AnnotatedFunction> {
        self.unindexed_functions.get(id)
    }

    fn is_empty(&self) -> bool {
        self.unindexed_functions.is_empty()
    }
}

pub fn annotate_functions(
    functions: Vec<Function>,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
) -> Result<AnnotatedUnindexedFunctions, FunctionTypeInferenceError> {
    // // In the preliminary annotations, functions are annotated based only on the variable categories of the called function.
    // let preliminary_annotations_res: Result<Vec<FunctionAnnotations>, FunctionTypeInferenceError> = functions
    //     .iter()
    //     .map(|function| infer_types_for_function(function, snapshot, type_manager, indexed_annotated_functions, None))
    //     .collect();
    // let preliminary_annotations =
    //     AnnotatedUnindexedFunctions::new(functions.into_boxed_slice(), preliminary_annotations_res?.into_boxed_slice());
    //
    // // In the second round, finer annotations are available at the function calls so the annotations in function bodies can be refined.
    // let annotations_res = preliminary_annotations
    //     .iter_functions()
    //     .map(|function| {
    //         annotate_function(
    //             function,
    //             snapshot,
    //             type_manager,
    //             indexed_annotated_functions,
    //             Some(&preliminary_annotations),
    //         )
    //     })
    //     .collect::<Result<Vec<FunctionAnnotations>, FunctionTypeInferenceError>>()?;
    //
    // // TODO: ^Optimise. There's no reason to do all of type inference again. We can re-use the graphs, and restart at the source of any SCC.
    // // TODO: We don't propagate annotations until convergence, so we don't always detect unsatisfiable queries
    // // Further, In a chain of three functions where the first two bodies have no function calls
    // // but rely on the third function to infer annotations, the annotations will not reach the first function.
    // let (ir, _) = preliminary_annotations.into_parts();
    // let annotated = AnnotatedUnindexedFunctions::new(ir, annotations_res.into_boxed_slice());
    // Ok(annotated)
    todo!()
}

pub fn annotate_function(
    function: &Function,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    indexed_annotated_functions: &IndexedAnnotatedFunctions,
    local_functions: Option<&AnnotatedUnindexedFunctions>,
) -> Result<AnnotatedFunction, FunctionTypeInferenceError> {
    // let root_graph = infer_types(
    //     snapshot,
    //     function.block(),
    //     function.variable_registry(),
    //     type_manager,
    //     &BTreeMap::new(),
    //     indexed_annotated_functions,
    //     local_functions,
    // )
    // .map_err(|err| FunctionTypeInferenceError::TypeInference {
    //     name: function.name().to_string(),
    //     typedb_source: err,
    // })?;
    // let body_annotations = TypeAnnotations::build(root_graph);
    // let return_types = function.return_operation().return_types(body_annotations.vertex_annotations());
    // Ok(FunctionAnnotations { return_annotations: return_types, block_annotations: body_annotations })
    todo!("We need to allow a function to contain an entire pipeline, instead of just a match block")
}

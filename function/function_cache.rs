/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter::zip, sync::Arc};

use compiler::annotation::function::{
    annotate_functions, AnnotatedFunction, AnnotatedFunctions, IndexedAnnotatedFunctions,
};
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::pipeline::function_signature::{FunctionSignatureIndex, HashMapFunctionSignatureIndex};
use storage::{sequence_number::SequenceNumber, snapshot::ReadableSnapshot, MVCCStorage};

use crate::{
    function::SchemaFunction,
    function_manager::{FunctionManager, FunctionReader},
    FunctionError,
};

#[derive(Debug)]
pub struct FunctionCache {
    parsed_functions: HashMap<DefinitionKey<'static>, SchemaFunction>,
    annotated_functions: Arc<IndexedAnnotatedFunctions>,
    index: HashMapFunctionSignatureIndex,
}

impl FunctionCache {
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        type_manager: &TypeManager,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, FunctionError> {
        let snapshot = storage.open_snapshot_read_at(open_sequence_number);
        let cache = Self::build_cache(&snapshot, type_manager);
        snapshot.close_resources();
        cache
    }

    pub(crate) fn build_cache(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<FunctionCache, FunctionError> {
        let schema_functions = FunctionReader::get_functions_all(snapshot)
            .map_err(|source| FunctionError::FunctionRetrieval { source })?;
        // Prepare ir
        let function_index = HashMapFunctionSignatureIndex::build(
            schema_functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)),
        );
        let functions_ir = FunctionManager::translate_functions(snapshot, &schema_functions, &function_index)?;

        // Run type-inference
        let unindexed_cache =
            annotate_functions(functions_ir, snapshot, type_manager, &IndexedAnnotatedFunctions::empty())
                .map_err(|source| FunctionError::CommittedFunctionsTypeCheck { typedb_source: source })?;

        // Convert them to our cache
        let mut parsed_functions = HashMap::new();
        let mut annotated_functions = HashMap::new();
        for (parsed, annotated) in zip(schema_functions.into_iter(), unindexed_cache.into_iter_functions()) {
            annotated_functions.insert(parsed.function_id.clone(), annotated);
            parsed_functions.insert(parsed.function_id.clone(), parsed);
        }

        Ok(FunctionCache {
            index: function_index,
            parsed_functions,
            annotated_functions: Arc::new(IndexedAnnotatedFunctions::new(annotated_functions)),
        })
    }

    pub(crate) fn get_function_key(&self, name: &str) -> Option<DefinitionKey<'static>> {
        self.index
            .get_function_signature(name)
            .unwrap()
            .map(|signature| signature.function_id().as_definition_key().unwrap().clone())
    }

    pub(crate) fn get_function(&self, definition_key: DefinitionKey<'static>) -> Option<&SchemaFunction> {
        self.parsed_functions.get(&definition_key)
    }

    pub(crate) fn get_annotated_functions(&self) -> Arc<IndexedAnnotatedFunctions> {
        self.annotated_functions.clone()
    }

    pub(crate) fn get_annotated_function(&self, definition_key: DefinitionKey<'static>) -> Option<&AnnotatedFunction> {
        self.annotated_functions.get_function(definition_key)
    }
}

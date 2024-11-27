/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter::zip, sync::Arc};

use compiler::annotation::function::{annotate_stored_functions, AnnotatedFunction, AnnotatedSchemaFunctions};
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
    annotated_functions: Arc<AnnotatedSchemaFunctions>,
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
        let mut functions_ir = FunctionManager::translate_functions(snapshot, &schema_functions, &function_index)?;

        // Run type-inference
        let annotated_functions = annotate_stored_functions(&mut functions_ir, snapshot, type_manager)
            .map_err(|source| FunctionError::CommittedFunctionsTypeCheck { typedb_source: source })?;

        let mut parsed_functions =
            schema_functions.into_iter().map(|parsed| (parsed.function_id.clone(), parsed)).collect();
        Ok(FunctionCache {
            index: function_index,
            parsed_functions,
            annotated_functions: Arc::new(annotated_functions),
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

    pub(crate) fn get_annotated_functions(&self) -> Arc<AnnotatedSchemaFunctions> {
        self.annotated_functions.clone()
    }

    pub(crate) fn get_annotated_function(&self, definition_key: DefinitionKey<'static>) -> Option<&AnnotatedFunction> {
        self.annotated_functions.get(&definition_key)
    }
}

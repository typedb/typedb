/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter::zip, sync::Arc};

use compiler::inference::{
    annotated_functions::{AnnotatedFunctions, IndexedAnnotatedFunctions},
    type_annotations::FunctionAnnotations,
    type_inference::infer_types_for_functions,
};
use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::program::{function::Function, function_signature::{FunctionIDAPI, FunctionSignature, HashMapFunctionSignatureIndex}};
use storage::{MVCCStorage, sequence_number::SequenceNumber};
use storage::snapshot::ReadableSnapshot;

use crate::{function::SchemaFunction, function_manager::FunctionReader, FunctionError};
use crate::function_manager::FunctionManager;

#[derive(Debug)]
pub struct FunctionCache {
    indexed_schema_functions: Box<[Option<SchemaFunction>]>,
    indexed_annotated_functions: Arc<IndexedAnnotatedFunctions>,
    index: HashMap<String, FunctionSignature>,
}

impl FunctionCache {
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        type_manager: &TypeManager,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, FunctionError> {
        let snapshot = storage
            .open_snapshot_read_at(open_sequence_number)
            .map_err(|error| FunctionError::SnapshotOpen { source: error })?;


        let (function_index, indexed_schema_functions, indexed_annotated_functions) = Self::build_indexed_annotated_schema_functions(
            &snapshot, type_manager
        )?;

        Ok(Self {
            indexed_schema_functions,
            indexed_annotated_functions: Arc::new(indexed_annotated_functions),
            index: function_index.into_map()
        })
    }

    pub(crate) fn build_indexed_annotated_schema_functions(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(HashMapFunctionSignatureIndex, Box<[Option<SchemaFunction>]>, IndexedAnnotatedFunctions), FunctionError> {
        let schema_functions = FunctionReader::get_functions_all(snapshot)
            .map_err(|source| FunctionError::FunctionRead { source })?;
        // Prepare ir
        let function_index =
            HashMapFunctionSignatureIndex::build(schema_functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let ir = FunctionManager::translate_functions(&function_index, &schema_functions)?;

        // Run type-inference
        let unindexed_cache =
            infer_types_for_functions(ir, snapshot, &type_manager, &IndexedAnnotatedFunctions::empty())
                .map_err(|source| FunctionError::TypeInference { source })?;

        // Convert them to our cache
        let required_cache_count =
            schema_functions.iter().map(|function| function.function_id.as_usize() + 1).max().unwrap_or(0);
        let mut schema_functions_index =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<SchemaFunction>]>>();
        let mut translated_schema_functions_index =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<Function>]>>();
        let mut annotated_schema_functions_index =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<FunctionAnnotations>]>>();

        let (boxed_translated, boxed_annotations) = unindexed_cache.into_parts();
        let zipped =
            zip(schema_functions.into_iter(), zip(boxed_translated.into_vec().into_iter(), boxed_annotations.into_vec().into_iter()));
        for (schema_function, (translated_function, annotations)) in zipped {
            let cache_index = schema_function.function_id.as_usize();
            schema_functions_index[cache_index] = Some(schema_function);
            translated_schema_functions_index[cache_index] = Some(translated_function);
            annotated_schema_functions_index[cache_index] = Some(annotations);
        }
        Ok((
            function_index,
            schema_functions_index,
            IndexedAnnotatedFunctions::new(translated_schema_functions_index, annotated_schema_functions_index)
        ))
    }

    pub(crate) fn get_function_key(&self, name: &str) -> Option<DefinitionKey<'static>> {
        self.index.get(name).map(|signature| signature.function_id().as_definition_key().unwrap().clone())
    }

    pub(crate) fn get_function(&self, definition_key: DefinitionKey<'static>) -> Option<&SchemaFunction> {
        self.indexed_schema_functions[definition_key.as_usize()].as_ref()
    }

    pub(crate) fn get_annotated_functions(&self) -> Arc<IndexedAnnotatedFunctions> {
        self.indexed_annotated_functions.clone()
    }

    pub(crate) fn get_function_ir(&self, definition_key: DefinitionKey<'static>) -> Option<&Function> {
        self.indexed_annotated_functions.get_function(definition_key)
    }

    pub(crate) fn get_function_annotations(
        &self,
        definition_key: DefinitionKey<'static>,
    ) -> Option<&FunctionAnnotations> {
        self.indexed_annotated_functions.get_annotations(definition_key)
    }
}

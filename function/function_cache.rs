/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, iter::zip, sync::Arc};

use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::{
    program::{
        function::FunctionIR,
        function_signature::{FunctionIDTrait, FunctionSignature, HashMapFunctionIndex},
        program::{CompiledFunctions, CompiledSchemaFunctions, Program},
    },
};
use storage::{sequence_number::SequenceNumber, MVCCStorage};

use crate::{function::SchemaFunction, function_manager::FunctionReader, FunctionManagerError};

pub struct FunctionCache {
    uncompiled: Box<[Option<SchemaFunction>]>,
    compiled: CompiledSchemaFunctions,
    index: HashMap<String, FunctionSignature>,
}

impl FunctionCache {
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        type_manager: &TypeManager,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, FunctionManagerError> {
        let snapshot = storage
            .open_snapshot_read_at(open_sequence_number)
            .map_err(|error| FunctionManagerError::SnapshotOpen { source: error })?;

        let functions = FunctionReader::get_functions_all(&snapshot)
            .map_err(|source| FunctionManagerError::FunctionRead { source })?;

        // Prepare ir
        let function_index =
            HashMapFunctionIndex::build(functions.iter().map(|f| (f.function_id.clone().into(), &f.parsed)));
        let ir = Program::compile_functions(&function_index, functions.iter().map(|f| &f.parsed)).unwrap();
        // Run type-inference
        let local_function_cache =
            infer_types_for_functions(ir, &snapshot, &type_manager, &CompiledSchemaFunctions::empty())
                .map_err(|source| FunctionManagerError::TypeInference { source })?;

        // Convert them to our cache
        let required_cache_count =
            functions.iter().map(|function| function.function_id.as_usize() + 1).max().unwrap_or(0);
        let mut uncompiled_functions =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<SchemaFunction>]>>();
        let mut ir_cache = (0..required_cache_count).map(|_| None).collect::<Box<[Option<FunctionIR>]>>();
        let mut annotations_cache =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<FunctionAnnotations>]>>();

        let (boxed_ir, boxed_annotations) = local_function_cache.into_parts();
        let zipped =
            zip(functions.into_iter(), zip(boxed_ir.into_vec().into_iter(), boxed_annotations.into_vec().into_iter()));
        let mut index = HashMap::new();
        for (function, (ir, annotations)) in zipped {
            index.insert(function.name().clone(), function.function_id.clone());
            let cache_index = function.function_id.as_usize();
            uncompiled_functions[cache_index] = Some(function);
            ir_cache[cache_index] = Some(ir);
            annotations_cache[cache_index] = Some(annotations);
        }
        let compiled_functions = CompiledSchemaFunctions::new(ir_cache, annotations_cache);
        let index = function_index.into_parts();
        Ok(Self { uncompiled: uncompiled_functions, compiled: compiled_functions, index })
    }

    pub(crate) fn get_function_key(&self, name: &str) -> Option<DefinitionKey<'static>> {
        self.index.get(name).map(|signature| signature.function_id().as_definition_key().unwrap().clone())
    }

    pub(crate) fn get_function(&self, definition_key: DefinitionKey<'static>) -> Option<&SchemaFunction> {
        self.uncompiled[definition_key.as_usize()].as_ref()
    }

    pub(crate) fn get_function_ir(&self, definition_key: DefinitionKey<'static>) -> Option<&FunctionIR> {
        self.compiled.get_function_ir(definition_key)
    }

    pub(crate) fn get_function_annotations(
        &self,
        definition_key: DefinitionKey<'static>,
    ) -> Option<&FunctionAnnotations> {
        self.compiled.get_function_annotations(definition_key)
    }
}

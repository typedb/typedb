/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    inference::type_inference::{FunctionAnnotations, TypeAnnotations},
    program::{
        block::FunctionalBlock,
        function::FunctionIR,
        function_signature::{FunctionID, FunctionIDTrait, FunctionSignatureIndex},
        FunctionDefinitionError, ProgramDefinitionError,
    },
    translator::{block_builder::TypeQLBuilder, function_builder::TypeQLFunctionBuilder},
};

pub struct Program {
    entry: FunctionalBlock,
    functions: Vec<FunctionIR>,
}

impl Program {
    pub fn new(entry: FunctionalBlock, functions: Vec<FunctionIR>) -> Self {
        // TODO: verify exactly the required functions are provided
        // TODO: ^ Why? I've since interpreted it as the query-local functions
        debug_assert!(Self::all_variables_categorised(&entry));
        Self { entry, functions }
    }

    pub fn entry(&self) -> &FunctionalBlock {
        &self.entry
    }

    pub fn entry_mut(&mut self) -> &mut FunctionalBlock {
        &mut self.entry
    }

    pub(crate) fn functions(&self) -> &Vec<FunctionIR> {
        &self.functions
    }

    pub(crate) fn into_parts(self) -> (FunctionalBlock, Vec<FunctionIR>) {
        let Self { entry, functions } = self;
        (entry, functions)
    }

    pub fn compile<'index>(
        function_index: &impl FunctionSignatureIndex,
        match_: &typeql::query::stage::Match,
        preamble_functions: Vec<&typeql::Function>,
    ) -> Result<Self, ProgramDefinitionError> {
        let functions: Vec<FunctionIR> = preamble_functions
            .iter()
            .map(|function| {
                TypeQLFunctionBuilder::build_ir(function_index, &function)
                    .map_err(|source| ProgramDefinitionError::FunctionDefinition { source })
            })
            .collect::<Result<Vec<FunctionIR>, ProgramDefinitionError>>()?;
        let entry = TypeQLBuilder::build_match(function_index, match_)
            .map_err(|source| ProgramDefinitionError::PatternDefinition { source })?;

        Ok(Self { entry, functions })
    }

    pub fn compile_functions<'index, 'functions>(
        function_index: &impl FunctionSignatureIndex,
        functions_to_compile: impl Iterator<Item = &'functions typeql::Function>,
    ) -> Result<Vec<FunctionIR>, FunctionDefinitionError> {
        let ir: Result<Vec<FunctionIR>, FunctionDefinitionError> =
            functions_to_compile.map(|function| TypeQLFunctionBuilder::build_ir(function_index, &function)).collect();
        Ok(ir?)
    }

    fn all_variables_categorised(block: &FunctionalBlock) -> bool {
        let context = block.context();
        let mut variables = context.variables();
        variables.all(|var| context.get_variable_category(var).is_some())
    }
}

pub struct AnnotatedProgram {
    pub(crate) entry: FunctionalBlock,
    pub(crate) entry_annotations: TypeAnnotations,
    pub(crate) local_functions: LocalFunctionCache,
    pub(crate) schema_functions: Arc<SchemaFunctionCache>,
}

impl AnnotatedProgram {
    pub(crate) fn new(
        entry: FunctionalBlock,
        entry_annotations: TypeAnnotations,
        local_functions: LocalFunctionCache,
        schema_functions: Arc<SchemaFunctionCache>,
    ) -> Self {
        Self { entry, entry_annotations, local_functions, schema_functions }
    }

    fn get_function_ir(&self, function_id: FunctionID) -> Option<&FunctionIR> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_function_ir(definition_key),
            FunctionID::Preamble(index) => self.local_functions.get_function_ir(index),
        }
    }

    fn get_function_annotations(&self, function_id: FunctionID) -> Option<&FunctionAnnotations> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_function_annotations(definition_key),
            FunctionID::Preamble(index) => self.local_functions.get_function_annotations(index),
        }
    }
}

pub struct SchemaFunctionCache {
    ir: Box<[Option<FunctionIR>]>,
    annotations: Box<[Option<FunctionAnnotations>]>,
}

impl SchemaFunctionCache {
    pub fn new(ir: Box<[Option<FunctionIR>]>, annotations: Box<[Option<FunctionAnnotations>]>) -> Self {
        Self { ir, annotations }
    }

    pub fn empty() -> Self {
        Self { ir: Box::new([]), annotations: Box::new([]) }
    }
}

pub trait CompiledFunctionCache {
    type KeyType;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR>;

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations>;
}

impl CompiledFunctionCache for SchemaFunctionCache {
    type KeyType = DefinitionKey<'static>;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id.as_usize())?.as_ref()
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id.as_usize())?.as_ref()
    }
}

// May hold IR & Annotations for either Schema functions or Preamble functions
// For schema functions, The index does not correspond to function_id.as_usize().
pub struct LocalFunctionCache {
    ir: Box<[FunctionIR]>,
    annotations: Box<[FunctionAnnotations]>,
}

impl LocalFunctionCache {
    pub fn new(ir: Box<[FunctionIR]>, annotations: Box<[FunctionAnnotations]>) -> Self {
        Self { ir, annotations }
    }

    pub fn iter_ir(&self) -> impl Iterator<Item = &FunctionIR> {
        self.ir.iter()
    }

    pub fn into_parts(self) -> (Box<[FunctionIR]>, Box<[FunctionAnnotations]>) {
        let Self { ir, annotations } = self;
        (ir, annotations)
    }
}

impl CompiledFunctionCache for LocalFunctionCache {
    type KeyType = usize;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id)
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashSet, sync::Arc};

    use typeql::query::Pipeline;

    use crate::{
        inference::{
            tests::{managers, schema_consts::setup_types, setup_storage},
            type_inference::infer_types,
        },
        pattern::{constraint::Constraint, Scope},
        program::{
            function_signature::{FunctionID, HashMapFunctionIndex},
            program::{CompiledFunctionCache, Program, SchemaFunctionCache},
            ProgramDefinitionError,
        },
        PatternDefinitionError,
    };

    #[test]
    fn from_typeql() {
        let raw_query = "
            with
            fun cat_names($c: animal) -> { name } :
                match
                    $c has cat-name $n;
                return { $n };

            match
                $x isa animal;
                $n in cat_names($x);
            filter $x;
        ";
        let query = typeql::parse_query(raw_query).unwrap().into_pipeline();
        let Pipeline { stages, preambles, .. } = query;
        let entry = stages.into_iter().map(|stage| stage.into_match()).find(|_| true).unwrap();
        let function = preambles.into_iter().map(|preamble| preamble.function).find(|_| true).unwrap();

        let function_id = FunctionID::Preamble(0);

        let should_be_unresolved_error = Program::compile(&HashMapFunctionIndex::empty(), &entry, vec![]);
        assert!(matches!(
            should_be_unresolved_error,
            Err(ProgramDefinitionError::PatternDefinition {
                source: PatternDefinitionError::UnresolvedFunction { .. }
            })
        ));

        let function_index = HashMapFunctionIndex::build([(FunctionID::Preamble(0), &function)].into_iter());
        let program = Program::compile(&function_index, &entry, vec![&function]).unwrap();
        match &program.entry.conjunction().constraints()[2] {
            Constraint::FunctionCallBinding(call) => {
                assert_eq!(function_id, call.function_call().function_id())
            }
            _ => assert!(false),
        }

        let storage = setup_storage();
        let (type_manager, _) = managers();
        let ((type_animal, type_cat, type_dog), _, _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);
        let empty_cache = Arc::new(SchemaFunctionCache::empty());

        let snapshot = storage.clone().open_snapshot_read();
        let var_f_c = program.functions[0]
            .block()
            .context()
            .get_variable_named("c", program.functions[0].block().scope_id())
            .unwrap()
            .clone();
        let var_x = program.entry.context().get_variable_named("x", program.entry.scope_id()).unwrap().clone();
        let annotated_program = infer_types(program, &snapshot, &type_manager, empty_cache).unwrap();
        assert_eq!(
            &Arc::new(HashSet::from([type_cat.clone()])),
            annotated_program
                .get_function_annotations(function_id)
                .unwrap()
                .block_annotations
                .variable_annotations(var_f_c)
                .unwrap()
        );
        assert_eq!(
            &Arc::new(HashSet::from([type_cat.clone()])),
            annotated_program.entry_annotations.variable_annotations(var_x).unwrap(),
        );
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use ir::program::{block::FunctionalBlock, function::Function, function_signature::FunctionID};

use crate::inference::{
    annotated_functions::{AnnotatedCommittedFunctions, AnnotatedFunctions, AnnotatedUncommittedFunctions},
    type_annotations::{FunctionAnnotations, TypeAnnotations},
};

pub struct AnnotatedProgram {
    pub(crate) entry: FunctionalBlock,
    pub(crate) entry_annotations: TypeAnnotations,
    pub(crate) uncommitted_functions: AnnotatedUncommittedFunctions,
    pub(crate) committed_functions: Arc<AnnotatedCommittedFunctions>,
}

impl AnnotatedProgram {
    pub(crate) fn new(
        entry: FunctionalBlock,
        entry_annotations: TypeAnnotations,
        local_functions: AnnotatedUncommittedFunctions,
        schema_functions: Arc<AnnotatedCommittedFunctions>,
    ) -> Self {
        Self { entry, entry_annotations, uncommitted_functions: local_functions, committed_functions: schema_functions }
    }

    pub fn get_entry(&self) -> &FunctionalBlock {
        &self.entry
    }

    pub fn get_entry_annotations(&self) -> &TypeAnnotations {
        &self.entry_annotations
    }

    fn get_function(&self, function_id: FunctionID) -> Option<&Function> {
        match function_id {
            FunctionID::Schema(definition_key) => self.committed_functions.get_function(definition_key),
            FunctionID::Preamble(index) => self.uncommitted_functions.get_function(index),
        }
    }

    fn get_function_annotations(&self, function_id: FunctionID) -> Option<&FunctionAnnotations> {
        match function_id {
            FunctionID::Schema(definition_key) => self.committed_functions.get_annotations(definition_key),
            FunctionID::Preamble(index) => self.uncommitted_functions.get_annotations(index),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashSet, sync::Arc};

    use ir::program::{
        function_signature::{FunctionID, HashMapFunctionIndex},
        program::Program,
    };
    use typeql::query::Pipeline;

    use crate::inference::{
        annotated_functions::AnnotatedCommittedFunctions,
        tests::{managers, schema_consts::setup_types, setup_storage},
        type_inference::infer_types,
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
        let function_index = HashMapFunctionIndex::build([(FunctionID::Preamble(0), &function)].into_iter());
        let program = Program::compile(&function_index, &entry, vec![&function]).unwrap();
        let storage = setup_storage();
        let (type_manager, _) = managers();
        let ((type_animal, type_cat, type_dog), _, _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager);
        let empty_cache = Arc::new(AnnotatedCommittedFunctions::empty());

        let snapshot = storage.clone().open_snapshot_read();
        let var_f_c = program.functions()[0]
            .block()
            .context()
            .get_variable_named("c", program.functions()[0].block().scope_id())
            .unwrap()
            .clone();
        let var_x = program.entry().context().get_variable_named("x", program.entry().scope_id()).unwrap().clone();
        let annotated_program = infer_types(program, &snapshot, &type_manager, empty_cache).unwrap();
        assert_eq!(
            &Arc::new(HashSet::from([type_cat.clone()])),
            annotated_program
                .get_function_annotations(function_id)
                .unwrap()
                .block_annotations
                .variable_annotations_of(var_f_c)
                .unwrap()
        );
        assert_eq!(
            &Arc::new(HashSet::from([type_cat.clone()])),
            annotated_program.entry_annotations.variable_annotations_of(var_x).unwrap(),
        );
    }
}

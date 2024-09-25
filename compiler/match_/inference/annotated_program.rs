/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};

use answer::variable::Variable;
use ir::program::{block::Block, function::Function, function_signature::FunctionID};

use crate::{
    expression::compiled_expression::CompiledExpression,
    match_::inference::{
        annotated_functions::{AnnotatedFunctions, AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_annotations::{FunctionAnnotations, TypeAnnotations},
    },
};

pub struct AnnotatedProgram {
    pub(crate) entry: Block,
    pub(crate) entry_annotations: TypeAnnotations,
    pub(crate) entry_expressions: HashMap<Variable, CompiledExpression>,
    pub(crate) schema_functions: Arc<IndexedAnnotatedFunctions>,
    pub(crate) preamble_functions: AnnotatedUnindexedFunctions,
}

impl AnnotatedProgram {
    pub(crate) fn new(
        entry: Block,
        entry_annotations: TypeAnnotations,
        entry_expressions: HashMap<Variable, CompiledExpression>,
        local_functions: AnnotatedUnindexedFunctions,
        schema_functions: Arc<IndexedAnnotatedFunctions>,
    ) -> Self {
        Self { entry, entry_annotations, entry_expressions, preamble_functions: local_functions, schema_functions }
    }

    pub fn get_entry(&self) -> &Block {
        &self.entry
    }

    pub fn entry_annotations(&self) -> &TypeAnnotations {
        &self.entry_annotations
    }

    pub fn get_entry_expressions(&self) -> &HashMap<Variable, CompiledExpression> {
        &self.entry_expressions
    }

    pub fn contains_functions(&self) -> bool {
        self.schema_functions.is_empty()
    }

    fn get_function(&self, function_id: FunctionID) -> Option<&Function> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_function(definition_key),
            FunctionID::Preamble(index) => self.preamble_functions.get_function(index),
        }
    }

    fn get_function_annotations(&self, function_id: FunctionID) -> Option<&FunctionAnnotations> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_annotations(definition_key),
            FunctionID::Preamble(index) => self.preamble_functions.get_annotations(index),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ir::{
        program::function_signature::{FunctionID, HashMapFunctionSignatureIndex},
        translation::{function::translate_function, match_::translate_match, TranslationContext},
    };
    use typeql::query::Pipeline;

    use crate::match_::inference::{
        annotated_functions::{AnnotatedFunctions, IndexedAnnotatedFunctions},
        tests::{managers, schema_consts::setup_types, setup_storage},
        type_inference::{infer_types_for_functions, infer_types_for_match_block},
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
            select $x;
        ";
        let query = typeql::parse_query(raw_query).unwrap().into_pipeline();
        let Pipeline { stages, preambles, .. } = query;
        let typeql_match = stages.into_iter().map(|stage| stage.into_match()).find(|_| true).unwrap();
        let typeql_function = preambles.into_iter().map(|preamble| preamble.function).find(|_| true).unwrap();
        let function_id = FunctionID::Preamble(0);
        let function_index =
            HashMapFunctionSignatureIndex::build([(FunctionID::Preamble(0), &typeql_function)].into_iter());
        let function = translate_function(&function_index, &typeql_function).unwrap();

        let mut translation_context = TranslationContext::new();
        let entry = translate_match(&mut translation_context, &function_index, &typeql_match).unwrap().finish();
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();
        let ((type_animal, type_cat, type_dog), _, _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let empty_cache = IndexedAnnotatedFunctions::empty();

        let snapshot = storage.clone().open_snapshot_read();
        let &var_f_c = function.variable_registry().variable_names().iter().find(|(_, v)| v.as_str() == "c").unwrap().0;
        let var_x = *translation_context.visible_variables.get("x").unwrap();

        let function_annotations =
            infer_types_for_functions(vec![function], &snapshot, &type_manager, &empty_cache).unwrap();

        let entry_annotations = infer_types_for_match_block(
            &entry,
            &translation_context.variable_registry,
            &snapshot,
            &type_manager,
            &BTreeMap::new(),
            &empty_cache,
            &function_annotations,
        )
        .unwrap();

        assert_eq!(
            BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]),
            **function_annotations
                .get_annotations(function_id.as_usize())
                .unwrap()
                .block_annotations
                .vertex_annotations_of(&var_f_c.into())
                .unwrap()
        );
        assert_eq!(
            BTreeSet::from([type_animal.clone(), type_cat.clone(), type_dog.clone()]),
            **entry_annotations.vertex_annotations_of(&var_x.into()).unwrap(),
        );
    }
}

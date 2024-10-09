/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use typeql::query::stage::delete::DeletableKind;

use crate::{
    pipeline::{block::Block, function_signature::HashMapFunctionSignatureIndex},
    translation::{
        constraints::{add_statement, add_typeql_relation, register_typeql_var},
        TranslationContext,
    },
    RepresentationError,
};

pub fn translate_insert(
    context: &mut TranslationContext,
    insert: &typeql::query::stage::Insert,
) -> Result<Block, RepresentationError> {
    let mut builder = Block::builder(context.new_block_builder_context());
    let function_index = HashMapFunctionSignatureIndex::empty();
    for statement in &insert.statements {
        add_statement(&function_index, &mut builder.conjunction_mut(), statement)?;
    }
    Ok(builder.finish())
}

pub fn translate_delete(
    context: &mut TranslationContext,
    delete: &typeql::query::stage::Delete,
) -> Result<(Block, Vec<Variable>), RepresentationError> {
    let mut builder = Block::builder(context.new_block_builder_context());
    let mut conjunction = builder.conjunction_mut();
    let mut constraints = conjunction.constraints_mut();
    let mut deleted_concepts = Vec::new();
    for deletable in &delete.deletables {
        match &deletable.kind {
            DeletableKind::Has { attribute, owner } => {
                let translated_owner = register_typeql_var(&mut constraints, owner)?;
                let translated_attribute = register_typeql_var(&mut constraints, attribute)?;
                constraints.add_has(translated_owner, translated_attribute)?;
            }
            DeletableKind::Links { players, relation } => {
                let translated_relation = register_typeql_var(&mut constraints, relation)?;
                add_typeql_relation(&mut constraints, translated_relation, players)?;
            }
            DeletableKind::Concept { variable } => {
                let translated_variable = constraints.get_or_declare_variable(variable.name().unwrap())?;
                deleted_concepts.push(translated_variable);
            }
        }
    }
    for var in &deleted_concepts {
        builder.context_mut().record_variable_reference(var.clone());
    }
    Ok((builder.finish(), deleted_concepts))
}

/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use typeql::query::stage::delete::DeletableKind;
use typeql::statement::thing::RolePlayer;

use crate::{
    pipeline::{block::Block, function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{
        constraints::{add_statement, add_typeql_relation, register_typeql_var},
        TranslationContext,
    },
    RepresentationError,
};
use crate::pattern::variable_category::VariableCategory;

pub fn translate_insert(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    insert: &typeql::query::stage::Insert,
) -> Result<Block, Box<RepresentationError>> {
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let function_index = HashMapFunctionSignatureIndex::empty();
    for statement in &insert.statements {
        add_statement(&function_index, &mut builder.conjunction_mut(), statement)?;
    }
    builder.finish()
}


fn verify_deleted_variables_available(context: &mut TranslationContext, delete: &typeql::query::stage::Delete) -> Result<(), Box<RepresentationError>> {
    fn verify_variable_available(context: &TranslationContext, var: &typeql::Variable) -> Result<Variable, Box<RepresentationError>> {
        match context.get_variable(var.name().unwrap()) {
            Some(translated) => Ok(translated),
            None => Err(Box::new(RepresentationError::DeleteVariableUnavailable {
                variable: var.name().unwrap().to_owned()
            }))
        }
    }
    delete.deletables.iter().try_for_each(|deletable| {
        match &deletable.kind {
            DeletableKind::Has { owner, attribute } => {
                verify_variable_available(context, owner)?;
                verify_variable_available(context, attribute)?;
            }
            DeletableKind::Links { relation, players } => {
                verify_variable_available(context, relation)?;
                players.role_players.iter().try_for_each(|rp| match rp {
                    RolePlayer::Typed(_, player) | RolePlayer::Untyped(player) => {
                        verify_variable_available(context, player).map(|_| ())
                    }
                })?;
            }
            DeletableKind::Concept { variable } => {
                let translated = verify_variable_available(context, variable)?;
                context.variable_registry.set_deleted_variable_category(translated)?;
            }
        };
        Ok(())
    })
}

pub fn translate_delete(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    delete: &typeql::query::stage::Delete,
) -> Result<(Block, Vec<Variable>), Box<RepresentationError>> {
    verify_deleted_variables_available(context, delete)?;
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
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
    Ok((builder.finish()?, deleted_concepts))
}

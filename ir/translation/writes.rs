/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use typeql::{common::Spanned, Expression, Identifier, query::stage::delete::DeletableKind, Statement, statement::thing::RolePlayer};
use typeql::common::Span;
use typeql::statement::thing::{Constraint, HasValue, Head};

use crate::{
    pipeline::{block::Block, function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{
        constraints::{add_statement, add_typeql_relation, register_typeql_var},
        TranslationContext,
    },
    RepresentationError,
};

macro_rules! verify_variable_available {
    ($context:ident, $var:expr -> $error:ident ) => {
        match $context.get_variable($var.name().unwrap()) {
            Some(translated) => Ok(translated),
            None => Err(Box::new(RepresentationError::$error {
                variable: $var.name().unwrap().to_owned(),
                source_span: $var.span(),
            })),
        }
    };
}

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

pub fn translate_update(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    update: &typeql::query::stage::Update,
) -> Result<(Block, Vec<Variable>), Box<RepresentationError>> {
    todo!("Not implemeneted update")
    // /*
    //     let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    //     let function_index = HashMapFunctionSignatureIndex::empty();
    //     for statement in &insert.statements {
    //         add_statement(&function_index, &mut builder.conjunction_mut(), statement)?;
    //     }
    //     builder.finish()
    //  */
    // verify_deleted_variables_available(context, delete)?;
    // let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    // let mut conjunction = builder.conjunction_mut();
    // let mut constraints = conjunction.constraints_mut();
    // let mut deleted_concepts = Vec::new();
    // for deletable in &delete.deletables {
    //     match &deletable.kind {
    //         DeletableKind::Has { attribute, owner } => {
    //             let translated_owner = register_typeql_var(&mut constraints, owner)?;
    //             let translated_attribute = register_typeql_var(&mut constraints, attribute)?;
    //             constraints.add_has(translated_owner, translated_attribute, deletable.span())?;
    //         }
    //         DeletableKind::Links { players, relation } => {
    //             let translated_relation = register_typeql_var(&mut constraints, relation)?;
    //             add_typeql_relation(&mut constraints, translated_relation, players)?;
    //         }
    //         DeletableKind::Concept { variable } => {
    //             let translated_variable =
    //                 constraints.get_or_declare_variable(variable.name().unwrap(), variable.span())?;
    //             deleted_concepts.push(translated_variable);
    //         }
    //     }
    // }
    // Ok((builder.finish()?, deleted_concepts))
}

pub fn translate_delete(
    context: &mut TranslationContext,
    value_parameters: &mut ParameterRegistry,
    delete: &typeql::query::stage::Delete,
) -> Result<(Block, Vec<Variable>), Box<RepresentationError>> {
    validate_deleted_variables_availability(context, delete)?;
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let mut conjunction = builder.conjunction_mut();
    let mut constraints = conjunction.constraints_mut();
    let mut deleted_concepts = Vec::new();
    for deletable in &delete.deletables {
        match &deletable.kind {
            DeletableKind::Has { attribute, owner } => {
                let translated_owner = register_typeql_var(&mut constraints, owner)?;
                let translated_attribute = register_typeql_var(&mut constraints, attribute)?;
                constraints.add_has(translated_owner, translated_attribute, deletable.span())?;
            }
            DeletableKind::Links { players, relation } => {
                let translated_relation = register_typeql_var(&mut constraints, relation)?;
                add_typeql_relation(&mut constraints, translated_relation, players)?;
            }
            DeletableKind::Concept { variable } => {
                let translated_variable =
                    constraints.get_or_declare_variable(variable.name().unwrap(), variable.span())?;
                deleted_concepts.push(translated_variable);
            }
        }
    }
    Ok((builder.finish()?, deleted_concepts))
}

fn validate_update_statements_and_variables(
    context: &mut TranslationContext,
    update: &typeql::query::stage::Update,
) -> Result<(), Box<RepresentationError>> {
    // TODO: It's not really comfortable to check the variable availability here.
    update.statements.iter().try_for_each(|statement| {
        if let Statement::Thing(thing_statement) = statement {
            match &thing_statement.head {
                Head::Variable(variable) => {
                    verify_variable_available!(context, variable -> UpdateVariableUnavailable)?;
                },
                Head::Relation(_, _) => todo!("Return error here?"),
            }

            for constraint in thing_statement.constraints {
                match constraint {
                    Constraint::Has(has_constraint) => {
                        match &has_constraint.value {
                            HasValue::Variable(variable) => {verify_variable_available!(context, variable -> UpdateVariableUnavailable)?;}
                            HasValue::Expression(expression) => validate_update_expression_variables_availability(context, expression)?,
                            HasValue::Comparison(_) => todo!("Check here or not?"),
                        }
                    }
                    Constraint::Links(links_constraint) => {
//todo impelemnt
                    }
                    Constraint::Isa(_) | Constraint::Iid(_) => todo!("Return error here?"),
                }
            }
        } else {
            todo!("Return error here?")
        }
        match &deletable.kind {
            DeletableKind::Has { owner, attribute } => {
                verify_variable_available!(context, owner -> DeleteVariableUnavailable)?;
                verify_variable_available!(context, attribute -> DeleteVariableUnavailable)?;
            }
            DeletableKind::Links { relation, players } => {
                verify_variable_available!(context, relation -> DeleteVariableUnavailable)?;
                players.role_players.iter().try_for_each(|rp| match rp {
                    RolePlayer::Typed(_, player) | RolePlayer::Untyped(player) => {
                        verify_variable_available!(context, player -> DeleteVariableUnavailable).map(|_| ())
                    }
                })?;
            }
            DeletableKind::Concept { variable } => {
                let translated = verify_variable_available!(context, variable -> DeleteVariableUnavailable)?;
                context.variable_registry.set_deleted_variable_category(translated)?;
            }
        };
        Ok(())
    })
}

fn validate_update_expression_variables_availability(
    context: &mut TranslationContext,
    expression: &Expression,
) -> Result<(), Box<RepresentationError>> {
    match expression {
        Expression::Variable(variable) => {
            verify_variable_available!(context, variable -> DeleteVariableUnavailable)?;
            Ok(())
        }
        Expression::ListIndex(list_index) => {
            verify_variable_available!(context, list_index.variable -> DeleteVariableUnavailable)?;
            validate_update_expression_variables_availability(context, &list_index.index)
        }
        Expression::Value(value) => Ok(()),
        Expression::Function(function_call) => {
            // TODO: Verify function names here or later? What happens if I don't?
            function_call.args.iter().try_fold((), |_, arg| validate_update_expression_variables_availability(context, &arg))
        }
        Expression::Operation(operation) => {
            validate_update_expression_variables_availability(context, &operation.left)?;
            validate_update_expression_variables_availability(context, &operation.right)
        }
        Expression::Paren(paren) => {
            validate_update_expression_variables_availability(context, &paren.inner)
        }
        Expression::List(list) => {
            list.items.iter().try_fold((), |_, item| validate_update_expression_variables_availability(context, &item))
        }
        Expression::ListIndexRange(list_index_range) => {
            verify_variable_available!(context, list_index_range.var -> DeleteVariableUnavailable)?;
            validate_update_expression_variables_availability(context, &list_index_range.from)?;
            validate_update_expression_variables_availability(context, &list_index_range.to)
        }
    }
}

fn validate_deleted_variables_availability(
    context: &mut TranslationContext,
    delete: &typeql::query::stage::Delete,
) -> Result<(), Box<RepresentationError>> {
    delete.deletables.iter().try_for_each(|deletable| {
        match &deletable.kind {
            DeletableKind::Has { owner, attribute } => {
                verify_variable_available!(context, owner -> DeleteVariableUnavailable)?;
                verify_variable_available!(context, attribute -> DeleteVariableUnavailable)?;
            }
            DeletableKind::Links { relation, players } => {
                verify_variable_available!(context, relation -> DeleteVariableUnavailable)?;
                players.role_players.iter().try_for_each(|rp| match rp {
                    RolePlayer::Typed(_, player) | RolePlayer::Untyped(player) => {
                        verify_variable_available!(context, player -> DeleteVariableUnavailable).map(|_| ())
                    }
                })?;
            }
            DeletableKind::Concept { variable } => {
                let translated = verify_variable_available!(context, variable -> DeleteVariableUnavailable)?;
                context.variable_registry.set_deleted_variable_category(translated)?;
            }
        };
        Ok(())
    })
}

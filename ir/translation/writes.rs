/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use error::UnimplementedFeature;
use typeql::{
    common::Spanned,
    query::stage::{
        delete::{Deletable, DeletableKind},
        Put,
    },
    statement::thing::{Constraint, HasValue, Head, RolePlayer},
    Expression, Statement,
};

use crate::{
    pattern::conjunction::ConjunctionBuilder,
    pipeline::{block::Block, function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{
        constraints::{add_statement, add_typeql_relation, register_typeql_var},
        match_::add_patterns,
        verify_variable_available, PipelineTranslationContext,
    },
    RepresentationError,
};

pub fn translate_insert(
    context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    insert: &typeql::query::stage::Insert,
) -> Result<Block, Box<RepresentationError>> {
    validate_insert(insert)?;
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let function_index = HashMapFunctionSignatureIndex::empty();
    add_patterns(&function_index, &mut builder.conjunction_mut(), &insert.patterns)?;
    builder.finish()
}

fn validate_insert(insert: &typeql::query::stage::Insert) -> Result<(), Box<RepresentationError>> {
    for pattern in &insert.patterns {
        validate_insert_pattern(pattern)?;
    }
    Ok(())
}

fn validate_insert_pattern(pattern: &typeql::Pattern) -> Result<(), Box<RepresentationError>> {
    match pattern {
        typeql::Pattern::Optional(typeql::pattern::Optional { patterns, .. }) => {
            let [pattern] = &**patterns else {
                return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                    feature: UnimplementedFeature::MultipleOptionalWrites,
                }));
            };
            if matches!(pattern, typeql::Pattern::Optional(typeql::pattern::Optional { .. })) {
                return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                    feature: UnimplementedFeature::NestedOptionalWrites,
                }));
            }
            if let typeql::Pattern::Statement(Statement::Thing(thing_stmt)) = pattern {
                if let [Constraint::Isa(_) | Constraint::Iid(_)] = &*thing_stmt.constraints {
                    return Err(Box::new(RepresentationError::IllegalStatementForInsert {
                        source_span: thing_stmt.span(),
                    }));
                } else if thing_stmt.constraints.len() != 1 {
                    return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                        feature: UnimplementedFeature::MultipleOptionalWrites,
                    }));
                }
            }
            validate_insert_pattern(pattern)?;
        }
        typeql::Pattern::Conjunction(typeql::pattern::Conjunction { span, .. })
        | typeql::Pattern::Disjunction(typeql::pattern::Disjunction { span, .. })
        | typeql::Pattern::Negation(typeql::pattern::Negation { span, .. }) => {
            return Err(Box::new(RepresentationError::IllegalNestedPatternForInsert { source_span: *span }))
        }
        typeql::Pattern::Statement(Statement::Thing(thing_stmt)) => {
            for constraint in &thing_stmt.constraints {
                match constraint {
                    Constraint::Iid(_) => {
                        return Err(Box::new(RepresentationError::IllegalStatementForInsert {
                            source_span: thing_stmt.span(),
                        }))
                    }
                    Constraint::Isa(_) | Constraint::Has(_) | Constraint::Links(_) => (),
                }
            }
        }
        typeql::Pattern::Statement(stmt) => {
            return Err(Box::new(RepresentationError::IllegalStatementForInsert { source_span: stmt.span() }))
        }
    }
    Ok(())
}

pub fn translate_update(
    context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    update: &typeql::query::stage::Update,
) -> Result<Block, Box<RepresentationError>> {
    validate_update_statements_and_variables(context, update)?;
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let function_index = HashMapFunctionSignatureIndex::empty();
    for statement in &update.statements {
        add_statement(&function_index, &mut builder.conjunction_mut(), statement)?;
    }
    builder.finish()
}

pub fn translate_put(
    context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    put: &Put,
) -> Result<Block, Box<RepresentationError>> {
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let function_index = HashMapFunctionSignatureIndex::empty();
    for statement in &put.statements {
        add_statement(&function_index, &mut builder.conjunction_mut(), statement)?;
    }
    let block = builder.finish()?;
    block.conjunction().constraints().iter().try_for_each(|constraint| match constraint {
        crate::pattern::constraint::Constraint::RoleName(_)
        | crate::pattern::constraint::Constraint::Isa(_)
        | crate::pattern::constraint::Constraint::Links(_)
        | crate::pattern::constraint::Constraint::Has(_)
        | crate::pattern::constraint::Constraint::Comparison(_)
        | crate::pattern::constraint::Constraint::LinksDeduplication(_)
        | crate::pattern::constraint::Constraint::Value(_) => Ok(()),
        constraint => Err(Box::new(RepresentationError::IllegalStatementForPut {
            constraint_type: constraint.name().to_owned(),
            source_span: constraint.source_span(),
        })),
    })?;
    Ok(block)
}

pub fn translate_delete(
    context: &mut PipelineTranslationContext,
    value_parameters: &mut ParameterRegistry,
    delete: &typeql::query::stage::Delete,
) -> Result<(Block, Vec<Variable>), Box<RepresentationError>> {
    validate_delete(delete)?;
    validate_deleted_variables_availability(context, delete)?;
    let mut builder = Block::builder(context.new_block_builder_context(value_parameters));
    let mut deleted_concepts = Vec::new();
    add_deletables(&delete.deletables, builder.conjunction_mut(), &mut deleted_concepts)?;
    let block = builder.finish()?;
    context.last_stage_visible_variables.retain(|name, var| !deleted_concepts.contains(var));
    Ok((block, deleted_concepts))
}

fn validate_delete(delete: &typeql::query::stage::Delete) -> Result<(), Box<RepresentationError>> {
    for deletable in &delete.deletables {
        match &deletable.kind {
            DeletableKind::Optional { deletables } if deletables.len() != 1 => {
                return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                    feature: UnimplementedFeature::MultipleOptionalWrites,
                }));
            }
            DeletableKind::Optional { deletables } if matches!(deletables[0].kind, DeletableKind::Optional { .. }) => {
                return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                    feature: UnimplementedFeature::NestedOptionalWrites,
                }));
            }
            _ => (),
        }
    }
    Ok(())
}

fn add_deletables(
    deletables: &[Deletable],
    mut conjunction: ConjunctionBuilder<'_, '_>,
    deleted_concepts: &mut Vec<Variable>,
) -> Result<(), Box<RepresentationError>> {
    for deletable in deletables {
        match &deletable.kind {
            DeletableKind::Has { attribute, owner } => {
                let mut constraints = conjunction.constraints_mut();
                let translated_owner = register_typeql_var(&mut constraints, owner)?;
                let translated_attribute = register_typeql_var(&mut constraints, attribute)?;
                constraints.add_has(translated_owner, translated_attribute, deletable.span())?;
            }
            DeletableKind::Links { players, relation } => {
                let mut constraints = conjunction.constraints_mut();
                let translated_relation = register_typeql_var(&mut constraints, relation)?;
                add_typeql_relation(&mut constraints, translated_relation, players)?;
            }
            DeletableKind::Concept { variable } => {
                let mut constraints = conjunction.constraints_mut();
                let translated_variable =
                    constraints.get_or_declare_variable(variable.name().unwrap(), variable.span())?;
                deleted_concepts.push(translated_variable);
            }
            DeletableKind::Optional { deletables } => {
                debug_assert_eq!(deletables.len(), 1);
                debug_assert!(!matches!(deletables[0].kind, DeletableKind::Optional { .. }));
                let optional_builder = conjunction.add_optional(deletable.span())?;
                add_deletables(deletables, optional_builder, deleted_concepts)?;
            }
        }
    }
    Ok(())
}

fn validate_update_statements_and_variables(
    context: &mut PipelineTranslationContext,
    update: &typeql::query::stage::Update,
) -> Result<(), Box<RepresentationError>> {
    update.statements.iter().try_for_each(|statement| {
        if let Statement::Thing(thing_statement) = statement {
            match &thing_statement.head {
                Head::Variable(variable) => {
                    verify_variable_available!(context, variable => UpdateVariableUnavailable)?;
                }
                Head::Relation(_, relation) => {
                    return Err(Box::new(RepresentationError::IllegalStatementForUpdate { source_span: relation.span }))
                }
            }

            for constraint in &thing_statement.constraints {
                match constraint {
                    Constraint::Has(has_constraint) => match &has_constraint.value {
                        HasValue::Variable(variable) => {
                            verify_variable_available!(context, variable => UpdateVariableUnavailable)?;
                        }
                        HasValue::Expression(expression) => {
                            validate_update_expression_variables_availability(context, expression)?
                        }
                        HasValue::Comparison(comparison) => {
                            validate_update_expression_variables_availability(context, &comparison.rhs)?
                        }
                    },
                    Constraint::Links(links_constraint) => {
                        links_constraint.relation.role_players.iter().try_for_each(|rp| match rp {
                            RolePlayer::Typed(_, player) | RolePlayer::Untyped(player) => {
                                verify_variable_available!(context, player => UpdateVariableUnavailable).map(|_| ())
                            }
                        })?;
                    }
                    Constraint::Isa(isa) => {
                        return Err(Box::new(RepresentationError::IllegalStatementForUpdate { source_span: isa.span }))
                    }
                    Constraint::Iid(iid) => {
                        return Err(Box::new(RepresentationError::IllegalStatementForUpdate { source_span: iid.span }))
                    }
                }
            }
        } else {
            return Err(Box::new(RepresentationError::IllegalStatementForUpdate { source_span: statement.span() }));
        }
        Ok(())
    })
}

fn validate_update_expression_variables_availability(
    context: &mut PipelineTranslationContext,
    expression: &Expression,
) -> Result<(), Box<RepresentationError>> {
    match expression {
        Expression::Variable(variable) => {
            verify_variable_available!(context, variable => DeleteVariableUnavailable)?;
            Ok(())
        }
        Expression::ListIndex(list_index) => {
            verify_variable_available!(context, list_index.variable => DeleteVariableUnavailable)?;
            validate_update_expression_variables_availability(context, &list_index.index)
        }
        Expression::Value(value) => Ok(()),
        Expression::Function(function_call) => {
            // TODO: We may want to verify user-defined function names here as well.
            // They are generally not supported in the execution, so we skip it now.
            function_call
                .args
                .iter()
                .try_fold((), |_, arg| validate_update_expression_variables_availability(context, arg))
        }
        Expression::Operation(operation) => {
            validate_update_expression_variables_availability(context, &operation.left)?;
            validate_update_expression_variables_availability(context, &operation.right)
        }
        Expression::Paren(paren) => validate_update_expression_variables_availability(context, &paren.inner),
        Expression::List(list) => {
            list.items.iter().try_fold((), |_, item| validate_update_expression_variables_availability(context, item))
        }
        Expression::ListIndexRange(list_index_range) => {
            verify_variable_available!(context, list_index_range.var => DeleteVariableUnavailable)?;
            validate_update_expression_variables_availability(context, &list_index_range.from)?;
            validate_update_expression_variables_availability(context, &list_index_range.to)
        }
    }
}

fn validate_deleted_variables_availability(
    context: &mut PipelineTranslationContext,
    delete: &typeql::query::stage::Delete,
) -> Result<(), Box<RepresentationError>> {
    for deletable in &delete.deletables {
        validate_deleted_variable_availability_deletable(context, deletable)?;
    }
    Ok(())
}

fn validate_deleted_variable_availability_deletable(
    context: &mut PipelineTranslationContext,
    deletable: &Deletable,
) -> Result<(), Box<RepresentationError>> {
    match &deletable.kind {
        DeletableKind::Has { owner, attribute } => {
            verify_variable_available!(context, owner => DeleteVariableUnavailable)?;
            verify_variable_available!(context, attribute => DeleteVariableUnavailable)?;
        }
        DeletableKind::Links { relation, players } => {
            verify_variable_available!(context, relation => DeleteVariableUnavailable)?;
            players.role_players.iter().try_for_each(|rp| match rp {
                RolePlayer::Typed(_, player) | RolePlayer::Untyped(player) => {
                    verify_variable_available!(context, player => DeleteVariableUnavailable).map(|_| ())
                }
            })?;
        }
        DeletableKind::Concept { variable } => {
            let translated = verify_variable_available!(context, variable => DeleteVariableUnavailable)?;
            context.variable_registry.set_deleted_variable_category(translated)?;
        }
        DeletableKind::Optional { deletables } if deletables.len() == 1 => {
            let deletable = &deletables[0];
            debug_assert!(!matches!(deletable.kind, DeletableKind::Optional { .. }));
            validate_deleted_variable_availability_deletable(context, deletable)?;
        }
        DeletableKind::Optional { deletables } => {
            unreachable!("Only one statement permitted in a delete try block! {deletable:?}");
        }
    };
    Ok(())
}

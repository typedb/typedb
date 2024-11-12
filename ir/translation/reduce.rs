/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::{
    query::{pipeline::stage::Operator as TypeQLOperator, stage::reduce::Reducer as TypeQLReducer},
    token::ReduceOperator as TypeQLReduceOperator,
};

use crate::{
    pattern::variable_category::VariableCategory,
    pipeline::{
        reduce::{Reduce, Reducer},
        VariableRegistry,
    },
    RepresentationError,
    translation::TranslationContext,
};

pub fn translate_reduce(
    context: &mut TranslationContext,
    typeql_reduce: &typeql::query::stage::Reduce,
) -> Result<Reduce, RepresentationError> {
    let mut reductions = Vec::with_capacity(typeql_reduce.reduce_assignments.len());
    for reduce_assign in &typeql_reduce.reduce_assignments {
        let reducer = build_reducer(context, &reduce_assign.reducer)?;
        let (category, is_optional) = resolve_category_optionality(&reducer, &context.variable_registry);
        let assigned_var = context.register_reduced_variable(
            reduce_assign.variable.name().unwrap(),
            category,
            is_optional,
            reducer.clone(),
        );
        reductions.push((assigned_var, reducer));
    }

    let group = match &typeql_reduce.within_group {
        None => Vec::new(),
        Some(group) => group
            .iter()
            .map(|typeql_var| {
                let var_name = typeql_var.name().unwrap();
                context.visible_variables.get(var_name).map_or_else(
                    || {
                        Err(RepresentationError::OperatorStageVariableUnavailable {
                            variable_name: var_name.to_owned(),
                            declaration: typeql::query::pipeline::stage::Stage::Operator(TypeQLOperator::Reduce(
                                typeql_reduce.clone(),
                            )),
                        })
                    },
                    |&variable| Ok(variable),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    Ok(Reduce::new(reductions, group))
}

fn resolve_category_optionality(reduce: &Reducer, variable_registry: &VariableRegistry) -> (VariableCategory, bool) {
    match reduce {
        Reducer::Count => (VariableCategory::Value, false),
        Reducer::CountVar(_) => (VariableCategory::Value, false),
        Reducer::Sum(_) => (VariableCategory::Value, false),
        Reducer::Max(_) => (VariableCategory::Value, true),
        Reducer::Mean(_) => (VariableCategory::Value, true),
        Reducer::Median(_) => (VariableCategory::Value, true),
        Reducer::Min(_) => (VariableCategory::Value, true),
        Reducer::Std(_) => (VariableCategory::Value, true),
    }
}

pub(crate) fn build_reducer(
    context: &TranslationContext,
    reduce_value: &TypeQLReducer,
) -> Result<Reducer, RepresentationError> {
    match reduce_value {
        TypeQLReducer::Count(count) => match &count.variable {
            None => Ok(Reducer::Count),
            Some(typeql_var) => match context.get_variable(typeql_var.name().unwrap()) {
                None => Err(RepresentationError::ReduceVariableNotAvailable {
                    variable_name: typeql_var.name().unwrap().to_owned(),
                    declaration: reduce_value.clone(),
                }),
                Some(var) => Ok(Reducer::CountVar(var)),
            },
        },
        TypeQLReducer::Stat(stat) => {
            let Some(var) = context.get_variable(stat.variable.name().unwrap()) else {
                return Err(RepresentationError::ReduceVariableNotAvailable {
                    variable_name: stat.variable.name().unwrap().to_owned(),
                    declaration: reduce_value.clone(),
                });
            };
            match &stat.reduce_operator {
                TypeQLReduceOperator::Sum => Ok(Reducer::Sum(var)),
                TypeQLReduceOperator::Max => Ok(Reducer::Max(var)),
                TypeQLReduceOperator::Mean => Ok(Reducer::Mean(var)),
                TypeQLReduceOperator::Median => Ok(Reducer::Median(var)),
                TypeQLReduceOperator::Min => Ok(Reducer::Min(var)),
                TypeQLReduceOperator::Std => Ok(Reducer::Std(var)),
                TypeQLReduceOperator::Count | TypeQLReduceOperator::List => unreachable!(), // Not stats
            }
        }
    }
}

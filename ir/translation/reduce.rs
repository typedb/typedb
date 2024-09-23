/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::HashMap;
use typeql::query::stage::reduce::{ReduceValue, Stat};
use typeql::token::ReduceOperator;
use answer::variable::Variable;
use crate::pattern::variable_category::{VariableCategory, VariableOptionality};
use crate::PatternDefinitionError;
use crate::program::block::VariableRegistry;
use crate::program::function::Reducer;
use crate::program::reduce::Reduce;
use crate::translation::TranslationContext;

pub fn translate_reduce(
    context: &mut TranslationContext,
    typeql_reduce: &typeql::query::stage::Reduce,
) -> Result<Reduce, PatternDefinitionError> {
    let mut reductions = Vec::with_capacity(typeql_reduce.reductions.len());
    for reduce_assign in &typeql_reduce.reductions {
        let reducer = build_reduce_value(&context.visible_variables, &reduce_assign.reduce_value, typeql_reduce)?;
        let (category, is_optional) = resolve_assigned_variable_category_optionality(&reducer, &context.variable_registry);
        let assigned_var = context.register_reduced_variable(reduce_assign.assign_to.name().unwrap(), category, is_optional, reducer.clone());
        reductions.push((assigned_var, reducer));
    }

    let group = match &typeql_reduce.within_group {
        None => Vec::new(),
        Some(group) => {
            group.iter().map(|typeql_var| {
                let var_name= typeql_var.name().unwrap();
                context.visible_variables.get(var_name).map_or_else(
                    || Err(PatternDefinitionError::OperatorStageVariableUnavailable {
                        variable_name: var_name.to_owned(),
                        declaration: typeql::query::pipeline::stage::Stage::Reduce(typeql_reduce.clone())
                    }),
                    |variable| Ok(variable.clone())
                )
            }).collect::<Result<Vec<_>,_>>()?
        }
    };
    Ok(Reduce::new(reductions, group))
}

fn resolve_assigned_variable_category_optionality(reduce: &Reducer<Variable>, variable_registry: &VariableRegistry) -> (VariableCategory, bool) {
    match reduce {
        Reducer::Count(_) => (VariableCategory::Value, false),
        Reducer::Sum(_) => (VariableCategory::Value, true),
    }
}


fn build_reduce_value(visible_variables: &HashMap<String, Variable>, reduce_value: &ReduceValue, reduce: &typeql::query::pipeline::stage::Reduce) -> Result<Reducer<Variable>, PatternDefinitionError> {
    match reduce_value {
        ReduceValue::Count(count) => {
            debug_assert!(count.variables.len() == 1); // TODO: The spec only allows 1?
            let Some(var) = visible_variables.get(count.variables.get(0).unwrap().name().unwrap()) else {
                return Err(todo!())
            };
            Ok(Reducer::Count(var.clone()))
        },
        ReduceValue::Stat(stat) => {
            let Some(var) = visible_variables.get(stat.variable.name().unwrap()) else {
                return Err(PatternDefinitionError::OperatorStageVariableUnavailable {
                    variable_name: stat.variable.name().unwrap().to_owned(),
                    declaration: typeql::query::pipeline::stage::Stage::Reduce(reduce.clone())
                })
            };
            match &stat.reduce_operator {
                ReduceOperator::Sum => Ok(Reducer::Sum(var.clone())),
                _ => todo!(),
            }
        }
    }
}

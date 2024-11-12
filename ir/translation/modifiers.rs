/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use typeql::{query::stage::Operator, token::Order};

use crate::{
    pipeline::modifier::{Limit, Offset, Require, Select, Sort},
    RepresentationError,
    translation::{literal::FromTypeQLLiteral, TranslationContext},
};

pub fn translate_select(
    context: &mut TranslationContext,
    typeql_select: &typeql::query::stage::modifier::Select,
) -> Result<Select, Box<RepresentationError>> {
    let selected_variables = typeql_select
        .variables
        .iter()
        .map(|typeql_var| match context.get_variable(typeql_var.name().unwrap()) {
            None => Err(RepresentationError::OperatorStageVariableUnavailable {
                variable_name: typeql_var.name().unwrap().to_owned(),
                declaration: typeql::query::pipeline::stage::Stage::Operator(Operator::Select(typeql_select.clone())),
            }),
            Some(variable) => Ok(variable),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let select = Select::new(selected_variables);
    context.visible_variables.retain(|name, var| select.variables.contains(var));
    Ok(select)
}

pub fn translate_sort(
    context: &mut TranslationContext,
    sort: &typeql::query::stage::modifier::Sort,
) -> Result<Sort, Box<RepresentationError>> {
    let sort_on = sort
        .ordered_variables
        .iter()
        .map(|ordered_var| {
            let is_ascending = ordered_var.ordering.map(|order| order == Order::Asc).unwrap_or(true);
            match context.get_variable(ordered_var.variable.name().unwrap()) {
                None => Err(RepresentationError::OperatorStageVariableUnavailable {
                    variable_name: ordered_var.variable.name().unwrap().to_owned(),
                    declaration: typeql::query::pipeline::stage::Stage::Operator(
                        typeql::query::pipeline::stage::Operator::Sort(sort.clone()),
                    ),
                }),
                Some(variable) => Ok((variable, is_ascending)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Sort::new(sort_on))
}

pub fn translate_offset(
    context: &mut TranslationContext,
    offset: &typeql::query::stage::modifier::Offset,
) -> Result<Offset, Box<RepresentationError>> {
    u64::from_typeql_literal(&offset.offset)
        .map(Offset::new)
        .map_err(|source| Box::new(RepresentationError::LiteralParseError { literal: offset.offset.value.clone(), source }))
}

pub fn translate_limit(
    context: &mut TranslationContext,
    limit: &typeql::query::stage::modifier::Limit,
) -> Result<Limit, Box<RepresentationError>> {
    u64::from_typeql_literal(&limit.limit)
        .map(Limit::new)
        .map_err(|source| Box::new(RepresentationError::LiteralParseError { literal: limit.limit.value.clone(), source }))
}

pub fn translate_require(
    context: &mut TranslationContext,
    typeql_require: &typeql::query::pipeline::stage::modifier::Require,
) -> Result<Require, Box<RepresentationError>> {
    let required_variables = typeql_require
        .variables
        .iter()
        .map(|typeql_var| match context.visible_variables.get(typeql_var.name().unwrap()) {
            None => Err(RepresentationError::OperatorStageVariableUnavailable {
                variable_name: typeql_var.name().unwrap().to_owned(),
                declaration: typeql::query::pipeline::stage::Stage::Operator(Operator::Require(typeql_require.clone())),
            }),
            Some(&variable) => Ok(variable),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let require = Require::new(required_variables);
    Ok(require)
}

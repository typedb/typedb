/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use typeql::{common::Spanned, token::Order};

use crate::{
    pipeline::modifier::{Distinct, Limit, Offset, Require, Select, Sort},
    translation::{literal::FromTypeQLLiteral, TranslationContext},
    RepresentationError,
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
                source_span: typeql_select.span(),
            }),
            Some(variable) => Ok(variable),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let select = Select::new(selected_variables, typeql_select.span());
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
                    source_span: sort.span(),
                }),
                Some(variable) => Ok((variable, is_ascending)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Sort::new(sort_on, sort.span()))
}

pub fn translate_offset(
    context: &mut TranslationContext,
    typeql_offset: &typeql::query::stage::modifier::Offset,
) -> Result<Offset, Box<RepresentationError>> {
    u64::from_typeql_literal(&typeql_offset.offset, typeql_offset.span())
        .map(|offset| Offset::new(offset, typeql_offset.span()))
        .map_err(|typedb_source| {
            Box::new(RepresentationError::LiteralParseError {
                literal: typeql_offset.offset.value.clone(),
                source_span: typeql_offset.span(),
                typedb_source,
            })
        })
}

pub fn translate_limit(
    context: &mut TranslationContext,
    typeql_limit: &typeql::query::stage::modifier::Limit,
) -> Result<Limit, Box<RepresentationError>> {
    u64::from_typeql_literal(&typeql_limit.limit, typeql_limit.span())
        .map(|limit| Limit::new(limit, typeql_limit.span()))
        .map_err(|typedb_source| {
            Box::new(RepresentationError::LiteralParseError {
                literal: typeql_limit.limit.value.clone(),
                source_span: typeql_limit.span(),
                typedb_source,
            })
        })
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
                source_span: typeql_require.span(),
            }),
            Some(&variable) => Ok(variable),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let require = Require::new(required_variables, typeql_require.span());
    Ok(require)
}

pub fn translate_distinct(
    context: &mut TranslationContext,
    typeql_distinct: &typeql::query::pipeline::stage::modifier::Distinct,
) -> Result<Distinct, Box<RepresentationError>> {
    Ok(Distinct)
}

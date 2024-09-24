/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use answer::variable::Variable;
use typeql::{query::stage::Modifier, token::Order};

use crate::{
    program::modifier::{Limit, Offset, Select, Sort},
    translation::{
        literal::{translate_literal, FromTypeQLLiteral},
        TranslationContext,
    },
    PatternDefinitionError,
};

pub fn translate_select(
    context: &mut TranslationContext,
    typeql_select: &typeql::query::stage::modifier::Select,
) -> Result<Select, PatternDefinitionError> {
    let selected_variables = typeql_select
        .variables
        .iter()
        .map(|typeql_var| match context.visible_variables.get(typeql_var.name().unwrap()) {
            None => Err(PatternDefinitionError::OperatorStageVariableUnavailable {
                variable_name: typeql_var.name().unwrap().to_owned(),
                declaration: typeql::query::pipeline::stage::Stage::Modifier(Modifier::Select(typeql_select.clone())),
            }),
            Some(v) => Ok(v.clone()),
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let select = Select::new(selected_variables);
    context.visible_variables.retain(|name, var| select.variables.contains(var));
    Ok(select)
}

pub fn translate_sort(
    context: &mut TranslationContext,
    sort: &typeql::query::stage::modifier::Sort,
) -> Result<Sort, PatternDefinitionError> {
    let sort_on = sort
        .ordered_variables
        .iter()
        .map(|ordered_var| {
            let is_ascending = ordered_var.ordering.map(|order| order == Order::Asc).unwrap_or(true);
            match context.visible_variables.get(ordered_var.variable.name().unwrap()) {
                None => Err(PatternDefinitionError::OperatorStageVariableUnavailable {
                    variable_name: ordered_var.variable.name().unwrap().to_owned(),
                    declaration: typeql::query::pipeline::stage::Stage::Modifier(
                        typeql::query::pipeline::stage::Modifier::Sort(sort.clone()),
                    ),
                }),
                Some(variable) => Ok((variable.clone(), is_ascending)),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Sort::new(sort_on))
}

pub fn translate_offset(
    context: &mut TranslationContext,
    offset: &typeql::query::stage::modifier::Offset,
) -> Result<Offset, PatternDefinitionError> {
    u64::from_typeql_literal(&offset.offset)
        .map(|offset| Offset::new(offset))
        .map_err(|source| PatternDefinitionError::LiteralParseError { literal: offset.offset.value.clone(), source })
}

pub fn translate_limit(
    context: &mut TranslationContext,
    limit: &typeql::query::stage::modifier::Limit,
) -> Result<Limit, PatternDefinitionError> {
    u64::from_typeql_literal(&limit.limit)
        .map(|limit| Limit::new(limit))
        .map_err(|source| PatternDefinitionError::LiteralParseError { literal: limit.limit.value.clone(), source })
}

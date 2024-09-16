/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::token::Order;

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
    let selected_variables = typeql_select.variables.iter().map(|typeql_var| typeql_var.name().unwrap()).collect();
    let select = Select::new(selected_variables, &context.visible_variables)
        .map_err(|source| PatternDefinitionError::ModifierDefinitionError { source })?;
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
            (
                ordered_var.variable.name().unwrap(),
                ordered_var.ordering.map(|order| order == Order::Asc).unwrap_or(true),
            )
        })
        .collect();
    Sort::new(sort_on, &context.visible_variables)
        .map_err(|source| PatternDefinitionError::ModifierDefinitionError { source })
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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value_type::ValueType;
use crate::pattern::constraint::Type;
use crate::pattern::pattern::Pattern;
use crate::pattern::variable::Variable;
use crate::PatternDefinitionError;
use crate::program::FunctionalBlock;
use crate::program::modifier::{Filter, Limit, Modifier, ModifierDefinitionError, Offset, Sort};

pub struct FunctionIR {
    arguments: Vec<Variable>,
    pattern: Pattern,
    modifiers: Vec<Modifier>,
}

impl FunctionIR {
    fn new<'a>(pattern: Pattern, arguments: impl Iterator<Item=&'a str>) -> Result<Self, PatternDefinitionError> {
        let mut argument_variables = Vec::new();
        let context = pattern.context();
        for arg in arguments {
            let var = context.get_variable(arg)
                .ok_or_else(|| PatternDefinitionError::FunctionArgumentUnused { argument_variable: arg.to_string() })?;
            argument_variables.push(var);
        }
        Ok(Self { arguments: argument_variables, pattern, modifiers: Vec::new() })
    }
}

impl FunctionalBlock for FunctionIR {
    fn add_limit(&mut self, limit: u64) {
        self.modifiers.push(Modifier::Limit(Limit::new(limit)));
    }

    fn add_offset(&mut self, offset: u64) {
        self.modifiers.push(Modifier::Offset(Offset::new(offset)))
    }

    fn add_sort(&mut self, sort_variables: Vec<(&str, bool)>) -> Result<(), ModifierDefinitionError> {
        let sort = Sort::new(sort_variables, &self.pattern.context())?;
        self.modifiers.push(Modifier::Sort(sort));
        Ok(())
    }

    fn add_filter(&mut self, variables: Vec<&str>) -> Result<(), ModifierDefinitionError> {
        let filter = Filter::new(variables, &self.pattern.context())?;
        self.modifiers.push(Modifier::Filter(filter));
        Ok(())
    }
}

pub enum FunctionValuePrototype {
    Thing(Type),
    ThingOptional(Type),
    Value(ValueType),
    ValueOptional(ValueType),
    ThingList(Type),
    ThingListOptional(Type),
    ValueList(ValueType),
    ValueListOptional(ValueType),
}
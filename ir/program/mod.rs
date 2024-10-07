/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, error::Error, fmt, sync::Arc};

use answer::variable::Variable;
use encoding::value::value::Value;
use error::typedb_error;
use itertools::Itertools;
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};
use typeql::schema::definable::function::{Function, FunctionBlock, ReturnReduction, ReturnSingle, ReturnStream};

use crate::{
    pattern::{
        constraint::Constraint,
        variable_category::{VariableCategory, VariableOptionality},
        ParameterID,
    },
    program::{function_signature::FunctionID, reduce::Reducer},
    RepresentationError,
};

pub mod block;
pub mod function;
pub mod function_signature;
pub mod modifier;
pub mod reduce;
pub mod fetch;

#[derive(Debug, Clone)]
pub enum FunctionReadError {
    FunctionNotFound { function_id: FunctionID },
    FunctionRetrieval { source: SnapshotGetError },
    FunctionsScan { source: Arc<SnapshotIteratorError> },
}

impl fmt::Display for FunctionReadError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FunctionRetrieval { source } => Some(source),
            Self::FunctionsScan { source } => Some(source),
            Self::FunctionNotFound { .. } => None,
        }
    }
}

typedb_error!(
    pub FunctionRepresentationError(component = "Function representation", prefix = "FNR") {
        FunctionArgumentUnused(
            1,
            "Function argument variable '{argument_variable}' is unused.\nSource:\n{declaration}",
            argument_variable: String,
            declaration: Function
        ),
        StreamReturnVariableUnavailable(
            2,
            "Function return variable '{return_variable}' is not available or defined.\nSource:\n{declaration}", // TODO: formatted
            return_variable: String,
            declaration: ReturnStream
        ),
        SingleReturnVariableUnavailable(
            3,
            "Function return variable '{return_variable}' is not available or defined.\nSource:\n{declaration}", // TODO: formatted
            return_variable: String,
            declaration: ReturnSingle
        ),
        BlockDefinition(
            4,
            "Function pattern contains an error.\nSource:\n{declaration}",
            declaration: FunctionBlock,
            ( typedb_source : Box<RepresentationError>)
        ),
        ReturnReduction(
            5,
            "Error building representation of the return reduction.\nSource:\n{declaration}",
            declaration: ReturnReduction,
            ( typedb_source : Box<RepresentationError>)
        ),
    }
);

#[derive(Debug, Clone)]
pub struct VariableRegistry {
    variable_names: HashMap<Variable, String>,
    variable_id_allocator: u16,
    variable_categories: HashMap<Variable, (VariableCategory, VariableCategorySource)>,
    variable_optionality: HashMap<Variable, VariableOptionality>,
}

impl VariableRegistry {
    pub(crate) fn new() -> VariableRegistry {
        Self {
            variable_names: HashMap::new(),
            variable_id_allocator: 0,
            variable_categories: HashMap::new(),
            variable_optionality: HashMap::new(),
        }
    }

    fn register_variable_named(&mut self, name: String) -> Variable {
        let variable = self.allocate_variable();
        self.variable_names.insert(variable, name);
        variable
    }

    fn register_anonymous_variable(&mut self) -> Variable {
        self.allocate_variable()
    }

    fn allocate_variable(&mut self) -> Variable {
        let variable = Variable::new(self.variable_id_allocator);
        self.variable_id_allocator += 1;
        variable
    }

    pub fn set_assigned_value_variable_category(
        &mut self,
        variable: Variable,
        category: VariableCategory,
        source: Constraint<Variable>,
    ) -> Result<(), RepresentationError> {
        self.set_variable_category(variable, category, VariableCategorySource::Constraint(source))
    }

    fn set_variable_category(
        &mut self,
        variable: Variable,
        category: VariableCategory,
        source: VariableCategorySource,
    ) -> Result<(), RepresentationError> {
        let existing_category = self.variable_categories.get_mut(&variable);
        match existing_category {
            None => {
                self.variable_categories.insert(variable, (category, source));
                Ok(())
            }
            Some((existing_category, existing_source)) => {
                let narrowest = existing_category.narrowest(category);
                match narrowest {
                    None => Err(RepresentationError::VariableCategoryMismatch {
                        variable_name: self
                            .variable_names
                            .get(&variable)
                            .cloned()
                            .unwrap_or_else(|| "$<INTERNAL>".to_owned()),
                        category_1: category,
                        // category_1_source: source,
                        category_2: *existing_category,
                        // category_2_source: existing_source.clone(),
                    }),
                    Some(narrowed) => {
                        if narrowed == *existing_category {
                            Ok(())
                        } else {
                            *existing_category = narrowed;
                            *existing_source = source;
                            Ok(())
                        }
                    }
                }
            }
        }
    }

    fn set_variable_is_optional(&mut self, variable: Variable, optional: bool) {
        match optional {
            true => self.variable_optionality.insert(variable, VariableOptionality::Optional),
            false => self.variable_optionality.remove(&variable),
        };
    }

    pub fn variable_categories(&self) -> impl Iterator<Item = (Variable, VariableCategory)> + '_ {
        self.variable_categories.iter().map(|(&variable, &(category, _))| (variable, category))
    }

    pub fn variable_names(&self) -> &HashMap<Variable, String> {
        &self.variable_names
    }

    pub fn get_variable_category(&self, variable: Variable) -> Option<VariableCategory> {
        self.variable_categories.get(&variable).map(|(category, _constraint)| *category)
    }

    pub fn get_variable_optionality(&self, variable: Variable) -> Option<VariableOptionality> {
        self.variable_optionality.get(&variable).cloned()
    }

    pub(crate) fn is_variable_optional(&self, variable: Variable) -> bool {
        match self.variable_optionality.get(&variable).unwrap_or(&VariableOptionality::Required) {
            VariableOptionality::Required => false,
            VariableOptionality::Optional => true,
        }
    }

    pub(crate) fn register_reduce_output_variable(
        &mut self,
        name: &str,
        category: VariableCategory,
        is_optional: bool,
        reducer: Reducer,
    ) -> Variable {
        let variable = self.register_variable_named(name.to_owned());
        self.set_variable_category(variable.clone(), category, VariableCategorySource::Reduce(reducer)).unwrap(); // We just created the variable. It cannot error
        self.set_variable_is_optional(variable.clone(), is_optional);
        variable
    }
}

impl fmt::Display for VariableRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Named variables:")?;
        for var in self.variable_names.keys().sorted_unstable() {
            writeln!(f, "  {}: ${}", var, self.variable_names[var])?;
        }
        writeln!(f, "Variable categories:")?;
        for var in self.variable_categories.keys().sorted_unstable() {
            writeln!(f, "  {}: {}", var, self.variable_categories[var].0)?;
        }
        writeln!(f, "Optional variables:")?;
        for var in self.variable_optionality.keys().sorted_unstable() {
            writeln!(f, "  {}", var)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum VariableCategorySource {
    Constraint(Constraint<Variable>),
    Reduce(Reducer),
}

#[derive(Clone, Debug, Default)]
pub struct ParameterRegistry {
    fetch_key_registry: HashMap<ParameterID, String>,
    value_registry: HashMap<ParameterID, Value<'static>>,
}

impl ParameterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register_value(&mut self, value: Value<'static>) -> ParameterID {
        let id = ParameterID { id: self.value_registry.len() };
        let _prev = self.value_registry.insert(id, value);
        debug_assert_eq!(_prev, None);
        id
    }

    pub(crate) fn register_fetch_key(&mut self, key: String) -> ParameterID {
        let id = ParameterID { id: self.fetch_key_registry.len() };
        let _prev = self.fetch_key_registry.insert(id, key);
        debug_assert_eq!(_prev, None);
        id
    }

    pub fn value(&self, id: ParameterID) -> Option<&Value<'static>> {
        self.value_registry.get(&id)
    }

    pub fn value_unchecked(&self, id: ParameterID) -> &Value<'static> {
        self.value_registry.get(&id).unwrap()
    }
}

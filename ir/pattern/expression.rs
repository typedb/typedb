/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
};

use answer::variable::Variable;
use encoding::value::value::Value;

use crate::{
    pattern::{
        constraint::{Constraint, ExpressionBinding},
        variable_category::VariableCategory,
    },
    program::block::BlockContext,
    PatternDefinitionError,
};
use crate::expressions::builtins::BuiltInFunctionID;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionTree {
    tree: Vec<Expression>,
}

impl ExpressionTree {
    pub(crate) fn new(expressions: Vec<Expression>) -> Self {
        Self { tree: expressions }
    }

    pub(crate) fn root(&self) -> usize {
        self.tree.len() - 1
    }

    pub fn tree(&self) -> &Vec<Expression> {
        &self.tree
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = Variable> + '_ {
        self.tree.iter().filter_map(|expr| match expr {
            Expression::Variable(variable) => Some(variable.clone()),
            Expression::ListIndex(list_index) => Some(list_index.list_variable.clone()),
            Expression::ListIndexRange(list_index_range) => Some(list_index_range.list_variable.clone()),

            Expression::Constant(_) | Expression::Operation(_) | Expression::BuiltInCall(_) | Expression::List(_) => {
                None
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expression {
    Constant(Value<'static>),
    Variable(Variable),
    Operation(Operation),
    BuiltInCall(BuiltInCall), // Other functions must be re-written as an anonymous assignment.
    ListIndex(ListIndex),

    List(ListConstructor),
    ListIndexRange(ListIndexRange),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Operation {
    pub(crate) operator: Operator,
    pub(crate) left_expression_index: usize,
    pub(crate) right_expression_index: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BuiltInCall {
    pub(crate) builtin_id: BuiltInFunctionID,
    pub(crate) args_index: Vec<usize>,
}

impl BuiltInCall {
    pub(crate) fn new(builtin_id: BuiltInFunctionID, args_index: Vec<usize>) -> Self {
        BuiltInCall { builtin_id, args_index }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndex {
    pub(crate) list_variable: Variable,
    pub(crate) index: usize,
}

impl ListIndex {
    pub(crate) fn new(list_variable: Variable, index: usize) -> ListIndex {
        Self { list_variable, index }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListConstructor {
    pub(crate) item_expression_indices: Vec<usize>,
}

impl ListConstructor {
    pub(crate) fn new(item_expression_indices: Vec<usize>) -> Self {
        Self { item_expression_indices }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndexRange {
    pub(crate) list_variable: Variable,
    pub(crate) from_expression_index: usize,
    pub(crate) to_expression_index: usize,
}

impl ListIndexRange {
    pub(crate) fn new(
        list_variable: Variable,
        from_expression_index: usize,
        to_expression_index: usize,
    ) -> ListIndexRange {
        Self { list_variable, from_expression_index, to_expression_index }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}

impl Operation {
    pub(crate) fn new(operator: Operator, left_expression_index: usize, right_expression_index: usize) -> Operation {
        Self { operator, left_expression_index, right_expression_index }
    }
}
impl ExpressionBinding<Variable> {

    // TODO: Split expressions into Value & List expressions
    pub(crate) fn validate(&self, context: &mut BlockContext) -> Result<(), ExpressionDefinitionError> {
        if self.expression().tree.is_empty() {
            Err(ExpressionDefinitionError::EmptyExpressionTree {})
        } else {
            self.validate_recursive(context, self.expression().root(), None)
        }
    }

    fn validate_recursive(
        &self,
        context: &mut BlockContext,
        index: usize,
        expects_list_opt: Option<bool>,
    ) -> Result<(), ExpressionDefinitionError> {
        if let Some(expr) = self.expression().tree.get(index) {
            match expects_list_opt {
                None => {}
                Some(true) => self.validate_is_list(context, expr)?,
                Some(false) => self.validate_is_value(context, expr)?,
            }
            match expr {
                Expression::Operation(operation) => {
                    self.validate_recursive(context, operation.left_expression_index, Some(false))?;
                    self.validate_recursive(context, operation.left_expression_index, Some(false))?;
                }
                Expression::BuiltInCall(built_in) => {
                    built_in.args_index.iter().map(|idx| {
                        self.validate_recursive(context, *idx, Some(false))
                    }).collect::<Result<Vec<_>, _>>()?;
                }
                Expression::List(list_constructor) => {
                    todo!("Verify each term is a value")
                }
                Expression::ListIndexRange(_)
                | Expression::ListIndex(_)
                | Expression::Constant(_)
                | Expression::Variable(_) => {}
            }
            Ok(())
        } else {
            Err(ExpressionDefinitionError::SubExpressionNotDefined)
        }
    }

    fn validate_is_list(&self, context: &mut BlockContext, expr: &Expression) -> Result<(), ExpressionDefinitionError> {
        if let Expression::Variable(variable) = expr {
            context
                .set_variable_category(
                    variable.clone(),
                    VariableCategory::ValueList,
                    Constraint::ExpressionBinding(self.clone()),
                )
                .map_err(|source| ExpressionDefinitionError::PatternDefinition { source: Box::new(source) })
        } else if self.expr_is_list(context, expr) {
            Ok(())
        } else {
            Err(ExpressionDefinitionError::ExpectedListArgumentReceivedValue)
        }
    }

    fn validate_is_value(
        &self,
        context: &mut BlockContext,
        expr: &Expression,
    ) -> Result<(), ExpressionDefinitionError> {
        if let Expression::Variable(variable) = expr {
            context
                .set_variable_category(
                    variable.clone(),
                    VariableCategory::Value,
                    Constraint::ExpressionBinding(self.clone()),
                )
                .map_err(|source| ExpressionDefinitionError::PatternDefinition { source: Box::new(source) })
        } else if self.expr_is_list(context, expr) {
            Err(ExpressionDefinitionError::ExpectedListArgumentReceivedValue)
        } else {
            Ok(())
        }
    }

    fn expr_is_list(&self, context: &mut BlockContext, expr: &Expression) -> bool {
        match expr {
            Expression::Constant(_) | Expression::Operation(_) | Expression::ListIndex(_) => false,
            Expression::List(_) | Expression::ListIndexRange(_) => true,
            Expression::BuiltInCall(built_in_call) => todo!(),
            Expression::Variable(var) => unreachable!(),
        }
    }
}

// Display traits
impl Hash for Expression {
    fn hash<H: Hasher>(&self, state: &mut H) {
        todo!()
    }
}

impl Display for ExpressionTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug)]
pub enum ExpressionDefinitionError {
    ExpectedValueArgumentReceivedList,
    ExpectedListArgumentReceivedValue,
    ArgumentNotBound,
    SubExpressionNotDefined,
    PatternDefinition { source: Box<PatternDefinitionError> },
    EmptyExpressionTree {},
}

impl Display for ExpressionDefinitionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for ExpressionDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedValueArgumentReceivedList => None,
            Self::ExpectedListArgumentReceivedValue => None,
            Self::ArgumentNotBound => None,
            Self::SubExpressionNotDefined => None,
            Self::PatternDefinition { source } => Some(source),
            Self::EmptyExpressionTree { .. } => None,
        }
    }
}

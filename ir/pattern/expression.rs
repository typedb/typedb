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
    expressions::builtins::BuiltInFunctionID,
    pattern::{
        constraint::{Constraint, ExpressionBinding},
        variable_category::VariableCategory,
    },
    program::block::BlockContext,
    PatternDefinitionError,
};
use crate::pattern::IrID;

enum ExpectedArgumentType {
    Single,
    List,
    Either,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionTree<ID: IrID> {
    tree: Vec<Expression<ID>>,
}

impl ExpressionTree<Variable> {
    pub(crate) fn new(expressions: Vec<Expression<Variable>>) -> Self {
        Self { tree: expressions }
    }

    pub(crate) fn root(&self) -> usize {
        self.tree.len() - 1
    }

    pub fn tree(&self) -> &Vec<Expression<Variable>> {
        &self.tree
    }

    pub fn ids(&self) -> impl Iterator<Item = Variable> + '_ {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression<ID: IrID> {
    Constant(Value<'static>),
    Variable(ID),
    Operation(Operation),
    BuiltInCall(BuiltInCall), // Other functions must be re-written as an anonymous assignment.
    ListIndex(ListIndex<ID>),

    List(ListConstructor),
    ListIndexRange(ListIndexRange<ID>),
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
pub struct ListIndex<ID: IrID> {
    pub(crate) list_variable: ID,
    pub(crate) index_expression_index: usize,
}

impl ListIndex<Variable> {
    pub(crate) fn new(list_variable: Variable, index: usize) -> ListIndex<Variable> {
        Self { list_variable, index_expression_index: index }
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
pub struct ListIndexRange<ID: IrID> {
    pub(crate) list_variable: ID,
    pub(crate) from_expression_index: usize,
    pub(crate) to_expression_index: usize,
}

impl<ID: IrID> ListIndexRange<ID> {
    pub(crate) fn new(
        list_variable: ID,
        from_expression_index: usize,
        to_expression_index: usize,
    ) -> Self {
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
    pub(crate) fn validate(&self, context: &mut BlockContext) -> Result<(), ExpressionDefinitionError> {
        if self.expression().tree.is_empty() {
            Err(ExpressionDefinitionError::EmptyExpressionTree {})
        } else {
            self.validate_recursive(context, self.expression().root(), ExpectedArgumentType::Either)
        }
    }

    fn validate_recursive(
        &self,
        context: &mut BlockContext,
        index: usize,
        expects_list_opt: ExpectedArgumentType,
    ) -> Result<(), ExpressionDefinitionError> {
        if let Some(expr) = self.expression().tree.get(index) {
            match expects_list_opt {
                ExpectedArgumentType::Either => {}
                ExpectedArgumentType::List => self.validate_is_list(context, expr)?,
                ExpectedArgumentType::Single => self.validate_is_value(context, expr)?,
            }
            // recurse
            match expr {
                Expression::Operation(operation) => {
                    self.validate_recursive(context, operation.left_expression_index, ExpectedArgumentType::Either)?;
                    self.validate_recursive(context, operation.left_expression_index, ExpectedArgumentType::Either)?;
                }
                Expression::BuiltInCall(built_in) => {
                    // TODO: We can get the categories from the builtin expected arguments?
                    built_in
                        .args_index
                        .iter()
                        .map(|idx| self.validate_recursive(context, *idx, ExpectedArgumentType::Either))
                        .collect::<Result<Vec<_>, _>>()?;
                }
                Expression::List(list_constructor) => {
                    list_constructor
                        .item_expression_indices
                        .iter()
                        .map(|index| self.validate_recursive(context, *index, ExpectedArgumentType::Single))
                        .collect::<Result<Vec<_>, _>>()?;
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

    fn validate_is_list(&self, context: &mut BlockContext, expr: &Expression<Variable>) -> Result<(), ExpressionDefinitionError> {
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
        expr: &Expression<Variable>,
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

    fn expr_is_list(&self, context: &mut BlockContext, expr: &Expression<Variable>) -> bool {
        match expr {
            Expression::Constant(_) | Expression::Operation(_) | Expression::ListIndex(_) => false,
            Expression::List(_) | Expression::ListIndexRange(_) => true,
            Expression::BuiltInCall(built_in_call) => todo!(),
            Expression::Variable(var) => unreachable!(),
        }
    }
}

// Display traits
impl<ID: IrID> Display for ExpressionTree<ID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<ID: IrID> Display for Expression<ID> {
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

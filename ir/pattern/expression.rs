/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Display, Formatter},
    hash::Hash,
};

use answer::variable::Variable;
use encoding::value::value::Value;

use crate::{
    pattern::{constraint::ExpressionBinding, IrID},
    program::block::BlockContext,
    PatternDefinitionError,
};

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

    pub fn root(&self) -> usize {
        self.tree.len() - 1
    }

    pub fn tree(&self) -> &Vec<Expression<Variable>> {
        &self.tree
    }

    pub fn ids(&self) -> impl Iterator<Item = Variable> + '_ {
        self.tree.iter().filter_map(|expr| match expr {
            Expression::Variable(variable) => Some(variable.clone()),
            Expression::ListIndex(list_index) => Some(list_index.list_variable()),
            Expression::ListIndexRange(list_index_range) => Some(list_index_range.list_variable()),

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
    operator: Operator,
    left_expression_index: usize,
    right_expression_index: usize,
}

impl Operation {
    pub fn operator(&self) -> Operator {
        self.operator
    }

    pub fn left_expression_index(&self) -> usize {
        self.left_expression_index
    }

    pub fn right_expression_index(&self) -> usize {
        self.right_expression_index
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BuiltInCall {
    builtin_id: BuiltInFunctionID,
    args_index: Vec<usize>,
}

impl BuiltInCall {
    pub(crate) fn new(builtin_id: BuiltInFunctionID, args_index: Vec<usize>) -> Self {
        BuiltInCall { builtin_id, args_index }
    }

    pub fn builtin_id(&self) -> BuiltInFunctionID {
        self.builtin_id
    }

    pub fn args_index(&self) -> &Vec<usize> {
        &self.args_index
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum BuiltInFunctionID {
    Abs,
    Ceil,
    Floor,
    Round,
    // TODO: The below
    // Max,
    // Min,
    // Length
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndex<ID: IrID> {
    list_variable: ID,
    index_expression_index: usize,
}

impl ListIndex<Variable> {
    pub(crate) fn new(list_variable: Variable, index: usize) -> ListIndex<Variable> {
        Self { list_variable, index_expression_index: index }
    }

    pub fn list_variable(&self) -> Variable {
        self.list_variable
    }

    pub fn index_expression_index(&self) -> usize {
        self.index_expression_index
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListConstructor {
    item_expression_indices: Vec<usize>,
}

impl ListConstructor {
    pub(crate) fn new(item_expression_indices: Vec<usize>) -> Self {
        Self { item_expression_indices }
    }

    pub fn item_expression_indices(&self) -> &Vec<usize> {
        &self.item_expression_indices
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndexRange<ID: IrID> {
    list_variable: ID,
    from_expression_index: usize,
    to_expression_index: usize,
}

impl<ID: IrID> ListIndexRange<ID> {
    pub(crate) fn new(list_variable: ID, from_expression_index: usize, to_expression_index: usize) -> Self {
        Self { list_variable, from_expression_index, to_expression_index }
    }

    pub fn list_variable(&self) -> ID {
        self.list_variable
    }

    pub fn from_expression_index(&self) -> usize {
        self.from_expression_index
    }

    pub fn to_expression_index(&self) -> usize {
        self.to_expression_index
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
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
            Ok(())
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

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

use crate::{
    pattern::{IrID, ParameterID},
    PatternDefinitionError,
};

enum ExpectedArgumentType {
    Single,
    List,
    Either,
}

pub type ExpressionTreeNodeId = usize;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionTree<ID> {
    preorder_tree: Vec<Expression<ID>>,
}

impl ExpressionTree<Variable> {
    pub(crate) fn empty() -> Self {
        Self { preorder_tree: Vec::new() }
    }
}

impl<ID: IrID> ExpressionTree<ID> {
    pub fn is_empty(&self) -> bool {
        self.preorder_tree.is_empty()
    }

    pub fn is_constant(&self) -> bool {
        matches!(&*self.preorder_tree, [Expression::Constant(_)])
    }

    pub fn expression_tree_preorder(&self) -> impl Iterator<Item = &Expression<ID>> {
        self.preorder_tree.iter()
    }

    pub fn get_root(&self) -> &Expression<ID> {
        self.preorder_tree.last().unwrap()
    }

    pub fn get(&self, expression_id: ExpressionTreeNodeId) -> &Expression<ID> {
        &self.preorder_tree[expression_id]
    }

    pub(crate) fn add(&mut self, expression: Expression<ID>) -> ExpressionTreeNodeId {
        self.preorder_tree.push(expression);
        self.preorder_tree.len() - 1
    }

    pub fn variables(&self) -> impl Iterator<Item = ID> + '_ {
        self.preorder_tree.iter().filter_map(|expr| match expr {
            &Expression::Variable(variable) => Some(variable),
            Expression::ListIndex(list_index) => Some(list_index.list_variable()),
            Expression::ListIndexRange(list_index_range) => Some(list_index_range.list_variable()),
            Expression::Constant(_) | Expression::Operation(_) | Expression::BuiltInCall(_) | Expression::List(_) => {
                None
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression<ID> {
    Constant(ParameterID),
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
    left_expression_id: ExpressionTreeNodeId,
    right_expression_id: ExpressionTreeNodeId,
}

impl Operation {
    pub(crate) fn new(
        operator: Operator,
        left_expression_id: ExpressionTreeNodeId,
        right_expression_id: ExpressionTreeNodeId,
    ) -> Operation {
        Self { operator, left_expression_id, right_expression_id }
    }

    pub fn operator(&self) -> Operator {
        self.operator
    }

    pub fn left_expression_id(&self) -> ExpressionTreeNodeId {
        self.left_expression_id
    }

    pub fn right_expression_id(&self) -> ExpressionTreeNodeId {
        self.right_expression_id
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BuiltInCall {
    builtin_id: BuiltInFunctionID,
    argument_expression_ids: Vec<ExpressionTreeNodeId>,
}

impl BuiltInCall {
    pub(crate) fn new(builtin_id: BuiltInFunctionID, argument_expression_ids: Vec<ExpressionTreeNodeId>) -> Self {
        BuiltInCall { builtin_id, argument_expression_ids }
    }

    pub fn builtin_id(&self) -> BuiltInFunctionID {
        self.builtin_id
    }

    pub fn argument_expression_ids(&self) -> &[ExpressionTreeNodeId] {
        &self.argument_expression_ids
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
pub struct ListIndex<ID> {
    list_variable: ID,
    index_expression_id: ExpressionTreeNodeId,
}

impl<ID: IrID> ListIndex<ID> {
    pub(crate) fn new(list_variable: ID, index_expression_id: ExpressionTreeNodeId) -> ListIndex<ID> {
        Self { list_variable, index_expression_id }
    }

    pub fn list_variable(&self) -> ID {
        self.list_variable
    }

    pub fn index_expression_id(&self) -> ExpressionTreeNodeId {
        self.index_expression_id
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListConstructor {
    item_expression_ids: Vec<ExpressionTreeNodeId>,
}

impl ListConstructor {
    pub(crate) fn new(item_expression_ids: Vec<ExpressionTreeNodeId>) -> Self {
        Self { item_expression_ids }
    }

    pub fn item_expression_ids(&self) -> &[ExpressionTreeNodeId] {
        &self.item_expression_ids
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndexRange<ID> {
    list_variable: ID,
    from_expression_id: ExpressionTreeNodeId,
    to_expression_id: ExpressionTreeNodeId,
}

impl<ID: IrID> ListIndexRange<ID> {
    pub(crate) fn new(
        list_variable: ID,
        from_expression_id: ExpressionTreeNodeId,
        to_expression_id: ExpressionTreeNodeId,
    ) -> Self {
        Self { list_variable, from_expression_id, to_expression_id }
    }

    pub fn list_variable(&self) -> ID {
        self.list_variable
    }

    pub fn from_expression_id(&self) -> ExpressionTreeNodeId {
        self.from_expression_id
    }

    pub fn to_expression_id(&self) -> ExpressionTreeNodeId {
        self.to_expression_id
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

#[derive(Debug, Clone)]
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
            Self::PatternDefinition { source } => todo!(),
            Self::EmptyExpressionTree { .. } => None,
        }
    }
}

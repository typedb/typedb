/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    mem,
};
use std::fmt::{Display, Formatter};

use answer::variable::Variable;
use structural_equality::StructuralEquality;

use crate::{
    pattern::{IrID, ParameterID},
    RepresentationError,
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

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> ExpressionTree<T> {
        let preorder_tree = self
            .preorder_tree
            .iter()
            .map(|node| match node {
                Expression::Variable(var) => Expression::Variable(var.map(mapping)),
                Expression::ListIndex(list_index) => Expression::ListIndex(list_index.map(mapping)),
                Expression::ListIndexRange(list_index_range) => {
                    Expression::ListIndexRange(list_index_range.map(mapping))
                }
                Expression::Constant(inner) => Expression::Constant(inner.clone()),
                Expression::Operation(inner) => Expression::Operation(inner.clone()),
                Expression::BuiltInCall(inner) => Expression::BuiltInCall(inner.clone()),
                Expression::List(inner) => Expression::List(inner.clone()),
            })
            .collect::<Vec<Expression<T>>>();
        ExpressionTree { preorder_tree }
    }
}

impl<ID: StructuralEquality> StructuralEquality for ExpressionTree<ID> {
    fn hash(&self) -> u64 {
        self.preorder_tree.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.preorder_tree.equals(&other.preorder_tree)
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

impl<ID: StructuralEquality> StructuralEquality for Expression<ID> {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
            ^ match self {
                Expression::Constant(inner) => StructuralEquality::hash(inner),
                Expression::Variable(inner) => StructuralEquality::hash(inner),
                Expression::Operation(inner) => StructuralEquality::hash(inner),
                Expression::BuiltInCall(inner) => StructuralEquality::hash(inner),
                Expression::ListIndex(inner) => StructuralEquality::hash(inner),
                Expression::List(inner) => StructuralEquality::hash(inner),
                Expression::ListIndexRange(inner) => StructuralEquality::hash(inner),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Constant(inner), Self::Constant(other_inner)) => inner.equals(other_inner),
            (Self::Variable(inner), Self::Variable(other_inner)) => inner.equals(other_inner),
            (Self::Operation(inner), Self::Operation(other_inner)) => inner.equals(other_inner),
            (Self::BuiltInCall(inner), Self::BuiltInCall(other_inner)) => inner.equals(other_inner),
            (Self::ListIndex(inner), Self::ListIndex(other_inner)) => inner.equals(other_inner),
            (Self::List(inner), Self::List(other_inner)) => inner.equals(other_inner),
            (Self::ListIndexRange(inner), Self::ListIndexRange(other_inner)) => inner.equals(other_inner),
            // this structure forces us to update the match block when the variants change!
            (Self::Constant(_), _)
            | (Self::Variable(_), _)
            | (Self::Operation(_), _)
            | (Self::BuiltInCall(_), _)
            | (Self::ListIndex(_), _)
            | (Self::List(_), _)
            | (Self::ListIndexRange(_), _) => false,
        }
    }
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

impl StructuralEquality for Operation {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.operator));
        hasher.write_u64(StructuralEquality::hash(&self.right_expression_id));
        hasher.write_u64(StructuralEquality::hash(&self.left_expression_id));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.operator.equals(&other.operator)
            && self.right_expression_id.equals(&other.right_expression_id)
            && self.left_expression_id.equals(&other.left_expression_id)
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

impl StructuralEquality for BuiltInCall {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.builtin_id));
        hasher.write_u64(StructuralEquality::hash(self.argument_expression_ids()));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.builtin_id.equals(&other.builtin_id)
            && self.argument_expression_ids().equals(other.argument_expression_ids())
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

impl StructuralEquality for BuiltInFunctionID {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl Display for BuiltInFunctionID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BuiltInFunctionID::Abs => fmt::Display::fmt(&typeql::token::Function::Abs, f),
            BuiltInFunctionID::Ceil => fmt::Display::fmt(&typeql::token::Function::Ceil, f),
            BuiltInFunctionID::Floor => fmt::Display::fmt(&typeql::token::Function::Floor, f),
            BuiltInFunctionID::Round => fmt::Display::fmt(&typeql::token::Function::Round, f),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndex<ID> {
    list_variable: ID,
    index_expression_id: ExpressionTreeNodeId,
}

impl<ID> ListIndex<ID> {
    pub(crate) fn new(list_variable: ID, index_expression_id: ExpressionTreeNodeId) -> ListIndex<ID> {
        Self { list_variable, index_expression_id }
    }
}

impl<ID: IrID> ListIndex<ID> {
    pub fn list_variable(&self) -> ID {
        self.list_variable
    }

    pub fn index_expression_id(&self) -> ExpressionTreeNodeId {
        self.index_expression_id
    }

    fn map<T: Clone>(&self, mapping: &HashMap<ID, T>) -> ListIndex<T> {
        ListIndex::new(self.list_variable.map(mapping), self.index_expression_id.clone())
    }
}

impl<ID: StructuralEquality> StructuralEquality for ListIndex<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.list_variable));
        hasher.write_u64(StructuralEquality::hash(&self.index_expression_id));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.list_variable.equals(&other.list_variable) && self.index_expression_id.equals(&other.index_expression_id)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListConstructor {
    item_expression_ids: Vec<ExpressionTreeNodeId>,
    len_id: ParameterID,
}

impl ListConstructor {
    pub fn new(item_expression_ids: Vec<ExpressionTreeNodeId>, len_id: ParameterID) -> Self {
        Self { item_expression_ids, len_id }
    }

    pub fn item_expression_ids(&self) -> &[ExpressionTreeNodeId] {
        &self.item_expression_ids
    }

    pub fn len_id(&self) -> ParameterID {
        self.len_id
    }
}

impl StructuralEquality for ListConstructor {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.item_expression_ids));
        hasher.write_u64(StructuralEquality::hash(&self.len_id));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.item_expression_ids.equals(&other.item_expression_ids) && self.len_id.equals(&other.len_id)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListIndexRange<ID> {
    list_variable: ID,
    from_expression_id: ExpressionTreeNodeId,
    to_expression_id: ExpressionTreeNodeId,
}

impl<ID> ListIndexRange<ID> {
    pub(crate) fn new(
        list_variable: ID,
        from_expression_id: ExpressionTreeNodeId,
        to_expression_id: ExpressionTreeNodeId,
    ) -> Self {
        Self { list_variable, from_expression_id, to_expression_id }
    }
}

impl<ID: IrID> ListIndexRange<ID> {
    pub fn list_variable(&self) -> ID {
        self.list_variable
    }

    pub fn from_expression_id(&self) -> ExpressionTreeNodeId {
        self.from_expression_id
    }

    pub fn to_expression_id(&self) -> ExpressionTreeNodeId {
        self.to_expression_id
    }

    fn map<T: Clone>(&self, mapping: &HashMap<ID, T>) -> ListIndexRange<T> {
        ListIndexRange::new(
            self.list_variable.map(mapping),
            self.from_expression_id.clone(),
            self.to_expression_id.clone(),
        )
    }
}

impl<ID: StructuralEquality> StructuralEquality for ListIndexRange<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.list_variable));
        hasher.write_u64(StructuralEquality::hash(&self.from_expression_id));
        hasher.write_u64(StructuralEquality::hash(&self.to_expression_id));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.list_variable.equals(&other.list_variable)
            && self.from_expression_id.equals(&other.from_expression_id)
            && self.to_expression_id.equals(&other.to_expression_id)
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

impl StructuralEquality for Operator {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Add => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Add, f),
            Operator::Subtract =>  fmt::Display::fmt(&typeql::token::ArithmeticOperator::Subtract, f),
            Operator::Multiply =>  fmt::Display::fmt(&typeql::token::ArithmeticOperator::Multiply, f),
            Operator::Divide =>  fmt::Display::fmt(&typeql::token::ArithmeticOperator::Divide, f),
            Operator::Modulo =>  fmt::Display::fmt(&typeql::token::ArithmeticOperator::Modulo, f),
            Operator::Power =>  fmt::Display::fmt(&typeql::token::ArithmeticOperator::Power, f),
        }
    }
}

// Display traits
impl<ID: IrID> fmt::Display for ExpressionTree<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl<ID: IrID> fmt::Display for Expression<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

#[derive(Debug, Clone)]
pub enum ExpressionDefinitionError {
    ExpectedValueArgumentReceivedList,
    ExpectedListArgumentReceivedValue,
    ArgumentNotBound,
    SubExpressionNotDefined,
    PatternDefinition { source: Box<RepresentationError> },
    EmptyExpressionTree {},
}

impl fmt::Display for ExpressionDefinitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

impl Error for ExpressionDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedValueArgumentReceivedList => None,
            Self::ExpectedListArgumentReceivedValue => None,
            Self::ArgumentNotBound => None,
            Self::SubExpressionNotDefined => None,
            Self::PatternDefinition { source: _ } => None, // TODO: Handle when migrating to TypeDBError. There is actually a source here.
            Self::EmptyExpressionTree { .. } => None,
        }
    }
}

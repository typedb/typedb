/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    fmt::Formatter,
    hash::{DefaultHasher, Hash, Hasher},
    mem,
};

use answer::variable::Variable;
use error::typedb_error;
use primitive::format_joined::FormatJoined;
use structural_equality::StructuralEquality;
use typeql::{common::Span, expression::NamespacedFunctionName};

use crate::{
    RepresentationError,
    pattern::{
        IrID, ParameterID, Pattern, ReferenceOptionality,
        conjunction::Conjunction,
        variable_category::{VariableCategory, VariableOptionality},
    },
    pipeline::function_signature::{FunctionID, FunctionSignature},
};

pub type ExpressionTreeNodeId = usize;

typedb_error! {
    pub ExpressionRepresentationError(component = "Expression representation", prefix = "ERP") {
        EmptyExpressionTree(1, "Illegal empty expression."),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionTree<ID> {
    preorder_tree: Vec<Expression<ID>>,
}

impl ExpressionTree<Variable> {
    pub(crate) fn empty() -> Self {
        Self { preorder_tree: Vec::new() }
    }

    pub fn return_optionality(&self) -> VariableOptionality {
        // Note, this is different from self.get_root().actual_result_optionality(&conjunction)
        let root_return_optionality = match self.get_root() {
            Expression::MayShortCircuit(_) => VariableOptionality::Optional,
            Expression::BuiltinValueFunctionCall(builtin) => builtin.function_id.optionality(),
            // At the root, a variable is also required and must be short-circuited
            Expression::Variable(_) => VariableOptionality::Required,
            Expression::ListIndex(_) | Expression::ListIndexRange(_) => VariableOptionality::Required,
            Expression::Constant(_) | Expression::Operation(_) | Expression::List(_) => VariableOptionality::Required,
        };
        let lazy_contains_short_circuit = || self.expression_tree_preorder().any(|expr| expr.may_short_circuit());

        if root_return_optionality == VariableOptionality::Optional || lazy_contains_short_circuit() {
            VariableOptionality::Optional
        } else {
            VariableOptionality::Required
        }
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

    pub fn root_node_id(&self) -> ExpressionTreeNodeId {
        self.preorder_tree.len() - 1
    }

    pub fn get_root(&self) -> &Expression<ID> {
        self.get(self.root_node_id())
    }

    pub fn get(&self, expression_id: ExpressionTreeNodeId) -> &Expression<ID> {
        &self.preorder_tree[expression_id]
    }

    pub(crate) fn add(&mut self, expression: Expression<ID>) -> ExpressionTreeNodeId {
        self.preorder_tree.push(expression);
        self.preorder_tree.len() - 1
    }

    pub fn argument_ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.preorder_tree.iter().filter_map(|expr| match expr {
            Expression::Variable(variable) => Some(**variable),
            Expression::ListIndex(list_index) => Some(**list_index.list_variable()),
            Expression::ListIndexRange(list_index_range) => Some(**list_index_range.list_variable()),
            Expression::Constant(_)
            | Expression::Operation(_)
            | Expression::BuiltinValueFunctionCall(_)
            | Expression::List(_)
            | Expression::MayShortCircuit(_) => None,
        })
    }

    pub fn parameter_ids(&self) -> impl Iterator<Item = ParameterID> + '_ {
        self.preorder_tree.iter().filter_map(|expr| match expr {
            Expression::Constant(parameter_id) => Some(parameter_id.clone()),
            Expression::Variable(_)
            | Expression::ListIndex(_)
            | Expression::ListIndexRange(_)
            | Expression::Operation(_)
            | Expression::BuiltinValueFunctionCall(_)
            | Expression::List(_)
            | Expression::MayShortCircuit(_) => None,
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
                Expression::BuiltinValueFunctionCall(inner) => Expression::BuiltinValueFunctionCall(inner.clone()),
                Expression::List(inner) => Expression::List(inner.clone()),
                Expression::MayShortCircuit(inner) => Expression::MayShortCircuit(*inner),
            })
            .collect::<Vec<Expression<T>>>();
        ExpressionTree { preorder_tree }
    }

    pub(crate) fn reference_optionalities(&self) -> impl IntoIterator<Item = (ID, ReferenceOptionality)> {
        let mut acc = HashMap::new();
        // The root is always required.
        collect_reference_optionalities(self, self.root_node_id(), ReferenceOptionality::Required, &mut acc);
        acc.into_iter()
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
    Variable(ExpressionVariable<ID>), // User-defined functions are re-written as an anonymous assignment.
    MayShortCircuit(ExpressionTreeNodeId), // For non-variables

    Operation(Operation),
    BuiltinValueFunctionCall(BuiltinValueFunctionCall),
    ListIndex(ListIndex<ID>),

    List(ListConstructor),
    ListIndexRange(ListIndexRange<ID>),
}

impl Expression<Variable> {
    pub(crate) fn actual_result_optionality(&self, conjunction: &Conjunction) -> VariableOptionality {
        fn of_var(conjunction: &Conjunction, variable: &ExpressionVariable<Variable>) -> VariableOptionality {
            // The result of checking a variable with '?' variable is always `Required`.
            match (variable.checked_isset, conjunction.optionality(&variable.variable)) {
                (true, _) | (_, VariableOptionality::Required) => VariableOptionality::Required,
                (false, VariableOptionality::Optional) => VariableOptionality::Optional,
            }
        }
        match self {
            Expression::Variable(variable) => of_var(conjunction, variable),
            Expression::ListIndex(inner) => of_var(conjunction, &inner.list_variable),
            Expression::ListIndexRange(inner) => of_var(conjunction, &inner.list_variable),
            Expression::BuiltinValueFunctionCall(builtin) => {
                error::needs_update_when_feature_is_implemented!(error::UnimplementedFeature::OptionalFunctions);
                VariableOptionality::Required
            }
            | Expression::Constant(_)
            | Expression::List(_)
            | Expression::Operation(_)
            | Expression::MayShortCircuit(_) => VariableOptionality::Required,
        }
    }

    fn may_short_circuit(&self) -> bool {
        match self {
            Expression::MayShortCircuit(_) => true,
            Expression::Variable(variable)
            | Expression::ListIndex(ListIndex { list_variable: variable, .. })
            | Expression::ListIndexRange(ListIndexRange { list_variable: variable, .. }) => variable.checked_isset,
            Expression::Constant(_)
            | Expression::Operation(_)
            | Expression::BuiltinValueFunctionCall(_)
            | Expression::List(_) => false,
        }
    }

    pub fn source_span(&self) -> Option<Span> {
        match self {
            Expression::Constant(inner) => Some(inner.source_span()),
            Expression::Variable(_) => None,
            Expression::MayShortCircuit(_) => None,
            Expression::Operation(inner) => inner.source_span(),
            Expression::BuiltinValueFunctionCall(inner) => inner.source_span(),
            Expression::ListIndex(inner) => inner.source_span(),
            Expression::List(inner) => inner.source_span(),
            Expression::ListIndexRange(inner) => inner.source_span(),
        }
    }
}

impl<ID: StructuralEquality> StructuralEquality for Expression<ID> {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
            ^ match self {
                Expression::Constant(inner) => StructuralEquality::hash(inner),
                Expression::Variable(inner) => StructuralEquality::hash(inner),
                Expression::Operation(inner) => StructuralEquality::hash(inner),
                Expression::BuiltinValueFunctionCall(inner) => StructuralEquality::hash(inner),
                Expression::ListIndex(inner) => StructuralEquality::hash(inner),
                Expression::List(inner) => StructuralEquality::hash(inner),
                Expression::ListIndexRange(inner) => StructuralEquality::hash(inner),
                Expression::MayShortCircuit(inner) => StructuralEquality::hash(inner),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Constant(inner), Self::Constant(other_inner)) => inner.equals(other_inner),
            (Self::Variable(inner), Self::Variable(other_inner)) => inner.equals(other_inner),
            (Self::Operation(inner), Self::Operation(other_inner)) => inner.equals(other_inner),
            (Self::BuiltinValueFunctionCall(inner), Self::BuiltinValueFunctionCall(other_inner)) => {
                inner.equals(other_inner)
            }
            (Self::ListIndex(inner), Self::ListIndex(other_inner)) => inner.equals(other_inner),
            (Self::List(inner), Self::List(other_inner)) => inner.equals(other_inner),
            (Self::ListIndexRange(inner), Self::ListIndexRange(other_inner)) => inner.equals(other_inner),
            (Self::MayShortCircuit(inner), Self::MayShortCircuit(other_inner)) => inner.equals(other_inner),
            // this structure forces us to update the match block when the variants change!
            (Self::Constant(_), _) | (Self::Variable(_), _) => false,
            | (Self::Operation(_), _)
            | (Self::BuiltinValueFunctionCall(_), _)
            | (Self::ListIndex(_), _)
            | (Self::List(_), _)
            | (Self::ListIndexRange(_), _)
            | (Self::MayShortCircuit(_), _) => false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionVariable<ID> {
    variable: ID,
    checked_isset: bool,
}

impl<ID> ExpressionVariable<ID> {
    pub fn new(variable: ID, checked_isset: bool) -> Self {
        Self { variable, checked_isset }
    }

    pub(crate) fn new_unchecked(variable: ID) -> Self {
        Self::new(variable, false)
    }
}

impl<ID: IrID> ExpressionVariable<ID> {
    pub fn variable(&self) -> ID {
        self.variable
    }

    pub fn checked_isset(&self) -> bool {
        self.checked_isset
    }

    fn map<T: Clone>(&self, mapping: &HashMap<ID, T>) -> ExpressionVariable<T> {
        ExpressionVariable::new(self.variable.map(mapping), self.checked_isset)
    }
}

impl<ID> std::ops::Deref for ExpressionVariable<ID> {
    type Target = ID;
    fn deref(&self) -> &Self::Target {
        &self.variable
    }
}
impl<ID: StructuralEquality> StructuralEquality for ExpressionVariable<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.variable));
        hasher.write_u64(StructuralEquality::hash(&self.checked_isset));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.variable.equals(&other.variable) && self.checked_isset.equals(&other.checked_isset)
    }
}

impl<ID: fmt::Display> fmt::Display for ExpressionVariable<ID> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.variable, if self.checked_isset { "?" } else { "" })
    }
}

#[derive(Debug, Clone)]
pub struct Operation {
    operator: Operator,
    left_expression_id: ExpressionTreeNodeId,
    right_expression_id: ExpressionTreeNodeId,
    source_span: Option<Span>,
}

impl Operation {
    pub(crate) fn new(
        operator: Operator,
        left_expression_id: ExpressionTreeNodeId,
        right_expression_id: ExpressionTreeNodeId,
        source_span: Option<Span>,
    ) -> Operation {
        Self { operator, left_expression_id, right_expression_id, source_span }
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

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl Hash for Operation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.operator, state);
        Hash::hash(&self.left_expression_id, state);
        Hash::hash(&self.right_expression_id, state);
    }
}

impl Eq for Operation {}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.operator.eq(&other.operator)
            && self.left_expression_id.eq(&other.left_expression_id)
            && self.right_expression_id.eq(&other.right_expression_id)
    }
}

impl StructuralEquality for Operation {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.operator));
        hasher.write_u64(StructuralEquality::hash(&self.left_expression_id));
        hasher.write_u64(StructuralEquality::hash(&self.right_expression_id));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.operator.equals(&other.operator)
            && self.left_expression_id.equals(&other.left_expression_id)
            && self.right_expression_id.equals(&other.right_expression_id)
    }
}

#[derive(Debug, Clone)]
pub struct BuiltinValueFunctionCall {
    function_id: BuiltinValueFunctionID,
    argument_expression_ids: Vec<ExpressionTreeNodeId>,
    source_span: Option<Span>,
}

impl BuiltinValueFunctionCall {
    pub(crate) fn new(
        function_id: BuiltinValueFunctionID,
        argument_expression_ids: Vec<ExpressionTreeNodeId>,
        source_span: Option<Span>,
    ) -> Self {
        Self { function_id, argument_expression_ids, source_span }
    }

    pub fn function_id(&self) -> BuiltinValueFunctionID {
        self.function_id
    }

    pub fn argument_expression_ids(&self) -> &[ExpressionTreeNodeId] {
        &self.argument_expression_ids
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl Hash for BuiltinValueFunctionCall {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.function_id, state);
        Hash::hash(&self.argument_expression_ids, state);
    }
}

impl Eq for BuiltinValueFunctionCall {}

impl PartialEq for BuiltinValueFunctionCall {
    fn eq(&self, other: &Self) -> bool {
        self.function_id.eq(&other.function_id) && self.argument_expression_ids.eq(&other.argument_expression_ids)
    }
}

impl StructuralEquality for BuiltinValueFunctionCall {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.function_id));
        hasher.write_u64(StructuralEquality::hash(&self.argument_expression_ids));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.function_id.equals(&other.function_id)
            && self.argument_expression_ids.equals(&other.argument_expression_ids)
    }
}

macro_rules! function_id_enum {
    ( $($id:ident = $name:literal,)* ) => {
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
        pub enum BuiltinValueFunctionID {
            $( $id, ) *
        }

        impl BuiltinValueFunctionID {
            pub(crate) fn resolve_namespaced(name: &NamespacedFunctionName) -> Result<Self, Box<RepresentationError>> {
                match name.name.as_str() {
                    $( $name => Ok(Self::$id), )*
                    other => Err(Box::new(RepresentationError::UnresolvedFunction {
                        function_name: other.to_owned(),
                        source_span: name.span.clone(),
                    }))
                }
            }

            pub fn name(&self) -> &'static str {
                match self {
                    $( Self::$id => $name, )*
                }
            }

            pub fn optionality(&self) -> VariableOptionality {
                error::needs_update_when_feature_is_implemented!(error::UnimplementedFeature::OptionalBuiltinFunctions);
                VariableOptionality::Required
            }
        }
    };
}

function_id_enum! {
    // math unary
    MathAbs = "std::math::abs",
    MathCeil = "std::math::ceil",
    MathFloor = "std::math::floor",
    MathRound = "std::math::round",
    MathLog10 = "std::math::log10",

    // math binary
    MathMax = "std::math::max",
    MathMin = "std::math::min",

    // string
    StringLen = "std::string::len",
}

impl StructuralEquality for BuiltinValueFunctionID {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for BuiltinValueFunctionID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.name(), f)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum BuiltinConceptFunctionID {
    Iid,
    Label,

    GetDoc,
    GetMeta,
    GetAllMeta,

    GetOwnsDoc,
    GetOwnsMeta,
    GetOwnsAllMeta,

    GetPlaysDoc,
    GetPlaysMeta,
    GetPlaysAllMeta,

    GetRelatesDoc,
    GetRelatesMeta,
    GetRelatesAllMeta,

    GetSubDoc,
    GetSubMeta,
    GetSubAllMeta,

    GetFunDoc,
    GetFunMeta,
    GetFunAllMeta,

    GetStructDoc,
    GetStructMeta,
    GetStructAllMeta,

    GetStructFieldDoc,
    GetStructFieldMeta,
    GetStructFieldAllMeta,
}

impl BuiltinConceptFunctionID {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Iid => typeql::token::Function::Iid.as_str(),
            Self::Label => typeql::token::Function::Label.as_str(),

            Self::GetDoc => "get_doc",
            Self::GetMeta => "get_meta",
            Self::GetAllMeta => "get_all_meta",

            Self::GetOwnsDoc => "get_owns_doc",
            Self::GetOwnsMeta => "get_owns_meta",
            Self::GetOwnsAllMeta => "get_owns_all_meta",

            Self::GetPlaysDoc => "get_plays_doc",
            Self::GetPlaysMeta => "get_plays_meta",
            Self::GetPlaysAllMeta => "get_plays_all_meta",

            Self::GetRelatesDoc => "get_relates_doc",
            Self::GetRelatesMeta => "get_relates_meta",
            Self::GetRelatesAllMeta => "get_relates_all_meta",

            Self::GetSubDoc => "get_sub_doc",
            Self::GetSubMeta => "get_sub_meta",
            Self::GetSubAllMeta => "get_sub_all_meta",

            Self::GetFunDoc => "get_fun_doc",
            Self::GetFunMeta => "get_fun_meta",
            Self::GetFunAllMeta => "get_fun_all_meta",

            Self::GetStructDoc => "get_struct_doc",
            Self::GetStructMeta => "get_struct_meta",
            Self::GetStructAllMeta => "get_struct_all_meta",

            Self::GetStructFieldDoc => "get_struct_field_doc",
            Self::GetStructFieldMeta => "get_struct_field_meta",
            Self::GetStructFieldAllMeta => "get_struct_field_all_meta",
        }
    }

    pub(crate) fn from_str(str: &str) -> Option<Self> {
        match str {
            "iid" => Some(Self::Iid),
            "label" => Some(Self::Label),

            "get_doc" => Some(Self::GetDoc),
            "get_meta" => Some(Self::GetMeta),
            "get_all_meta" => Some(Self::GetAllMeta),

            "get_owns_doc" => Some(Self::GetOwnsDoc),
            "get_owns_meta" => Some(Self::GetOwnsMeta),
            "get_owns_all_meta" => Some(Self::GetOwnsAllMeta),

            "get_plays_doc" => Some(Self::GetPlaysDoc),
            "get_plays_meta" => Some(Self::GetPlaysMeta),
            "get_plays_all_meta" => Some(Self::GetPlaysAllMeta),

            "get_relates_doc" => Some(Self::GetRelatesDoc),
            "get_relates_meta" => Some(Self::GetRelatesMeta),
            "get_relates_all_meta" => Some(Self::GetRelatesAllMeta),

            "get_sub_doc" => Some(Self::GetSubDoc),
            "get_sub_meta" => Some(Self::GetSubMeta),
            "get_sub_all_meta" => Some(Self::GetSubAllMeta),

            "get_fun_doc" => Some(Self::GetFunDoc),
            "get_fun_meta" => Some(Self::GetFunMeta),
            "get_fun_all_meta" => Some(Self::GetFunAllMeta),

            "get_struct_doc" => Some(Self::GetStructDoc),
            "get_struct_meta" => Some(Self::GetStructMeta),
            "get_struct_all_meta" => Some(Self::GetStructAllMeta),

            "get_struct_field_doc" => Some(Self::GetStructFieldDoc),
            "get_struct_field_meta" => Some(Self::GetStructFieldMeta),
            "get_struct_field_all_meta" => Some(Self::GetStructFieldAllMeta),

            _ => None,
        }
    }

    pub(crate) fn signature(self) -> FunctionSignature {
        macro_rules! function_signature {
            (($($arg_category:ident),*) -> $($return_category:ident),*) => {
                FunctionSignature::new(
                    FunctionID::Builtin(self),
                    vec![$(VariableCategory::$arg_category),*],
                    vec![$((VariableCategory::$return_category, VariableOptionality::Required)),*],
                    false,
                )
            };
            (($($arg_category:ident),*) -> { $($return_category:ident),* }) => {
                FunctionSignature::new(
                    FunctionID::Builtin(self),
                    vec![$(VariableCategory::$arg_category),*],
                    vec![$((VariableCategory::$return_category, VariableOptionality::Required)),*],
                    true,
                )
            };
        }

        match self {
            Self::Iid => function_signature!((Thing) -> Value),
            Self::Label => function_signature!((Type) -> Value),

            Self::GetDoc => function_signature!((Type) -> Value),
            Self::GetMeta => function_signature!((Value, Type) -> Value),
            Self::GetAllMeta => function_signature!((Type) -> { Value, Value }),

            Self::GetOwnsDoc => function_signature!((Type, Type) -> Value),
            Self::GetOwnsMeta => function_signature!((Value, Type, Type) -> Value),
            Self::GetOwnsAllMeta => function_signature!((Type, Type) -> { Value, Value }),

            Self::GetPlaysDoc => function_signature!((Type, Type) -> Value),
            Self::GetPlaysMeta => function_signature!((Value, Type, Type) -> Value),
            Self::GetPlaysAllMeta => function_signature!((Type, Type) -> { Value, Value }),

            Self::GetRelatesDoc => function_signature!((Type, Type) -> Value),
            Self::GetRelatesMeta => function_signature!((Value, Type, Type) -> Value),
            Self::GetRelatesAllMeta => function_signature!((Type, Type) -> { Value, Value }),

            Self::GetSubDoc => function_signature!((Type, Type) -> Value),
            Self::GetSubMeta => function_signature!((Value, Type, Type) -> Value),
            Self::GetSubAllMeta => function_signature!((Type, Type) -> { Value, Value }),

            Self::GetFunDoc => function_signature!((Value) -> Value),
            Self::GetFunMeta => function_signature!((Value, Value) -> Value),
            Self::GetFunAllMeta => function_signature!((Value) -> { Value, Value }),

            Self::GetStructDoc => function_signature!((Value) -> Value),
            Self::GetStructMeta => function_signature!((Value, Value) -> Value),
            Self::GetStructAllMeta => function_signature!((Value) -> { Value, Value }),

            Self::GetStructFieldDoc => function_signature!((Value, Value) -> Value),
            Self::GetStructFieldMeta => function_signature!((Value, Value, Value) -> Value),
            Self::GetStructFieldAllMeta => function_signature!((Value, Value) -> { Value, Value }),
        }
    }
}

impl StructuralEquality for BuiltinConceptFunctionID {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for BuiltinConceptFunctionID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.name(), f)
    }
}

#[derive(Debug, Clone)]
pub struct ListIndex<ID> {
    list_variable: ExpressionVariable<ID>,
    index_expression_id: ExpressionTreeNodeId,
    source_span: Option<Span>,
}

impl<ID> ListIndex<ID> {
    pub(crate) fn new(
        list_variable: ExpressionVariable<ID>,
        index_expression_id: ExpressionTreeNodeId,
        source_span: Option<Span>,
    ) -> ListIndex<ID> {
        Self { list_variable, index_expression_id, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> ListIndex<ID> {
    pub fn list_variable(&self) -> &ExpressionVariable<ID> {
        &self.list_variable
    }

    pub fn index_expression_id(&self) -> ExpressionTreeNodeId {
        self.index_expression_id
    }

    fn map<T: Clone>(&self, mapping: &HashMap<ID, T>) -> ListIndex<T> {
        ListIndex::new(self.list_variable.map(mapping), self.index_expression_id, self.source_span)
    }
}

impl<ID: Hash> Hash for ListIndex<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.list_variable, state);
        Hash::hash(&self.index_expression_id, state);
    }
}
impl<ID: PartialEq> Eq for ListIndex<ID> {}

impl<ID: PartialEq> PartialEq for ListIndex<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.list_variable.eq(&other.list_variable) && self.index_expression_id.eq(&other.index_expression_id)
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
#[derive(Debug, Clone)]
pub struct ListConstructor {
    item_expression_ids: Vec<ExpressionTreeNodeId>,
    len_id: ParameterID,
    source_span: Option<Span>,
}

impl ListConstructor {
    pub fn new(item_expression_ids: Vec<ExpressionTreeNodeId>, len_id: ParameterID, source_span: Option<Span>) -> Self {
        Self { item_expression_ids, len_id, source_span }
    }

    pub fn item_expression_ids(&self) -> &[ExpressionTreeNodeId] {
        &self.item_expression_ids
    }

    pub fn len_id(&self) -> &ParameterID {
        &self.len_id
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl Hash for ListConstructor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.item_expression_ids, state);
        Hash::hash(&self.len_id, state);
    }
}

impl Eq for ListConstructor {}

impl PartialEq for ListConstructor {
    fn eq(&self, other: &Self) -> bool {
        self.item_expression_ids.eq(&other.item_expression_ids) && self.len_id.eq(&other.len_id)
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
#[derive(Debug, Clone)]
pub struct ListIndexRange<ID> {
    list_variable: ExpressionVariable<ID>,
    from_expression_id: ExpressionTreeNodeId,
    to_expression_id: ExpressionTreeNodeId,
    source_span: Option<Span>,
}

impl<ID> ListIndexRange<ID> {
    pub(crate) fn new(
        list_variable: ExpressionVariable<ID>,
        from_expression_id: ExpressionTreeNodeId,
        to_expression_id: ExpressionTreeNodeId,
        source_span: Option<Span>,
    ) -> Self {
        Self { list_variable, from_expression_id, to_expression_id, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> ListIndexRange<ID> {
    pub fn list_variable(&self) -> &ExpressionVariable<ID> {
        &self.list_variable
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
            self.from_expression_id,
            self.to_expression_id,
            self.source_span,
        )
    }
}

impl<ID: Hash> Hash for ListIndexRange<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.list_variable, state);
        Hash::hash(&self.from_expression_id, state);
        Hash::hash(&self.to_expression_id, state);
    }
}

impl<ID: PartialEq> Eq for ListIndexRange<ID> {}

impl<ID: PartialEq> PartialEq for ListIndexRange<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.list_variable.eq(&other.list_variable)
            && self.from_expression_id.eq(&other.from_expression_id)
            && self.to_expression_id.eq(&other.to_expression_id)
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

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Add => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Add, f),
            Operator::Subtract => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Subtract, f),
            Operator::Multiply => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Multiply, f),
            Operator::Divide => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Divide, f),
            Operator::Modulo => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Modulo, f),
            Operator::Power => fmt::Display::fmt(&typeql::token::ArithmeticOperator::Power, f),
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
        match self {
            Expression::Constant(parameter_id) => {
                write!(f, "Constant({})", parameter_id)
            }
            Expression::Variable(variable) => {
                write!(f, "Variable({})", variable)
            }
            Expression::Operation(operation) => {
                write!(
                    f,
                    "Operator({} {} {} )",
                    operation.left_expression_id,
                    operation.operator(),
                    operation.right_expression_id
                )
            }
            Expression::BuiltinValueFunctionCall(builtin) => {
                write!(
                    f,
                    "FunctionCall({}({}))",
                    builtin.function_id(),
                    FormatJoined(&builtin.argument_expression_ids, ',')
                )
            }
            Expression::ListIndex(inner) => {
                write!(f, "ListIndex({}[{}])", inner.list_variable(), inner.index_expression_id)
            }
            Expression::List(list) => {
                write!(f, "ListConstructor([{}])", FormatJoined(&list.item_expression_ids, ','))
            }
            Expression::ListIndexRange(list_range) => {
                write!(
                    f,
                    "ListIndexRange({}[{}..{}])",
                    list_range.list_variable(),
                    list_range.from_expression_id,
                    list_range.to_expression_id
                )
            }
            Expression::MayShortCircuit(inner) => {
                write!(f, "MayShortCircuitOther({})", inner)
            }
        }
    }
}

fn collect_reference_optionalities<ID1: IrID>(
    tree: &ExpressionTree<ID1>,
    at: ExpressionTreeNodeId,
    context_optionality: ReferenceOptionality,
    acc: &mut HashMap<ID1, ReferenceOptionality>,
) {
    let of_checked_isset = |variable: &ExpressionVariable<ID1>| match variable.checked_isset {
        true => ReferenceOptionality::Optional,
        false => ReferenceOptionality::Required,
    };
    match tree.get(at) {
        Expression::Variable(variable) => {
            let optionality = match context_optionality == ReferenceOptionality::Optional || variable.checked_isset {
                true => ReferenceOptionality::Optional,
                false => ReferenceOptionality::Required,
            };
            *acc.entry(**variable).or_insert(ReferenceOptionality::Optional) &= optionality;
        }
        Expression::MayShortCircuit(inner) => {
            collect_reference_optionalities(tree, *inner, ReferenceOptionality::Optional, acc);
        }
        Expression::Operation(Operation { left_expression_id, right_expression_id, .. }) => {
            collect_reference_optionalities(tree, *left_expression_id, ReferenceOptionality::Required, acc);
            collect_reference_optionalities(tree, *right_expression_id, ReferenceOptionality::Required, acc);
        }
        Expression::BuiltinValueFunctionCall(BuiltinValueFunctionCall { argument_expression_ids, .. }) => {
            for arg_id in argument_expression_ids {
                error::needs_update_when_feature_is_implemented!(error::UnimplementedFeature::OptionalArguments);
                collect_reference_optionalities(tree, *arg_id, ReferenceOptionality::Required, acc);
            }
        }
        Expression::ListIndex(ListIndex { list_variable, index_expression_id, .. }) => {
            *acc.entry(**list_variable).or_insert(ReferenceOptionality::Optional) &= of_checked_isset(list_variable);
        }
        Expression::List(ListConstructor { item_expression_ids, .. }) => {
            for item_id in item_expression_ids {
                collect_reference_optionalities(tree, *item_id, ReferenceOptionality::Required, acc);
            }
        }
        Expression::ListIndexRange(list_index) => {
            *acc.entry(*list_index.list_variable).or_insert(ReferenceOptionality::Optional) &=
                of_checked_isset(&list_index.list_variable);
            collect_reference_optionalities(tree, list_index.from_expression_id, ReferenceOptionality::Required, acc);
            collect_reference_optionalities(tree, list_index.to_expression_id, ReferenceOptionality::Required, acc);
        }
        Expression::Constant(_) => {
            // No variables, no problems
        }
    }
}

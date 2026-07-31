/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::marker::PhantomData;
use typeql::common::Span;
use answer::variable::Variable;
use encoding::value::{
    ValueEncodable,
    value_type::{ValueType, ValueTypeCategory},
};
use error::needs_update_when_feature_is_implemented;
use ir::{
    pattern::{
        ParameterID,
        expression::{
            BuiltinValueFunctionCall, BuiltinValueFunctionID, Expression, ExpressionTree, ListConstructor, ListIndex,
            ListIndexRange, Operation,
        },
    },
    pipeline::ParameterRegistry,
};

use crate::annotation::expression::{ExpressionCompileError, compiled_expression::{ExecutableExpression, ExpressionValueType}, instructions::{
    CompilableExpression, ExpressionInstruction,
    binary::{
        MathMaxDecimalDecimal, MathMaxDoubleDouble, MathMaxIntegerInteger, MathMinDecimalDecimal,
        MathMinDoubleDouble, MathMinIntegerInteger,
    },
    list_operations,
    load::{LoadConstant, LoadVariable},
    op_codes::ExpressionOpCode,
    unary::{
        LenString, MathAbsDecimal, MathAbsDouble, MathAbsInteger, MathCeilDecimal, MathCeilDouble,
        MathFloorDecimal, MathFloorDouble, MathRoundDecimal, MathRoundDouble,
    },
}, operation_resolution, builtin_resolution};

pub struct ExpressionCompilationContext<'this> {
    expression_tree: &'this ExpressionTree<Variable>,
    variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
    parameters: &'this ParameterRegistry,
    type_stack: Vec<ExpressionValueType>,

    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<ParameterID>,
}

impl<'this> ExpressionCompilationContext<'this> {
    fn empty(
        expression_tree: &'this ExpressionTree<Variable>,
        variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
        parameters: &'this ParameterRegistry,
    ) -> Self {
        ExpressionCompilationContext {
            expression_tree,
            variable_value_categories,
            parameters,
            instructions: Vec::new(),
            variable_stack: Vec::new(),
            constant_stack: Vec::new(),
            type_stack: Vec::new(),
        }
    }

    pub fn compile(
        expression_tree: &ExpressionTree<Variable>,
        variable_value_categories: &HashMap<Variable, ExpressionValueType>,
        parameters: &ParameterRegistry,
    ) -> Result<ExecutableExpression<Variable>, Box<ExpressionCompileError>> {
        debug_assert!(expression_tree.argument_ids().all(|var| variable_value_categories.contains_key(&var)));
        let mut builder = ExpressionCompilationContext::empty(expression_tree, variable_value_categories, parameters);
        builder.compile_recursive(expression_tree.get_root())?;
        let return_type = builder.pop_type()?;
        let ExpressionCompilationContext { instructions, variable_stack, constant_stack, .. } = builder;
        Ok(ExecutableExpression { instructions, variables: variable_stack, constants: constant_stack, return_type })
    }

    fn compile_recursive(&mut self, expression: &Expression<Variable>) -> Result<(), Box<ExpressionCompileError>> {
        match expression {
            Expression::Constant(constant) => self.compile_constant(constant),
            Expression::Variable(variable) => self.compile_variable(variable),
            Expression::Operation(op) => self.compile_op(op),
            Expression::BuiltinValueFunctionCall(builtin) => self.compile_value_builtin(builtin),
            Expression::ListIndex(list_index) => self.compile_list_index(list_index),
            Expression::List(list_constructor) => self.compile_list_constructor(list_constructor),
            Expression::ListIndexRange(list_index_range) => self.compile_list_index_range(list_index_range),
        }
    }

    fn compile_constant(&mut self, constant: &ParameterID) -> Result<(), Box<ExpressionCompileError>> {
        self.constant_stack.push(constant.clone());

        self.push_type_single(self.parameters.value_unchecked(constant).value_type());
        self.append_instruction(LoadConstant::OP_CODE);

        Ok(())
    }

    fn compile_variable(&mut self, variable: &Variable) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(variable));

        self.variable_stack.push(*variable);
        self.append_instruction(LoadVariable::OP_CODE);
        // TODO: We need a way to know if a variable is a list or a single
        match self.variable_value_categories.get(variable).unwrap() {
            ExpressionValueType::Single(value_type) => self.push_type_single(value_type.clone()),
            ExpressionValueType::List(value_type) => self.push_type_list(value_type.clone()),
        }
        Ok(())
    }

    fn compile_list_constructor(
        &mut self,
        list_constructor: &ListConstructor,
    ) -> Result<(), Box<ExpressionCompileError>> {
        for expression_id in list_constructor.item_expression_ids().iter().rev() {
            self.compile_recursive(self.expression_tree.get(*expression_id))?;
        }

        self.compile_constant(list_constructor.len_id())?;
        self.append_instruction(list_operations::ListConstructor::OP_CODE);

        if self.pop_type_single()?.category() != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::InternalListLengthMustBeInteger {})?;
        }
        let n_elements = list_constructor.item_expression_ids().len();
        if n_elements > 0 {
            let element_type = self.pop_type_single()?;
            for _ in 1..list_constructor.item_expression_ids().len() {
                if self.pop_type_single()? != element_type {
                    Err(ExpressionCompileError::HeterogeneusListConstructor {
                        source_span: list_constructor.source_span(),
                    })?;
                }
            }
            self.push_type_list(element_type)
        } else {
            Err(ExpressionCompileError::EmptyListConstructorCannotInferValueType {
                source_span: list_constructor.source_span(),
            })?;
        }

        Ok(())
    }

    fn compile_list_index(&mut self, list_index: &ListIndex<Variable>) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index.list_variable()));

        self.compile_recursive(self.expression_tree.get(list_index.index_expression_id()))?;
        self.compile_variable(&list_index.list_variable())?;

        self.append_instruction(list_operations::ListIndex::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let index_type = self.pop_type_single()?.category();
        if index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index.source_span() })?
        }
        self.push_type_single(list_variable_type); // reuse
        Ok(())
    }

    fn compile_list_index_range(
        &mut self,
        list_index_range: &ListIndexRange<Variable>,
    ) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index_range.list_variable()));
        self.compile_recursive(self.expression_tree.get(list_index_range.from_expression_id()))?;
        self.compile_recursive(self.expression_tree.get(list_index_range.to_expression_id()))?;
        self.compile_variable(&list_index_range.list_variable())?;

        self.append_instruction(list_operations::ListIndexRange::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let from_index_type = self.pop_type_single()?.category();
        if from_index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index_range.source_span() })?
        }
        let to_index_type = self.pop_type_single()?.category();
        if to_index_type != ValueTypeCategory::Integer {
            Err(ExpressionCompileError::ListIndexMustBeInteger { source_span: list_index_range.source_span() })?
        }

        self.push_type_single(list_variable_type);
        Ok(())
    }

    fn compile_op(&mut self, operation: &Operation) -> Result<(), Box<ExpressionCompileError>> {
        let operator = operation.operator();
        self.compile_recursive(self.expression_tree.get(operation.left_expression_id()))?;
        let left_category = self.peek_type_single()?.category();
        self.compile_recursive(self.expression_tree.get(operation.right_expression_id()))?;
        let right_category = self.peek_type_single()?.category();
        operation_resolution::resolve_op(self, operator, left_category, right_category, operation.source_span())
    }

    fn compile_value_builtin(&mut self, builtin: &BuiltinValueFunctionCall) -> Result<(), Box<ExpressionCompileError>> {
        match builtin.function_id() {
            BuiltinValueFunctionID::Abs => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Integer => MathAbsInteger::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathAbsDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathAbsDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Ceil => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathCeilDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathCeilDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Floor => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathFloorDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathFloorDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Round => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::Double => MathRoundDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathRoundDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Min => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                let arg_1_category = self.peek_type_single()?.category();
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[1]))?;
                let arg_2_category = self.peek_type_single()?.category();
                // Both arguments must have the same type category
                if arg_1_category != arg_2_category {
                    return Err(Box::new(ExpressionCompileError::UnsupportedDifferentArgumentForBuiltin {
                        function: builtin.function_id(),
                        arg_1_category,
                        arg_2_category,
                        source_span: builtin.source_span(),
                    }));
                }
                match arg_1_category {
                    ValueTypeCategory::Integer => MathMinIntegerInteger::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathMinDoubleDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathMinDecimalDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: arg_1_category,
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Max => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                let arg_1_category = self.peek_type_single()?.category();
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[1]))?;
                let arg_2_category = self.peek_type_single()?.category();
                // Both arguments must have the same type category
                if arg_1_category != arg_2_category {
                    return Err(Box::new(ExpressionCompileError::UnsupportedDifferentArgumentForBuiltin {
                        function: builtin.function_id(),
                        arg_1_category,
                        arg_2_category,
                        source_span: builtin.source_span(),
                    }));
                }
                match arg_1_category {
                    ValueTypeCategory::Integer => MathMaxIntegerInteger::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathMaxDoubleDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => MathMaxDecimalDecimal::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: arg_1_category,
                        source_span: builtin.source_span(),
                    })?,
                }
            }
            BuiltinValueFunctionID::Len => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()?.category() {
                    ValueTypeCategory::String => LenString::validate_and_append(self)?,
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin {
                        function: builtin.function_id(),
                        category: self.peek_type_single()?.category(),
                        source_span: builtin.source_span(),
                    })?,
                }
            }
        }
        Ok(())
    }

    fn pop_type(&mut self) -> Result<ExpressionValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn pop_type_single(&mut self) -> Result<ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedSingleWasList {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn pop_type_list(&mut self) -> Result<ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::List(value)) => Ok(value),
            Some(ExpressionValueType::Single(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedListWasSingle {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn push_type_single(&mut self, value: ValueType) {
        self.type_stack.push(ExpressionValueType::Single(value));
    }

    pub(crate) fn push_type_list(&mut self, value: ValueType) {
        self.type_stack.push(ExpressionValueType::List(value));
    }

    fn peek_type_single(&self) -> Result<&ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.last() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedSingleWasList {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    #[expect(unused, reason = "lists are not yet implemented in the compiler")]
    pub(crate) fn peek_type_list(&mut self) -> Result<&ValueType, Box<ExpressionCompileError>> {
        match self.type_stack.last() {
            Some(ExpressionValueType::List(value)) => Ok(value),
            Some(ExpressionValueType::Single(_)) => {
                Err(Box::new(ExpressionCompileError::InternalExpectedListWasSingle {}))
            }
            None => Err(ExpressionCompileError::InternalStackWasEmpty {})?,
        }
    }

    pub(crate) fn append_instruction(&mut self, op_code: ExpressionOpCode) {
        self.instructions.push(op_code)
    }
}

// Helpers for the function resolution
pub trait BuiltinValueFunctionResolver {
    const ID: BuiltinValueFunctionID;

    fn resolve_validate_append(builtin: &BuiltinValueFunctionCall, builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>>;
}

pub(super) trait UnaryValueFunctionResolver {
    const UNARY_ID: BuiltinValueFunctionID;
    fn resolve_validate_append_unary(t1: ValueTypeCategory, builder: &mut ExpressionCompilationContext<'_>, source_span: Option<Span>) -> Result<(), Box<ExpressionCompileError>>;
}

pub(super) trait BinaryValueFunctionResolver {
    const BINARY_ID: BuiltinValueFunctionID;
    const ARGS_MUST_HAVE_SAME_CATEGORIES: bool;
    fn resolve_validate_append_binary(t1: ValueTypeCategory, t2: ValueTypeCategory, builder: &mut ExpressionCompilationContext<'_>, source_span: Option<Span>) -> Result<(), Box<ExpressionCompileError>>;
}

struct UnaryValueFunctionResolverImpl<T: UnaryValueFunctionResolver>(PhantomData<T>);
impl<T: UnaryValueFunctionResolver> BuiltinValueFunctionResolver for UnaryValueFunctionResolverImpl<T> {
    const ID: BuiltinValueFunctionID = T::UNARY_ID;

    fn resolve_validate_append(builtin: &BuiltinValueFunctionCall, builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(T::UNARY_ID == builtin.function_id() && builtin.argument_expression_ids().len() == 1 );
        needs_update_when_feature_is_implemented!(Lists); // If functions accept list
        builder.compile_recursive(builder.expression_tree.get(builtin.argument_expression_ids()[0]))?;
        let t1 = builder.peek_type_single()?.category();
        T::resolve_validate_append_unary(t1, builder, builtin.source_span())
    }
}

struct BinaryValueFunctionResolverImpl<T: BinaryValueFunctionResolver>(PhantomData<T>);
impl<T: BinaryValueFunctionResolver> BuiltinValueFunctionResolver for BinaryValueFunctionResolverImpl<T> {
    const ID: BuiltinValueFunctionID = T::BINARY_ID;

    fn resolve_validate_append(builtin: &BuiltinValueFunctionCall, builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        debug_assert!(T::BINARY_ID == builtin.function_id() && builtin.argument_expression_ids().len() == 2);
        needs_update_when_feature_is_implemented!(Lists); // If functions accept list
        builder.compile_recursive(builder.expression_tree.get(builtin.argument_expression_ids()[0]))?;
        let arg_1_category = builder.peek_type_single()?.category();
        builder.compile_recursive(builder.expression_tree.get(builtin.argument_expression_ids()[1]))?;
        let arg_2_category = builder.peek_type_single()?.category();

        if T::ARGS_MUST_HAVE_SAME_CATEGORIES && arg_1_category != arg_2_category {
            return Err(Box::new(ExpressionCompileError::UnsupportedDifferentArgumentForBuiltin {
                function: builtin.function_id(),
                arg_1_category,
                arg_2_category,
                source_span: builtin.source_span(),
            }));
        }
        T::resolve_validate_append_binary(arg_1_category, arg_2_category, builder, builtin.source_span())
    }
}

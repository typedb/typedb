/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::{value::Value, value_type::ValueTypeCategory, ValueEncodable};
use ir::pattern::expression::{
    BuiltInCall, BuiltInFunctionID, Expression, ExpressionTree, ListConstructor, ListIndex,
    ListIndexRange, Operation, Operator,
};

use crate::{
    expression::compiled_expression::{CompiledExpression, ExpressionValueType},
    instruction::expression::{
        list_operations,
        load_cast::{CastLeftLongToDouble, CastRightLongToDouble, LoadConstant, LoadVariable},
        op_codes::ExpressionOpCode,
        operators,
        unary::{MathAbsDouble, MathAbsLong, MathCeilDouble, MathFloorDouble, MathRoundDouble},
        CompilableExpression, ExpressionInstruction,
    },
};
use crate::expression::ExpressionCompileError;

pub struct ExpressionCompilationContext<'this> {
    expression_tree: &'this ExpressionTree<Variable>,
    variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
    type_stack: Vec<ExpressionValueType>,

    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<Value<'static>>,
}

impl<'this> ExpressionCompilationContext<'this> {
    fn empty(
        expression_tree: &'this ExpressionTree<Variable>,
        variable_value_categories: &'this HashMap<Variable, ExpressionValueType>,
    ) -> Self {
        ExpressionCompilationContext {
            expression_tree,
            variable_value_categories,
            instructions: Vec::new(),
            variable_stack: Vec::new(),
            constant_stack: Vec::new(),
            type_stack: Vec::new(),
        }
    }

    pub fn compile(
        expression_tree: &ExpressionTree<Variable>,
        variable_value_categories: &HashMap<Variable, ExpressionValueType>,
    ) -> Result<CompiledExpression, ExpressionCompileError> {
        debug_assert!(expression_tree.variables().all(|var| variable_value_categories.contains_key(&var)));
        let mut builder = ExpressionCompilationContext::empty(expression_tree, variable_value_categories);
        builder.compile_recursive(expression_tree.get_root())?;
        let return_type = builder.pop_type()?;
        let ExpressionCompilationContext { instructions, variable_stack, constant_stack, .. } = builder;
        Ok(CompiledExpression { instructions, variables: variable_stack, constants: constant_stack, return_type })
    }

    fn compile_recursive(&mut self, expression: &Expression<Variable>) -> Result<(), ExpressionCompileError> {
        match expression {
            Expression::Constant(constant) => self.compile_constant(constant),
            Expression::Variable(variable) => self.compile_variable(variable),
            Expression::Operation(op) => self.compile_op(op),
            Expression::BuiltInCall(builtin) => self.compile_builtin(builtin),
            Expression::ListIndex(list_index) => self.compile_list_index(list_index),
            Expression::List(list_constructor) => self.compile_list_constructor(list_constructor),
            Expression::ListIndexRange(list_index_range) => self.compile_list_index_range(list_index_range),
        }
    }

    fn compile_constant(&mut self, constant: &Value<'static>) -> Result<(), ExpressionCompileError> {
        self.constant_stack.push(constant.clone());

        self.push_type_single(constant.value_type().category());
        self.append_instruction(LoadConstant::OP_CODE);

        Ok(())
    }

    fn compile_variable(&mut self, variable: &Variable) -> Result<(), ExpressionCompileError> {
        debug_assert!(self.variable_value_categories.contains_key(variable));

        self.variable_stack.push(variable.clone());
        self.append_instruction(LoadVariable::OP_CODE);
        // TODO: We need a way to know if a variable is a list or a single
        match self.variable_value_categories.get(&variable).unwrap() {
            ExpressionValueType::Single(value_type) => self.push_type_single(value_type.clone()),
            ExpressionValueType::List(value_type) => self.push_type_list(value_type.clone()),
        }
        Ok(())
    }

    fn compile_list_constructor(
        &mut self,
        list_constructor: &ListConstructor,
    ) -> Result<(), ExpressionCompileError> {
        for expression_id in list_constructor.item_expression_ids().iter().rev() {
            self.compile_recursive(self.expression_tree.get(*expression_id))?;
        }
        self.compile_constant(&Value::Long(list_constructor.item_expression_ids().len() as i64))?;
        self.append_instruction(list_operations::ListConstructor::OP_CODE);

        if self.pop_type_single()? != ValueTypeCategory::Long {
            Err(ExpressionCompileError::InternalUnexpectedValueType)?;
        }
        let n_elements = list_constructor.item_expression_ids().len();
        if n_elements > 0 {
            let element_type = self.pop_type_single()?;
            for _ in 1..list_constructor.item_expression_ids().len() {
                if self.pop_type_single()? != element_type {
                    Err(ExpressionCompileError::HeterogenousValuesInList)?;
                }
            }
            self.push_type_list(element_type)
        } else {
            Err(ExpressionCompileError::EmptyListConstructorCannotInferValueType)?;
        }

        Ok(())
    }

    fn compile_list_index(&mut self, list_index: &ListIndex<Variable>) -> Result<(), ExpressionCompileError> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index.list_variable()));

        self.compile_recursive(self.expression_tree.get(list_index.index_expression_id()))?;
        self.compile_variable(&list_index.list_variable())?;

        self.append_instruction(list_operations::ListIndex::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let index_type = self.pop_type_single()?;
        if index_type != ValueTypeCategory::Long {
            Err(ExpressionCompileError::ListIndexMustBeLong)?
        }
        self.push_type_single(list_variable_type); // reuse
        Ok(())
    }

    fn compile_list_index_range(
        &mut self,
        list_index_range: &ListIndexRange<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index_range.list_variable()));
        self.compile_recursive(self.expression_tree.get(list_index_range.from_expression_id()))?;
        self.compile_recursive(self.expression_tree.get(list_index_range.to_expression_id()))?;
        self.compile_variable(&list_index_range.list_variable())?;

        self.append_instruction(list_operations::ListIndexRange::OP_CODE);

        let list_variable_type = self.pop_type_list()?;
        let from_index_type = self.pop_type_single()?;
        if from_index_type != ValueTypeCategory::Long {
            Err(ExpressionCompileError::ListIndexMustBeLong)?
        }
        let to_index_type = self.pop_type_single()?;
        if to_index_type != ValueTypeCategory::Long {
            Err(ExpressionCompileError::ListIndexMustBeLong)?
        }

        Ok(self.push_type_single(list_variable_type))
    }

    fn compile_op(&mut self, operation: &Operation) -> Result<(), ExpressionCompileError> {
        let operator = operation.operator();
        let right_expression = self.expression_tree.get(operation.right_expression_id());
        self.compile_recursive(self.expression_tree.get(operation.left_expression_id()))?;
        let left_category = self.peek_type_single()?;
        match left_category {
            ValueTypeCategory::Boolean => self.compile_op_boolean(operator, right_expression),
            ValueTypeCategory::Long => self.compile_op_long(operator, right_expression),
            ValueTypeCategory::Double => self.compile_op_double(operator, right_expression),
            ValueTypeCategory::Decimal => self.compile_op_decimal(operator, right_expression),
            ValueTypeCategory::Date => self.compile_op_date(operator, right_expression),
            ValueTypeCategory::DateTime => self.compile_op_datetime(operator, right_expression),
            ValueTypeCategory::DateTimeTZ => self.compile_op_datetime_tz(operator, right_expression),
            ValueTypeCategory::Duration => self.compile_op_duration(operator, right_expression),
            ValueTypeCategory::String => self.compile_op_string(operator, right_expression),
            ValueTypeCategory::Struct => self.compile_op_struct(operator, right_expression),
        }
    }

    fn compile_op_boolean(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Decimal,
            right_category,
        })
    }

    fn compile_op_long(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        match right_category {
            ValueTypeCategory::Long => {
                self.compile_op_long_long(op)?;
            }
            ValueTypeCategory::Double => {
                // The left needs to be cast
                CastLeftLongToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Long,
                right_category,
            })?,
        }
        Ok(())
    }

    fn compile_op_double(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        match right_category {
            ValueTypeCategory::Long => {
                // The right needs to be cast
                CastRightLongToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Double => {
                self.compile_op_double_double(op)?;
            }
            _ => Err(ExpressionCompileError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Double,
                right_category,
            })?,
        }
        Ok(())
    }

    fn compile_op_decimal(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Decimal,
            right_category,
        })
    }

    fn compile_op_string(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::String,
            right_category,
        })
    }

    fn compile_op_date(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Date,
            right_category,
        })
    }

    fn compile_op_datetime(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTime,
            right_category,
        })
    }

    fn compile_op_datetime_tz(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::DateTimeTZ,
            right_category,
        })
    }

    fn compile_op_duration(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Duration,
            right_category,
        })
    }

    fn compile_op_struct(
        &mut self,
        op: Operator,
        right: &Expression<Variable>,
    ) -> Result<(), ExpressionCompileError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type_single()?.clone();
        Err(ExpressionCompileError::UnsupportedOperandsForOperation {
            op,
            left_category: ValueTypeCategory::Struct,
            right_category,
        })
    }

    // Ops with Left, Right resolved
    fn compile_op_long_long(&mut self, op: Operator) -> Result<(), ExpressionCompileError> {
        match op {
            Operator::Add => operators::OpLongAddLong::validate_and_append(self)?,
            Operator::Subtract => operators::OpLongSubtractLong::validate_and_append(self)?,
            Operator::Multiply => operators::OpLongMultiplyLong::validate_and_append(self)?,
            Operator::Divide => operators::OpLongDivideLong::validate_and_append(self)?,
            Operator::Modulo => operators::OpLongModuloLong::validate_and_append(self)?,
            Operator::Power => operators::OpLongPowerLong::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_op_double_double(&mut self, op: Operator) -> Result<(), ExpressionCompileError> {
        match op {
            Operator::Add => operators::OpDoubleAddDouble::validate_and_append(self)?,
            Operator::Subtract => operators::OpDoubleSubtractDouble::validate_and_append(self)?,
            Operator::Multiply => operators::OpDoubleMultiplyDouble::validate_and_append(self)?,
            Operator::Divide => operators::OpDoubleDivideDouble::validate_and_append(self)?,
            Operator::Modulo => operators::OpDoubleModuloDouble::validate_and_append(self)?,
            Operator::Power => operators::OpDoublePowerDouble::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_builtin(&mut self, builtin: &BuiltInCall) -> Result<(), ExpressionCompileError> {
        match builtin.builtin_id() {
            BuiltInFunctionID::Abs => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()? {
                    ValueTypeCategory::Long => MathAbsLong::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathAbsDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Ceil => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()? {
                    ValueTypeCategory::Double => MathCeilDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Floor => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()? {
                    ValueTypeCategory::Double => MathFloorDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Round => {
                self.compile_recursive(self.expression_tree.get(builtin.argument_expression_ids()[0]))?;
                match self.peek_type_single()? {
                    ValueTypeCategory::Double => MathRoundDouble::validate_and_append(self)?,
                    // TODO: ValueTypeCategory::Decimal ?
                    _ => Err(ExpressionCompileError::UnsupportedArgumentsForBuiltin)?,
                }
            }
        }
        Ok(())
    }

    fn pop_type(&mut self) -> Result<ExpressionValueType, ExpressionCompileError> {
        match self.type_stack.pop() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompileError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn pop_type_single(&mut self) -> Result<ValueTypeCategory, ExpressionCompileError> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => Err(ExpressionCompileError::ExpectedSingleWasList),
            None => Err(ExpressionCompileError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn pop_type_list(&mut self) -> Result<ValueTypeCategory, ExpressionCompileError> {
        match self.type_stack.pop() {
            Some(ExpressionValueType::List(value)) => Ok(value),
            Some(ExpressionValueType::Single(_)) => Err(ExpressionCompileError::ExpectedListWasSingle),
            None => Err(ExpressionCompileError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn push_type_single(&mut self, value: ValueTypeCategory) {
        self.type_stack.push(ExpressionValueType::Single(value));
    }

    pub(crate) fn push_type_list(&mut self, value: ValueTypeCategory) {
        self.type_stack.push(ExpressionValueType::List(value));
    }

    fn peek_type_single(&self) -> Result<&ValueTypeCategory, ExpressionCompileError> {
        match self.type_stack.last() {
            Some(ExpressionValueType::Single(value)) => Ok(value),
            Some(ExpressionValueType::List(_)) => Err(ExpressionCompileError::ExpectedSingleWasList),
            None => Err(ExpressionCompileError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn peek_type_list(&mut self) -> Result<ValueTypeCategory, ExpressionCompileError> {
        match self.type_stack.last() {
            Some(ExpressionValueType::List(value)) => Ok(*value),
            Some(ExpressionValueType::Single(_)) => Err(ExpressionCompileError::ExpectedListWasSingle),
            None => Err(ExpressionCompileError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn append_instruction(&mut self, op_code: ExpressionOpCode) {
        self.instructions.push(op_code)
    }
}

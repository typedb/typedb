/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use chrono::{NaiveDate, NaiveDateTime};
use encoding::value::{
    decimal_value::Decimal,
    duration_value::Duration,
    value::Value,
    value_type::{ValueType, ValueTypeCategory},
    ValueEncodable,
};

use crate::{
    expressions::{
        builtins::{
            list_operations,
            load_cast::{CastLeftLongToDouble, CastRightLongToDouble, LoadConstant, LoadVariable},
            unary::{MathAbsDouble, MathAbsLong, MathCeilDouble, MathFloorDouble, MathRoundDouble},
            BuiltInFunctionID,
        },
        evaluator::ExpressionEvaluationState,
        op_codes::ExpressionOpCode,
        todo__dissolve__builtins::ValueTypeTrait,
        ExpressionCompilationError, ExpressionEvaluationError,
    },
    pattern::expression::{
        BuiltInCall, Expression, ExpressionTree, ListConstructor, ListIndex, ListIndexRange, Operation, Operator,
    },
};

pub trait ExpressionInstruction: Sized {
    const OP_CODE: ExpressionOpCode;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError>;
}

pub trait SelfCompiling: ExpressionInstruction {
    fn return_value_category(&self) -> Option<ValueTypeCategory>;

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError>;
}

pub struct CompiledExpression {
    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<Value<'static>>,

    return_type: ValueTypeCategory,
}

impl CompiledExpression {
    pub(crate) fn instructions(&self) -> &Vec<ExpressionOpCode> {
        &self.instructions
    }

    pub fn variables(&self) -> &[Variable] {
        self.variable_stack.as_slice()
    }

    pub fn constants(&self) -> &[Value<'static>] {
        self.constant_stack.as_slice()
    }
}

impl CompiledExpression {
    pub(crate) fn return_type(&self) -> ValueTypeCategory {
        self.return_type
    }
}

pub struct ExpressionTreeCompiler<'this> {
    ir_tree: &'this ExpressionTree<Variable>,
    variable_value_categories: HashMap<Variable, ValueTypeCategory>,
    type_stack: Vec<ValueTypeCategory>,

    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<Value<'static>>,
}

impl<'this> ExpressionTreeCompiler<'this> {
    pub(crate) fn pop_type(&mut self) -> Result<ValueTypeCategory, ExpressionCompilationError> {
        match self.type_stack.pop() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompilationError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn push_type(&mut self, value: ValueTypeCategory) {
        self.type_stack.push(value);
    }

    fn peek_type(&self) -> Result<&ValueTypeCategory, ExpressionCompilationError> {
        match self.type_stack.last() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompilationError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn append_instruction(&mut self, op_code: ExpressionOpCode) {
        self.instructions.push(op_code)
    }
}

impl<'this> ExpressionTreeCompiler<'this> {
    fn new(
        ir_tree: &'this ExpressionTree<Variable>,
        variable_value_categories: HashMap<Variable, ValueTypeCategory>,
    ) -> Self {
        ExpressionTreeCompiler {
            ir_tree,
            variable_value_categories,
            instructions: Vec::new(),
            variable_stack: Vec::new(),
            constant_stack: Vec::new(),
            type_stack: Vec::new(),
        }
    }

    pub fn compile(
        ir_tree: &ExpressionTree<Variable>,
        variable_value_categories: HashMap<Variable, ValueTypeCategory>,
    ) -> Result<CompiledExpression, ExpressionCompilationError> {
        let mut builder = ExpressionTreeCompiler::new(ir_tree, variable_value_categories);
        match builder.compile_recursive(ir_tree.root()) {
            Ok(_) => {
                let return_type = builder.pop_type()?;
                let ExpressionTreeCompiler { instructions, variable_stack, constant_stack, .. } = builder;
                Ok(CompiledExpression { instructions, variable_stack, constant_stack, return_type })
            }
            Err(err) => {
                todo!("Handle error during expression compilation")
            }
        }
    }

    fn compile_recursive(&mut self, index: usize) -> Result<(), ExpressionCompilationError> {
        match &self.ir_tree.tree()[index] {
            Expression::Constant(constant) => self.compile_constant(constant),
            Expression::Variable(variable) => self.compile_variable(variable),
            Expression::Operation(op) => self.compile_op(op),
            Expression::BuiltInCall(builtin) => self.compile_builtin(builtin),
            Expression::ListIndex(list_index) => self.compile_list_index(list_index),
            Expression::List(list_constructor) => self.compile_list_constructor(list_constructor),
            Expression::ListIndexRange(list_index_range) => self.compile_list_index_range(list_index_range),
        }
    }

    fn compile_constant(&mut self, constant: &Value<'static>) -> Result<(), ExpressionCompilationError> {
        self.constant_stack.push(constant.clone());

        self.push_type(constant.value_type().category());
        self.append_instruction(LoadConstant::OP_CODE);

        Ok(())
    }

    fn compile_variable(&mut self, variable: &Variable) -> Result<(), ExpressionCompilationError> {
        debug_assert!(self.variable_value_categories.contains_key(variable));

        self.variable_stack.push(variable.clone());
        self.append_instruction(LoadVariable::OP_CODE);
        self.push_type(self.variable_value_categories.get(&variable).unwrap().clone());

        Ok(())
    }

    fn compile_list_constructor(
        &mut self,
        list_constructor: &ListConstructor,
    ) -> Result<(), ExpressionCompilationError> {
        for index in list_constructor.item_expression_indices.iter().rev() {
            self.compile_recursive(*index)?;
        }
        self.compile_constant(&Value::Long(list_constructor.item_expression_indices.len() as i64))?;
        self.append_instruction(list_operations::ListConstructor::OP_CODE);

        if self.pop_type()? != ValueTypeCategory::Long {
            Err(ExpressionCompilationError::InternalUnexpectedValueType)?;
        }
        let n_elements = list_constructor.item_expression_indices.len();
        if n_elements > 0 {
            let element_type = self.pop_type()?;
            for _ in 1..list_constructor.item_expression_indices.len() {
                if self.pop_type()? != element_type {
                    Err(ExpressionCompilationError::HeterogenousValuesInList)?;
                }
            }
            self.push_type(element_type)
        } else {
            todo!("I can't know the value type of empty lists"); // But do I need to?
        }

        Ok(())
    }

    fn compile_list_index(&mut self, list_index: &ListIndex<Variable>) -> Result<(), ExpressionCompilationError> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index.list_variable));

        self.compile_recursive(list_index.index_expression_index)?;
        self.compile_variable(&list_index.list_variable)?;
        self.append_instruction(list_operations::ListIndex::OP_CODE);

        let index_type = self.pop_type()?;
        if index_type != ValueTypeCategory::Long {
            Err(ExpressionCompilationError::ListIndexMustBeLong)?
        }
        let list_variable_type = self.pop_type()?;
        self.push_type(list_variable_type); // reuse
        Ok(())
    }

    fn compile_list_index_range(
        &mut self,
        list_index_range: &ListIndexRange<Variable>,
    ) -> Result<(), ExpressionCompilationError> {
        debug_assert!(self.variable_value_categories.contains_key(&list_index_range.list_variable));
        self.compile_recursive(list_index_range.from_expression_index)?;
        self.compile_recursive(list_index_range.to_expression_index)?;
        self.compile_variable(&list_index_range.list_variable)?;
        self.append_instruction(list_operations::ListIndexRange::OP_CODE);

        //
        let list_variable_type = self.pop_type()?;
        let from_index_type = self.pop_type()?;
        if from_index_type != ValueTypeCategory::Long {
            Err(ExpressionCompilationError::ListIndexMustBeLong)?
        }
        let to_index_type = self.pop_type()?;
        if to_index_type != ValueTypeCategory::Long {
            Err(ExpressionCompilationError::ListIndexMustBeLong)?
        }

        Ok(self.push_type(list_variable_type))
    }

    fn compile_op(&mut self, operation: &Operation) -> Result<(), ExpressionCompilationError> {
        let Operation { operator, left_expression_index, right_expression_index } = operation.clone();
        self.compile_recursive(operation.left_expression_index)?;
        let left_category = self.peek_type()?;
        match left_category {
            ValueTypeCategory::Boolean => self.compile_op_boolean(operator, right_expression_index),
            ValueTypeCategory::Long => self.compile_op_long(operator, right_expression_index),
            ValueTypeCategory::Double => self.compile_op_double(operator, right_expression_index),
            ValueTypeCategory::Decimal => self.compile_op_decimal(operator, right_expression_index),
            ValueTypeCategory::Date => self.compile_op_date(operator, right_expression_index),
            ValueTypeCategory::DateTime => self.compile_op_datetime(operator, right_expression_index),
            ValueTypeCategory::DateTimeTZ => self.compile_op_datetime_tz(operator, right_expression_index),
            ValueTypeCategory::Duration => self.compile_op_duration(operator, right_expression_index),
            ValueTypeCategory::String => self.compile_op_string(operator, right_expression_index),
            ValueTypeCategory::Struct => todo!(),
        }
    }

    fn compile_op_boolean(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_long(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type()?.clone();
        match right_category {
            ValueTypeCategory::Long => {
                self.compile_op_long_long(op)?;
            }
            ValueTypeCategory::Double => {
                // The left needs to be cast
                CastLeftLongToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            _ => Err(ExpressionCompilationError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Long,
                right_category,
            })?,
        }
        Ok(())
    }

    fn compile_op_double(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_type()?.clone();
        match right_category {
            ValueTypeCategory::Long => {
                // The right needs to be cast
                CastRightLongToDouble::validate_and_append(self)?;
                self.compile_op_double_double(op)?;
            }
            ValueTypeCategory::Double => {
                self.compile_op_double_double(op)?;
            }
            _ => Err(ExpressionCompilationError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Double,
                right_category,
            })?,
        }
        Ok(())
    }

    fn compile_op_string(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_duration(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_datetime_tz(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_datetime(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_date(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_decimal(&mut self, op: Operator, right: usize) -> Result<(), ExpressionCompilationError> {
        todo!()
    }

    // match operator {
    // Operator::Add => compile_op_add(left, right, instructions),
    // Operator::Subtract => compile_op_subtract(left, right, instructions),
    // Operator::Multiply => compile_op_multiply(left, right, instructions),
    // Operator::Divide => compile_op_divide(left, right, instructions),
    // Operator::Modulo => compile_op_modulo(left, right, instructions),
    // Operator::Power => compile_op_power(left, right, instructions),
    // }

    // Ops with Left, Right resolved
    fn compile_op_long_long(&mut self, op: Operator) -> Result<(), ExpressionCompilationError> {
        use crate::expressions::builtins::operators as ops;
        match op {
            Operator::Add => ops::OpLongAddLong::validate_and_append(self)?,
            Operator::Subtract => ops::OpLongSubtractLong::validate_and_append(self)?,
            Operator::Multiply => ops::OpLongMultiplyLong::validate_and_append(self)?,
            Operator::Divide => ops::OpLongDivideLong::validate_and_append(self)?,
            Operator::Modulo => ops::OpLongModuloLong::validate_and_append(self)?,
            Operator::Power => ops::OpLongPowerLong::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_op_double_double(&mut self, op: Operator) -> Result<(), ExpressionCompilationError> {
        use crate::expressions::builtins::operators as ops;
        match op {
            Operator::Add => ops::OpDoubleAddDouble::validate_and_append(self)?,
            Operator::Subtract => ops::OpDoubleSubtractDouble::validate_and_append(self)?,
            Operator::Multiply => ops::OpDoubleMultiplyDouble::validate_and_append(self)?,
            Operator::Divide => ops::OpDoubleDivideDouble::validate_and_append(self)?,
            Operator::Modulo => ops::OpDoubleModuloDouble::validate_and_append(self)?,
            Operator::Power => ops::OpDoublePowerDouble::validate_and_append(self)?,
        }
        Ok(())
    }

    fn compile_builtin(&mut self, builtin: &BuiltInCall) -> Result<(), ExpressionCompilationError> {
        match builtin.builtin_id {
            BuiltInFunctionID::Abs => {
                self.compile_recursive(builtin.args_index[0])?;
                match self.peek_type()? {
                    ValueTypeCategory::Long => MathAbsLong::validate_and_append(self)?,
                    ValueTypeCategory::Double => MathAbsDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => todo!(),
                    _ => Err(ExpressionCompilationError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Ceil => {
                self.compile_recursive(builtin.args_index[0])?;
                match self.peek_type()? {
                    ValueTypeCategory::Double => MathCeilDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => todo!(),
                    _ => Err(ExpressionCompilationError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Floor => {
                self.compile_recursive(builtin.args_index[0])?;
                match self.peek_type()? {
                    ValueTypeCategory::Double => MathFloorDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => todo!(),
                    _ => Err(ExpressionCompilationError::UnsupportedArgumentsForBuiltin)?,
                }
            }
            BuiltInFunctionID::Round => {
                self.compile_recursive(builtin.args_index[0])?;
                match self.peek_type()? {
                    ValueTypeCategory::Double => MathRoundDouble::validate_and_append(self)?,
                    ValueTypeCategory::Decimal => todo!(),
                    _ => Err(ExpressionCompilationError::UnsupportedArgumentsForBuiltin)?,
                }
            }
        }
        Ok(())
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::value::{value::Value, value_type::ValueTypeCategory, ValueEncodable};

use crate::{
    expressions::{
        builtins::{
            binary::{OpDoubleAddDouble, OpLongAddLong},
            load_cast::{CastLeftLongToDouble, CastRightLongToDouble, LoadConstant, LoadVariable},
        },
        evaluator::ExpressionEvaluationState,
        op_codes::ExpressionOpCode,
        todo__dissolve__builtins::ValueTypeTrait,
        ExpressionCompilationError, ExpressionEvaluationError,
    },
    inference::TypeInferenceError,
    pattern::expression::{Expression, ExpressionTree, Operation, Operator},
};

// Keep implementations 0 sized
pub trait ExpressionInstruction: Sized {
    const OP_CODE: ExpressionOpCode;
    fn evaluate<'a>(state: &mut ExpressionEvaluationState<'a>) -> Result<(), ExpressionEvaluationError>;
}

pub trait SelfCompiling: ExpressionInstruction {
    fn return_value_category(&self) -> Option<ValueTypeCategory>;

    fn validate_and_append(builder: &mut ExpressionTreeCompiler<'_>) -> Result<(), ExpressionCompilationError>;
}

pub struct CompiledExpressionTree {
    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<Value<'static>>,

    return_type: ValueTypeCategory,
}

impl CompiledExpressionTree {
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

impl CompiledExpressionTree {
    pub(crate) fn return_type(&self) -> ValueTypeCategory {
        self.return_type
    }
}

pub struct ExpressionTreeCompiler<'this> {
    ir_tree: &'this ExpressionTree,
    variable_value_categories: HashMap<Variable, ValueTypeCategory>,
    mock_stack: Vec<Value<'static>>, // TODO: Remove or use

    instructions: Vec<ExpressionOpCode>,
    variable_stack: Vec<Variable>,
    constant_stack: Vec<Value<'static>>,
}

impl<'this> ExpressionTreeCompiler<'this> {
    pub(crate) fn pop_mock(&mut self) -> Result<Value<'static>, ExpressionCompilationError> {
        match self.mock_stack.pop() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompilationError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn push_mock(&mut self, value: Value<'static>) {
        self.mock_stack.push(value);
    }

    fn peek_mock(&self) -> Result<&Value<'static>, ExpressionCompilationError> {
        match self.mock_stack.last() {
            Some(value) => Ok(value),
            None => Err(ExpressionCompilationError::InternalStackWasEmpty)?,
        }
    }

    pub(crate) fn append_instruction(&mut self, op_code: ExpressionOpCode) {
        self.instructions.push(op_code)
    }
}

impl<'this> ExpressionTreeCompiler<'this> {
    fn new(ir_tree: &'this ExpressionTree, variable_value_categories: HashMap<Variable, ValueTypeCategory>) -> Self {
        ExpressionTreeCompiler {
            ir_tree,
            variable_value_categories,
            instructions: Vec::new(),
            variable_stack: Vec::new(),
            constant_stack: Vec::new(),
            mock_stack: Vec::new(),
        }
    }

    pub fn compile(
        ir_tree: &ExpressionTree,
        variable_value_categories: HashMap<Variable, ValueTypeCategory>,
    ) -> Result<CompiledExpressionTree, ExpressionCompilationError> {
        let mut builder = ExpressionTreeCompiler::new(ir_tree, variable_value_categories);
        match builder.compile_recursive(ir_tree.root()) {
            Ok(_) => {
                let return_type = builder.pop_mock()?.value_type().category();
                let ExpressionTreeCompiler { mut mock_stack, instructions, variable_stack, constant_stack, .. } =
                    builder;
                Ok(CompiledExpressionTree { instructions, variable_stack, constant_stack, return_type })
            }
            Err(_) => todo!(),
        }
    }

    fn compile_recursive(&mut self, index: usize) -> Result<usize, ExpressionCompilationError> {
        match &self.ir_tree.tree()[index] {
            Expression::Constant(constant) => self.compile_constant(constant),
            Expression::Variable(variable) => self.compile_variable(variable),
            Expression::Operation(op) => self.compile_op(op),
            Expression::BuiltInCall(_) => todo!(),
            Expression::ListIndex(_) => todo!(),
            Expression::List(_) => todo!(),
            Expression::ListIndexRange(_) => todo!(),
        }
    }

    fn compile_constant(&mut self, constant: &Value<'static>) -> Result<usize, ExpressionCompilationError> {
        self.constant_stack.push(constant.clone());

        self.push_mock(Self::get_mock_value_for(constant.value_type().category().clone()));
        self.append_instruction(LoadConstant::OP_CODE);

        Ok(self.instructions.len() - 1)
    }

    fn compile_variable(&mut self, variable: &Variable) -> Result<usize, ExpressionCompilationError> {
        debug_assert!(self.variable_value_categories.contains_key(variable));

        self.variable_stack.push(variable.clone());
        self.append_instruction(LoadVariable::OP_CODE);
        self.push_mock(Self::get_mock_value_for(self.variable_value_categories.get(&variable).unwrap().clone()));

        Ok(self.instructions.len() - 1)
    }

    fn compile_op(&mut self, operation: &Operation) -> Result<usize, ExpressionCompilationError> {
        let Operation { operator, left_expression_index, right_expression_index } = operation.clone();
        self.compile_recursive(operation.left_expression_index)?;
        let left_category = self.peek_mock()?.value_type().category();
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

    fn compile_op_boolean(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_long(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_mock()?.value_type().category();
        match right_category {
            ValueTypeCategory::Long => {
                // No casting needed
                OpLongAddLong::validate_and_append(self)?;
            }
            ValueTypeCategory::Double => {
                // The left needs to be cast
                CastLeftLongToDouble::validate_and_append(self)?;
                OpDoubleAddDouble::validate_and_append(self)?;
            }
            _ => Err(ExpressionCompilationError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Long,
                right_category,
            })?,
        }
        Ok(self.instructions.len() - 1)
    }

    fn compile_op_double(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        self.compile_recursive(right)?;
        let right_category = self.peek_mock()?.value_type().category();
        match right_category {
            ValueTypeCategory::Long => {
                // The right needs to be cast
                CastRightLongToDouble::validate_and_append(self)?;
                OpDoubleAddDouble::validate_and_append(self)?;
            }
            ValueTypeCategory::Double => {
                // No casting needed
                OpDoubleAddDouble::validate_and_append(self)?;
            }
            _ => Err(ExpressionCompilationError::UnsupportedOperandsForOperation {
                op,
                left_category: ValueTypeCategory::Double,
                right_category,
            })?,
        }
        Ok(self.instructions.len() - 1)
    }

    fn compile_op_string(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_duration(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_datetime_tz(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_datetime(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_date(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
        todo!()
    }

    fn compile_op_decimal(&mut self, op: Operator, right: usize) -> Result<usize, ExpressionCompilationError> {
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
    fn get_mock_value_for(category: ValueTypeCategory) -> Value<'static> {
        debug_assert!(category != ValueTypeCategory::Struct);
        match category {
            // ValueTypeCategory::Boolean => <bool as ValueTypeTrait>::MOCK_VALUE.clone(),
            ValueTypeCategory::Long => <i64 as ValueTypeTrait>::MOCK_VALUE.clone(),
            ValueTypeCategory::Double => <f64 as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::Decimal => <Decimal as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::Date => <NaiveDate as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::DateTime => <NaiveDateTime as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::DateTimeTZ => <DateTime<Tz> as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::Duration => <Duration as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::String => <String as ValueTypeTrait>::MOCK_VALUE.clone(),
            // ValueTypeCategory::Struct => unreachable!(),
            _ => todo!(),
        }
    }
}

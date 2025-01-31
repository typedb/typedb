/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use compiler::annotation::expression::{
    compiled_expression::{ExecutableExpression, ExpressionValueType},
    expression_compiler::ExpressionCompilationContext,
    ExpressionCompileError,
};
use encoding::value::{value::Value, value_type::ValueTypeCategory};
use executor::read::expression_executor::{evaluate_expression, ExpressionValue};
use ir::{
    pattern::{constraint::Constraint, variable_category::VariableCategory},
    pipeline::{function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
    translation::{match_::translate_match, TranslationContext},
    RepresentationError,
};
use itertools::Itertools;
use typeql::query::stage::Stage;

#[derive(Debug)]
pub enum PatternDefitionOrExpressionCompileError {
    PatternDefinition { source: Box<RepresentationError> },
    ExpressionCompilation { source: Box<ExpressionCompileError> },
}

impl From<Box<RepresentationError>> for PatternDefitionOrExpressionCompileError {
    fn from(value: Box<RepresentationError>) -> Self {
        Self::PatternDefinition { source: value }
    }
}

impl From<Box<ExpressionCompileError>> for PatternDefitionOrExpressionCompileError {
    fn from(value: Box<ExpressionCompileError>) -> Self {
        Self::ExpressionCompilation { source: value }
    }
}

fn compile_expression_via_match(
    s: &str,
    variable_types: HashMap<&str, ExpressionValueType>,
) -> Result<
    (HashMap<String, Variable>, ExecutableExpression<Variable>, ParameterRegistry),
    PatternDefitionOrExpressionCompileError,
> {
    let query = format!("match let $x = {}; select $x;", s);
    // Avoid unbound variable errors
    let input_variable_categories =
        variable_types.iter().map(|(name, _)| ((*name).to_owned(), None, VariableCategory::Value)).collect();
    let (mut translation_context, _) = TranslationContext::new_with_function_arguments(input_variable_categories);
    let mut value_parameters = ParameterRegistry::new();
    if let Stage::Match(match_) = typeql::parse_query(query.as_str()).unwrap().into_pipeline().stages.first().unwrap() {
        let block = translate_match(
            &mut translation_context,
            &mut value_parameters,
            &HashMapFunctionSignatureIndex::empty(),
            match_,
        )?
        .finish()?;
        let variable_mapping = variable_types
            .keys()
            .map(|&name| (name.to_owned(), translation_context.get_variable(name).unwrap()))
            .collect::<HashMap<_, _>>();
        let variable_types_mapped = variable_types
            .into_iter()
            .map(|(name, type_)| (translation_context.get_variable(name).unwrap(), type_))
            .collect::<HashMap<_, _>>();

        let expression_binding = match &block.conjunction().constraints()[0] {
            Constraint::ExpressionBinding(binding) => binding,
            _ => unreachable!(),
        };
        let compiled = ExpressionCompilationContext::compile(
            expression_binding.expression(),
            &variable_types_mapped,
            &value_parameters,
        )?;
        Ok((variable_mapping, compiled, value_parameters))
    } else {
        unreachable!();
    }
}

macro_rules! as_value {
    ($actual:expr) => {
        match $actual {
            ExpressionValue::Single(value) => value,
            _ => panic!("Called as_value on an expression that was a list"),
        }
    };
}

macro_rules! as_list {
    ($actual:expr) => {
        match $actual {
            ExpressionValue::List(list) => list,
            _ => panic!("Called as_value on an expression that was a list"),
        }
    };
}

#[test]
fn test_basic() {
    {
        let (_, expr, params) = compile_expression_via_match("3 - 5", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(-2));
    }

    {
        let (_, expr, params) = compile_expression_via_match("7.0e0 + 9.0e0", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Double(16.0));
    }

    {
        let (vars, expr, params) = compile_expression_via_match(
            "$a + $b",
            HashMap::from([
                ("a", ExpressionValueType::Single(ValueTypeCategory::Integer.try_into_value_type().unwrap())),
                ("b", ExpressionValueType::Single(ValueTypeCategory::Integer.try_into_value_type().unwrap())),
            ]),
        )
        .unwrap();
        let [a, b] = ["a", "b"].map(|name| *vars.get(name).unwrap());

        let inputs = HashMap::from([
            (a, ExpressionValue::Single(Value::Integer(2))),
            (b, ExpressionValue::Single(Value::Integer(5))),
        ]);
        let result = evaluate_expression(&expr, inputs, &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(7));
    }
}

#[test]
fn test_ops_integer_double() {
    // Integer ops
    {
        {
            let (_, expr, params) = compile_expression_via_match("12 + 4", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Integer(16));
        }
        {
            let (_, expr, params) = compile_expression_via_match("12 - 4", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Integer(8));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12 * 4", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Integer(48));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12 / 4", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(3.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12 % 5", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Integer(2));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12 ^ 4", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(f64::powf(12.0, 4.0)));
        }
    }

    // Double ops
    {
        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 + 4.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(16.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 - 4.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(8.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 * 4.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(48.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 / 4.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(3.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 % 5.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(2.0));
        }

        {
            let (_, expr, params) = compile_expression_via_match("12.0e0 ^ 4.0e0", HashMap::new()).unwrap();
            let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
            assert_eq!(as_value!(result), Value::Double(f64::powf(12.0, 4.0)));
        }
    }

    // Integer-double cast ops
    {
        let (_, expr, params) = compile_expression_via_match("12.0e0 + 4", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Double(16.0));
    }

    {
        let (_, expr, params) = compile_expression_via_match("12 + 4.0e0", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Double(16.0));
    }
}

#[test]
fn test_functions() {
    {
        let (_, expr, params) = compile_expression_via_match("floor(2.5e0)", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(2));
    }

    {
        let (_, expr, params) = compile_expression_via_match("ceil(2.5e0)", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(3));
    }

    {
        let (_, expr, params) = compile_expression_via_match("round(2.5e0)", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(2));
    }

    {
        let (_, expr, params) = compile_expression_via_match("round(3.5e0)", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(4));
    }

    let err = compile_expression_via_match("round(3.5e0, 4.5e0)", HashMap::new()).unwrap_err();
    let PatternDefitionOrExpressionCompileError::PatternDefinition { source } = err else {
        panic!("wrong error type");
    };
    assert!(matches!(*source, RepresentationError::ExpressionBuiltinArgumentCountMismatch { .. }));
}

#[test]
fn list_ops() {
    {
        let (_, expr, params) = compile_expression_via_match("[12,34]", HashMap::new()).unwrap();
        let result = evaluate_expression(&expr, HashMap::new(), &params).unwrap();
        assert_eq!(&*as_list!(result), &[Value::Integer(12), Value::Integer(34)]);
    }

    {
        let (vars, expr, params) = compile_expression_via_match(
            "$y[1]",
            HashMap::from([(
                "y",
                ExpressionValueType::List(ValueTypeCategory::Integer.try_into_value_type().unwrap()),
            )]),
        )
        .unwrap();
        let y = ["y"].into_iter().map(|name| *vars.get(name).unwrap()).exactly_one().unwrap();

        let inputs = HashMap::from([(
            y,
            ExpressionValue::List([Value::Integer(56), Value::Integer(78), Value::Integer(90)].into()),
        )]);
        let result = evaluate_expression(&expr, inputs, &params).unwrap();
        assert_eq!(as_value!(result), Value::Integer(78));
    }

    {
        let (vars, expr, params) = compile_expression_via_match(
            "$y[1..3]",
            HashMap::from([(
                "y",
                ExpressionValueType::List(ValueTypeCategory::Integer.try_into_value_type().unwrap()),
            )]),
        )
        .unwrap();
        let y = ["y"].into_iter().map(|name| *vars.get(name).unwrap()).exactly_one().unwrap();

        let inputs = HashMap::from([(
            y,
            ExpressionValue::List(
                [Value::Integer(9), Value::Integer(87), Value::Integer(65), Value::Integer(43)].into(),
            ),
        )]);
        let result = evaluate_expression(&expr, inputs, &params).unwrap();
        assert_eq!(&*as_list!(result), &[Value::Integer(87), Value::Integer(65)]);
    }
}

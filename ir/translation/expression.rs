/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use typeql::{
    common::token::{ArithmeticOperator, Function},
    expression::{BuiltinFunctionName, Expression as TypeQLExpression, FunctionName},
};

use crate::{
    pattern::{
        constraint::ConstraintsBuilder,
        expression::{
            BuiltInCall, BuiltInFunctionID, Expression, ExpressionTree, ExpressionTreeNodeId, ListConstructor,
            ListIndex, ListIndexRange, Operation, Operator,
        },
    },
    program::function_signature::FunctionSignatureIndex,
    translation::{
        constraints::{add_function_call_binding_user, register_typeql_var, split_out_inline_expressions},
        literal::translate_literal,
    },
    PatternDefinitionError,
};

pub(crate) fn build_expression<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    expression: &TypeQLExpression,
) -> Result<ExpressionTree<Variable>, PatternDefinitionError> {
    let mut tree = ExpressionTree::empty();
    build_recursive(function_index, constraints, expression, &mut tree)?;
    Ok(tree)
}

fn build_recursive<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    expression: &TypeQLExpression,
    tree: &mut ExpressionTree<Variable>,
) -> Result<ExpressionTreeNodeId, PatternDefinitionError> {
    let expression = match expression {
        TypeQLExpression::Paren(inner) => {
            return build_recursive(function_index, constraints, &inner.inner, tree);
        }
        TypeQLExpression::Variable(var) => Expression::Variable(register_typeql_var(constraints, var)?),
        TypeQLExpression::ListIndex(list_index) => {
            let variable = register_typeql_var(constraints, &list_index.variable)?;
            let id = build_recursive(function_index, constraints, &list_index.index, tree)?;
            Expression::ListIndex(ListIndex::new(variable, id))
        }
        TypeQLExpression::Value(literal) => {
            let value = translate_literal(literal)
                .map_err(|source| PatternDefinitionError::LiteralParseError { literal: literal.to_string(), source })?;
            Expression::Constant(value)
        }
        TypeQLExpression::Operation(operation) => {
            let left_id = build_recursive(function_index, constraints, &operation.left, tree)?;
            let right_id = build_recursive(function_index, constraints, &operation.right, tree)?;
            Expression::Operation(Operation::new(translate_operator(&operation.op), left_id, right_id))
        }
        TypeQLExpression::Function(function_call) => build_function(function_index, constraints, function_call, tree)?, // Careful, could be either.
        TypeQLExpression::List(list) => {
            let sub_exprs = list
                .items
                .iter()
                .map(|sub_expr| build_recursive(function_index, constraints, sub_expr, tree))
                .collect::<Result<Vec<_>, _>>()?;
            Expression::List(ListConstructor::new(sub_exprs))
        }
        TypeQLExpression::ListIndexRange(range) => {
            let list_variable = register_typeql_var(constraints, &range.var)?;
            let left_id = build_recursive(function_index, constraints, &range.from, tree)?;
            let right_id = build_recursive(function_index, constraints, &range.to, tree)?;
            Expression::ListIndexRange(ListIndexRange::new(list_variable, left_id, right_id))
        }
    };
    Ok(tree.add(expression))
}

fn build_function<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    function_call: &typeql::expression::FunctionCall,
    tree: &mut ExpressionTree<Variable>,
) -> Result<Expression<Variable>, PatternDefinitionError> {
    match &function_call.name {
        FunctionName::Builtin(builtin) => {
            let args = function_call
                .args
                .iter()
                .map(|expr| build_recursive(function_index, constraints, expr, tree))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expression::BuiltInCall(BuiltInCall::new(to_builtin_id(builtin, &args)?, args)))
        }
        FunctionName::Identifier(identifier) => {
            let assign = constraints.create_anonymous_variable()?;
            let arguments = split_out_inline_expressions(function_index, constraints, &function_call.args)?;
            add_function_call_binding_user(
                function_index,
                constraints,
                vec![assign],
                identifier.as_str(),
                arguments,
                false,
            )?;
            Ok(Expression::Variable(assign))
        }
    }
}

fn translate_operator(operator: &ArithmeticOperator) -> Operator {
    match operator {
        ArithmeticOperator::Add => Operator::Add,
        ArithmeticOperator::Subtract => Operator::Subtract,
        ArithmeticOperator::Multiply => Operator::Multiply,
        ArithmeticOperator::Divide => Operator::Divide,
        ArithmeticOperator::Modulo => Operator::Modulo,
        ArithmeticOperator::Power => Operator::Power,
    }
}

fn check_builtin_arg_count(
    builtin: &BuiltinFunctionName,
    actual: usize,
    expected: usize,
) -> Result<(), PatternDefinitionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(PatternDefinitionError::ExpressionBuiltinArgumentCountMismatch {
            builtin: builtin.token.clone(),
            expected,
            actual,
        })
    }
}

fn to_builtin_id(
    typeql_id: &BuiltinFunctionName,
    args: &Vec<usize>,
) -> Result<BuiltInFunctionID, PatternDefinitionError> {
    Ok(match typeql_id.token {
        Function::Abs => {
            check_builtin_arg_count(typeql_id, args.len(), 1)?;
            BuiltInFunctionID::Abs
        }
        Function::Ceil => {
            check_builtin_arg_count(typeql_id, args.len(), 1)?;
            BuiltInFunctionID::Ceil
        }
        Function::Floor => {
            check_builtin_arg_count(typeql_id, args.len(), 1)?;
            BuiltInFunctionID::Floor
        }
        Function::Round => {
            check_builtin_arg_count(typeql_id, args.len(), 1)?;
            BuiltInFunctionID::Round
        }
        _ => todo!(),
    })
}

#[cfg(test)]
pub mod tests {
    use answer::variable::Variable;
    use encoding::value::value::Value;
    use itertools::Itertools;

    use crate::{
        pattern::expression::{Expression, Operation, Operator},
        program::{
            block::{BlockContext, FunctionalBlock},
            function_signature::HashMapFunctionSignatureIndex,
        },
        translation::{match_::translate_match, TranslationContext},
        PatternDefinitionError,
    };

    fn parse_query_get_match(
        context: &mut TranslationContext,
        query_str: &str,
    ) -> Result<FunctionalBlock, PatternDefinitionError> {
        let mut query = typeql::parse_query(query_str).unwrap().into_pipeline();
        let match_ = query.stages.remove(0).into_match();
        translate_match(context, &HashMapFunctionSignatureIndex::empty(), &match_).map(|builder| builder.finish())
    }

    #[test]
    fn basic() {
        let mut context = TranslationContext::new();
        let block = parse_query_get_match(
            &mut context,
            "
            match
                $y = 5 + 9 * 6;
            select $y;
        ",
        )
        .unwrap();
        let var_y = get_named_variable(&context, "y");

        let lhs = block.conjunction().constraints()[0].as_expression_binding().unwrap().left();
        let rhs = block.conjunction().constraints()[0]
            .as_expression_binding()
            .unwrap()
            .expression()
            .expression_tree_preorder()
            .cloned()
            .collect_vec();
        assert_eq!(lhs, var_y);
        assert_eq!(
            rhs,
            vec![
                Expression::Constant(Value::Long(5)),
                Expression::Constant(Value::Long(9)),
                Expression::Constant(Value::Long(6)),
                Expression::Operation(Operation::new(Operator::Multiply, 1, 2)),
                Expression::Operation(Operation::new(Operator::Add, 0, 3)),
            ]
        );
    }

    fn get_named_variable(translation_context: &TranslationContext, name: &str) -> Variable {
        translation_context
            .variable_registry
            .get_variables_named()
            .iter()
            .find(|(k, v)| v.as_str() == name)
            .unwrap()
            .0
            .clone()
    }
}

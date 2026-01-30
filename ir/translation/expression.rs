/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::value::Value;
use typeql::{
    common::{Span, Spanned},
    expression::{BuiltinFunctionName, FunctionName},
    token::{ArithmeticOperator, Function},
};

use crate::{
    pattern::{
        constraint::ConstraintsBuilder,
        expression::{
            BuiltinConceptFunctionID, BuiltinValueFunctionCall, BuiltinValueFunctionID, Expression, ExpressionTree,
            ExpressionTreeNodeId, ListConstructor, ListIndex, ListIndexRange, Operation, Operator,
        },
        ParameterID, Vertex,
    },
    pipeline::function_signature::FunctionSignatureIndex,
    translation::{
        constraints::{register_typeql_var, split_out_inline_expressions},
        literal::translate_literal,
        tokens::checked_identifier,
    },
    RepresentationError,
};

pub(super) fn add_typeql_expression(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    rhs: &typeql::Expression,
) -> Result<Vertex<Variable>, Box<RepresentationError>> {
    if let typeql::Expression::Value(literal) = rhs {
        let id = register_typeql_literal(constraints, literal)?;
        Ok(Vertex::Parameter(id))
    } else if let typeql::Expression::Variable(var) = rhs {
        Ok(Vertex::Variable(register_typeql_var(constraints, var)?))
    } else {
        let expression = build_expression(function_index, constraints, rhs)?;
        let variable = constraints.create_anonymous_variable(rhs.span())?;
        constraints.add_assignment(variable, expression, rhs.span())?;
        Ok(Vertex::Variable(variable))
    }
}

pub(crate) fn build_expression(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expression: &typeql::Expression,
) -> Result<ExpressionTree<Variable>, Box<RepresentationError>> {
    let mut tree = ExpressionTree::empty();
    build_recursive(function_index, constraints, expression, &mut tree)?;
    Ok(tree)
}

fn build_recursive(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expression: &typeql::Expression,
    tree: &mut ExpressionTree<Variable>,
) -> Result<ExpressionTreeNodeId, Box<RepresentationError>> {
    let expression = match expression {
        typeql::Expression::Paren(inner) => {
            return build_recursive(function_index, constraints, &inner.inner, tree);
        }
        typeql::Expression::Variable(var) => Expression::Variable(register_typeql_var(constraints, var)?),
        typeql::Expression::ListIndex(list_index) => {
            let variable = register_typeql_var(constraints, &list_index.variable)?;
            let id = build_recursive(function_index, constraints, &list_index.index, tree)?;
            Expression::ListIndex(ListIndex::new(variable, id, list_index.span()))
        }
        typeql::Expression::Value(literal) => {
            let id = register_typeql_literal(constraints, literal)?;
            Expression::Constant(id)
        }
        typeql::Expression::Operation(operation) => {
            let left_id = build_recursive(function_index, constraints, &operation.left, tree)?;
            let right_id = build_recursive(function_index, constraints, &operation.right, tree)?;
            Expression::Operation(Operation::new(
                translate_operator(&operation.op),
                left_id,
                right_id,
                operation.span(),
            ))
        }
        typeql::Expression::Function(function_call) => {
            build_function(function_index, constraints, function_call, tree)?
        } // Careful, could be either.
        typeql::Expression::List(list) => {
            let items = list
                .items
                .iter()
                .map(|sub_expr| build_recursive(function_index, constraints, sub_expr, tree))
                .collect::<Result<Vec<_>, _>>()?;
            let len_id = constraints.parameters().register_value(
                Value::Integer(items.len() as i64),
                list.span().expect("Parser did not provide List text range"),
            );
            Expression::List(ListConstructor::new(items, len_id, list.span()))
        }
        typeql::Expression::ListIndexRange(range) => {
            let list_variable = register_typeql_var(constraints, &range.var)?;
            let left_id = build_recursive(function_index, constraints, &range.from, tree)?;
            let right_id = build_recursive(function_index, constraints, &range.to, tree)?;
            Expression::ListIndexRange(ListIndexRange::new(list_variable, left_id, right_id, range.span()))
        }
    };
    Ok(tree.add(expression))
}

fn register_typeql_literal(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    literal: &typeql::Literal,
) -> Result<ParameterID, Box<RepresentationError>> {
    let value = translate_literal(literal).map_err(|typedb_source| RepresentationError::LiteralParseError {
        literal: literal.to_string(),
        source_span: literal.span(),
        typedb_source,
    })?;
    let id = constraints
        .parameters()
        .register_value(value, literal.span().expect("Parser did not provide Value text range"));
    Ok(id)
}

fn add_builtin_function_call(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    builtin_id: BuiltinConceptFunctionID,
    assigned: Vec<Variable>,
    args: &[typeql::Expression],
    source_span: Option<Span>,
) -> Result<(), Box<RepresentationError>> {
    let arguments = split_out_inline_expressions(function_index, constraints, args)?;
    constraints.add_builtin_function_binding(assigned, builtin_id, arguments, source_span)?;
    Ok(())
}

pub fn add_user_defined_function_call(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    function_name: &str,
    assigned: Vec<Variable>,
    args: &[typeql::Expression],
    source_span: Option<Span>,
) -> Result<(), Box<RepresentationError>> {
    let arguments = split_out_inline_expressions(function_index, constraints, args)?;
    let callee = function_index
        .get_function_signature(function_name)
        .map_err(|typedb_source| RepresentationError::FunctionReadError { typedb_source })?;
    let Some(callee) = callee else {
        return Err(Box::new(RepresentationError::UnresolvedFunction {
            function_name: function_name.to_owned(),
            source_span,
        }));
    };
    constraints.add_function_binding(assigned, &callee, arguments, function_name, source_span)?;
    Ok(())
}

fn build_function(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    function_call: &typeql::expression::FunctionCall,
    tree: &mut ExpressionTree<Variable>,
) -> Result<Expression<Variable>, Box<RepresentationError>> {
    match &function_call.name {
        FunctionName::Builtin(builtin) if is_builtin_value_function(builtin) => {
            let args = function_call
                .args
                .iter()
                .map(|expr| build_recursive(function_index, constraints, expr, tree))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expression::BuiltinValueFunctionCall(BuiltinValueFunctionCall::new(
                to_builtin_value_function_id(builtin, &args)?,
                args,
                builtin.span(),
            )))
        }
        FunctionName::Builtin(builtin) => {
            let assign = constraints.create_anonymous_variable(function_call.name.span())?;
            add_builtin_function_call(
                function_index,
                constraints,
                to_builtin_concept_function_id(builtin, &function_call.args)?,
                vec![assign],
                &function_call.args,
                function_call.span(),
            )?;
            Ok(Expression::Variable(assign))
        }
        FunctionName::Identifier(identifier) => {
            let assign = constraints.create_anonymous_variable(identifier.span())?;
            add_user_defined_function_call(
                function_index,
                constraints,
                checked_identifier(identifier)?,
                vec![assign],
                &function_call.args,
                function_call.span(),
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
    builtin: Function,
    actual: usize,
    expected: usize,
    source_span: Option<Span>,
) -> Result<(), Box<RepresentationError>> {
    if actual == expected {
        Ok(())
    } else {
        Err(Box::new(RepresentationError::ExpressionBuiltinArgumentCountMismatch {
            builtin,
            expected,
            actual,
            source_span,
        }))
    }
}

fn is_builtin_value_function(typeql_id: &BuiltinFunctionName) -> bool {
    matches!(
        typeql_id.token,
        Function::Abs
            | Function::Ceil
            | Function::Floor
            | Function::Round
            | Function::Max
            | Function::Min
            | Function::Len
    )
}

fn to_builtin_value_function_id(
    typeql_id: &BuiltinFunctionName,
    args: &[usize],
) -> Result<BuiltinValueFunctionID, Box<RepresentationError>> {
    let token = typeql_id.token;
    match token {
        Function::Abs => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Abs)
        }
        Function::Ceil => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Ceil)
        }
        Function::Floor => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Floor)
        }
        Function::Round => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Round)
        }
        Function::Max => {
            check_builtin_arg_count(token, args.len(), 2, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Max)
        }
        Function::Min => {
            check_builtin_arg_count(token, args.len(), 2, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Min)
        }
        Function::Len => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinValueFunctionID::Len)
        }
        _ => Err(Box::new(RepresentationError::InternalNotAValueBuiltin { token, source_span: typeql_id.span() })),
    }
}

fn to_builtin_concept_function_id<T>(
    typeql_id: &BuiltinFunctionName,
    args: &[T],
) -> Result<BuiltinConceptFunctionID, Box<RepresentationError>> {
    let token = typeql_id.token;
    match token {
        Function::Iid => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinConceptFunctionID::Iid)
        }
        Function::Label => {
            check_builtin_arg_count(token, args.len(), 1, typeql_id.span())?;
            Ok(BuiltinConceptFunctionID::Label)
        }
        _ => Err(Box::new(RepresentationError::InternalNotAConceptBuiltin { token, source_span: typeql_id.span() })),
    }
}

#[cfg(test)]
pub mod tests {
    use answer::variable::Variable;
    use encoding::value::value::Value;
    use itertools::Itertools;

    use crate::{
        pattern::{
            expression::{Expression, Operation, Operator},
            Vertex,
        },
        pipeline::{block::Block, function_signature::HashMapFunctionSignatureIndex, ParameterRegistry},
        translation::{match_::translate_match, PipelineTranslationContext},
        RepresentationError,
    };

    fn parse_query_get_match(
        context: &mut PipelineTranslationContext,
        value_parameters: &mut ParameterRegistry,
        query_str: &str,
    ) -> Result<Block, Box<RepresentationError>> {
        let mut query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
        let match_ = query.stages.remove(0).into_match();
        translate_match(context, value_parameters, &HashMapFunctionSignatureIndex::empty(), &match_)
            .and_then(|builder| builder.finish())
    }

    #[test]
    fn basic() {
        let mut context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let block =
            parse_query_get_match(&mut context, &mut value_parameters, "match let $y = 5 + 9 * 6; select $y;").unwrap();
        let var_y = get_named_variable(&context, "y");

        let lhs = block.conjunction().constraints()[0].as_expression_binding().unwrap().left();
        let rhs = block.conjunction().constraints()[0]
            .as_expression_binding()
            .unwrap()
            .expression()
            .expression_tree_preorder()
            .cloned()
            .collect_vec();
        assert_eq!(lhs, &Vertex::Variable(var_y));

        assert_eq!(rhs.len(), 5);
        let Expression::Constant(id) = &rhs[0] else { panic!("Expected Constant, found: {:?}", rhs[0]) };
        assert_eq!(value_parameters.value(id), Some(&Value::Integer(5)));
        let Expression::Constant(id) = &rhs[1] else { panic!("Expected Constant, found: {:?}", rhs[1]) };
        assert_eq!(value_parameters.value(id), Some(&Value::Integer(9)));
        let Expression::Constant(id) = &rhs[2] else { panic!("Expected Constant, found: {:?}", rhs[2]) };
        assert_eq!(value_parameters.value(id), Some(&Value::Integer(6)));
        assert_eq!(rhs[3], Expression::Operation(Operation::new(Operator::Multiply, 1, 2, None)));
        assert_eq!(rhs[4], Expression::Operation(Operation::new(Operator::Add, 0, 3, None)));
    }

    fn get_named_variable(translation_context: &PipelineTranslationContext, name: &str) -> Variable {
        *translation_context.variable_registry.variable_names().iter().find(|(k, v)| v.as_str() == name).unwrap().0
    }
}

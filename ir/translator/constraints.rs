/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::{
    expression::{Expression as TypeQLExpression, FunctionName},
    statement::AssignmentPattern,
    type_::NamedType,
};

use answer::variable::Variable;

use crate::{
    pattern::constraint::{ConstraintsBuilder, IsaKind},
    PatternDefinitionError,
    program::function_signature::FunctionSignatureIndex,
    translator::expression::build_expression,
};

pub(super) fn add_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    stmt: &typeql::Statement,
) -> Result<(), PatternDefinitionError> {
    match stmt {
        typeql::Statement::Is(_) => todo!(),
        typeql::Statement::InStream(in_stream) => add_in_iterable_statement(function_index, constraints, in_stream)?,
        typeql::Statement::Comparison(_) => todo!(),
        typeql::Statement::Assignment(assignment) => {
            add_typeql_assignment(function_index, constraints, &assignment.lhs, &assignment.rhs)?
        }
        typeql::Statement::Thing(thing) => add_thing_statement(constraints, thing)?,
        typeql::Statement::AttributeValue(_) => todo!(),
        typeql::Statement::AttributeComparison(_) => todo!(),
        typeql::Statement::Type(type_) => add_type_statement(constraints, type_)?,
    }
    Ok(())
}

fn add_thing_statement(
    constraints: &mut ConstraintsBuilder<'_>,
    thing: &typeql::statement::Thing,
) -> Result<(), PatternDefinitionError> {
    let var = match &thing.head {
        typeql::statement::thing::Head::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::Head::Relation(rel) => {
            let relation = constraints.create_anonymous_variable()?;
            add_typeql_relation(constraints, relation, rel)?;
            relation
        }
    };
    for constraint in &thing.constraints {
        match constraint {
            typeql::statement::thing::Constraint::Isa(isa) => add_typeql_isa(constraints, var, isa)?,
            typeql::statement::thing::Constraint::Iid(_) => todo!(),
            typeql::statement::thing::Constraint::Has(has) => add_typeql_has(constraints, var, has)?,
            typeql::statement::thing::Constraint::Links(links) => {
                add_typeql_relation(constraints, var, &links.relation)?
            }
        }
    }
    Ok(())
}

fn add_type_statement(
    constraints: &mut ConstraintsBuilder<'_>,
    type_: &typeql::statement::Type,
) -> Result<(), PatternDefinitionError> {
    let var = register_typeql_type_var_any(constraints, &type_.type_)?;
    for constraint in &type_.constraints {
        assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
        match &constraint.base {
            typeql::statement::type_::ConstraintBase::Sub(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Label(label) => match label {
                typeql::statement::type_::LabelConstraint::Name(label) => {
                    constraints.add_label(var, label.ident.as_str())?;
                }
                typeql::statement::type_::LabelConstraint::Scoped(_) => todo!(),
            },
            typeql::statement::type_::ConstraintBase::ValueType(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Owns(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Relates(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Plays(_) => todo!(),
        }
    }
    Ok(())
}

fn extend_from_inline_typeql_expression(
    constraints: &mut ConstraintsBuilder<'_>,
    expression: &TypeQLExpression,
) -> Result<Variable, PatternDefinitionError> {
    let var = match expression {
        TypeQLExpression::Variable(typeql_var) => register_typeql_var(constraints, &typeql_var)?,
        _ => todo!(),
    };
    Ok(var)
}

pub(crate) fn register_typeql_var(
    constraints: &mut ConstraintsBuilder<'_>,
    var: &typeql::Variable,
) -> Result<Variable, PatternDefinitionError> {
    match var {
        typeql::Variable::Named { ident, optional, .. } => {
            let var = constraints.get_or_declare_variable(ident.as_str())?;
            Ok(var)
        }
        typeql::Variable::Anonymous { optional, .. } => {
            let var = constraints.create_anonymous_variable()?;
            Ok(var)
        }
    }
}

fn register_typeql_type_var_any(
    constraints: &mut ConstraintsBuilder<'_>,
    type_: &typeql::TypeRefAny,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeRefAny::Type(type_) => register_typeql_type_var(constraints, type_),
        typeql::TypeRefAny::Optional(_) => todo!(),
        typeql::TypeRefAny::List(_) => todo!(),
    }
}

fn register_typeql_type_var(
    constraints: &mut ConstraintsBuilder<'_>,
    type_: &typeql::TypeRef,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeRef::Named(NamedType::Label(label)) => register_type_label_var(constraints, label),
        typeql::TypeRef::Named(NamedType::Role(scoped_label)) => todo!(),
        typeql::TypeRef::Named(NamedType::BuiltinValueType(builtin)) => todo!(),
        typeql::TypeRef::Variable(var) => register_typeql_var(constraints, var),
    }
}

fn register_type_label_var(
    constraints: &mut ConstraintsBuilder<'_>,
    label: &typeql::Label,
) -> Result<Variable, PatternDefinitionError> {
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_label(variable, label.ident.as_str())?;
    Ok(variable)
}

fn add_typeql_isa(
    constraints: &mut ConstraintsBuilder<'_>,
    thing: Variable,
    isa: &typeql::statement::thing::isa::Isa,
) -> Result<(), PatternDefinitionError> {
    let kind = match isa.kind {
        typeql::statement::thing::isa::IsaKind::Exact => IsaKind::Exact,
        typeql::statement::thing::isa::IsaKind::Subtype => IsaKind::Subtype,
    };
    let type_ = register_typeql_type_var(constraints, &isa.type_)?;
    constraints.add_isa(kind, thing, type_)?;
    Ok(())
}

fn add_typeql_has(
    constraints: &mut ConstraintsBuilder<'_>,
    owner: Variable,
    has: &typeql::statement::thing::Has,
) -> Result<(), PatternDefinitionError> {
    let attr = match &has.value {
        typeql::statement::thing::HasValue::Variable(var) => var,
        typeql::statement::thing::HasValue::Expression(_) => todo!(),
        typeql::statement::thing::HasValue::Comparison(_) => todo!(),
    };
    let attribute = register_typeql_var(constraints, attr)?;
    constraints.add_has(owner, attribute)?;
    if let Some(type_) = &has.type_ {
        let attribute_type = register_typeql_type_var_any(constraints, type_)?;
        constraints.add_isa(IsaKind::Subtype, attribute, attribute_type)?;
    }
    Ok(())
}

fn add_typeql_relation(
    constraints: &mut ConstraintsBuilder<'_>,
    relation: Variable,
    roleplayers: &typeql::statement::thing::Relation,
) -> Result<(), PatternDefinitionError> {
    for role_player in &roleplayers.role_players {
        match role_player {
            typeql::statement::thing::RolePlayer::Typed(type_, var) => {
                let player = register_typeql_var(constraints, var)?;
                let type_ = register_typeql_type_var_any(constraints, type_)?;
                constraints.add_role_player(relation, player, type_)?;
            }
            typeql::statement::thing::RolePlayer::Untyped(var) => {
                let player = register_typeql_var(constraints, var)?;
                let role_type = constraints.create_anonymous_variable()?;
                constraints.add_role_player(relation, player, role_type)?;
            }
        }
    }
    Ok(())
}

fn add_in_iterable_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    in_stream: &typeql::statement::InIterable,
) -> Result<(), PatternDefinitionError> {
    todo!()
    // match &in_stream.rhs.name {
    //     FunctionName::Builtin(_) => todo!(),
    //     FunctionName::Identifier(identifier) => add_function_call_user(
    //         function_index,
    //         constraints,
    //         &AssignmentPattern::Variables(in_stream.lhs.clone()),
    //         identifier.as_str(),
    //         &in_stream.rhs.args,
    //         true,
    //     ),
    // }
}

fn add_function_call_user(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    lhs: &AssignmentPattern,
    function_name: &str,
    args: &Vec<TypeQLExpression>,
    expects_stream: bool,
) -> Result<(), PatternDefinitionError> {
    let assigned: Vec<Variable> = assignment_pattern_to_variables(constraints, lhs)?;
    let arguments: Vec<Variable> = split_out_inline_expressions(function_index, constraints, args)?;
    add_function_call_user_impl(function_index, constraints, assigned, function_name, arguments, expects_stream)
}

fn add_typeql_assignment(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    lhs: &AssignmentPattern,
    rhs: &TypeQLExpression,
) -> Result<(), PatternDefinitionError> {
    if let typeql::expression::Expression::Function(function_call) = rhs {
        if let FunctionName::Identifier(identifier) = &function_call.name {
            return add_function_call_user(
                function_index,
                constraints,
                lhs,
                identifier.as_str(),
                &function_call.args,
                false,
            );
        }
    }

    let lhs = match lhs {
        AssignmentPattern::Variables(typeql_vars) => {
            let mut vars =
                typeql_vars.iter().map(|v| register_typeql_var(constraints, v)).collect::<Result<Vec<_>, _>>()?;
            if typeql_vars.len() == 1 {
                vars.pop().unwrap()
            } else {
                todo!("I don't know what's allowed and what isn't")
            }
        }
        AssignmentPattern::Deconstruct(_) => todo!("later"),
    };
    let expression = build_expression(function_index, constraints, rhs)?;
    constraints.add_expression(lhs, expression)?;
    Ok(())
}

// Helpers
pub(super) fn add_function_call_user_impl(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    assigned: Vec<Variable>,
    function_name: &str,
    arguments: Vec<Variable>,
    expects_stream: bool,
) -> Result<(), PatternDefinitionError> {
    let function_opt = function_index
        .get_function_signature(function_name)
        .map_err(|source| PatternDefinitionError::FunctionRead { source })?;
    if let Some(callee) = function_opt {
        match (expects_stream, callee.return_is_stream) {
            (true, true) | (false, false) => {}
            (false, true) => {
                Err(PatternDefinitionError::ExpectedSingeReceivedStream { function_name: function_name.to_owned() })?
            }
            (true, false) => {
                Err(PatternDefinitionError::ExpectedStreamReceivedSingle { function_name: function_name.to_owned() })?
            }
        }
        constraints.add_function_call(assigned.clone(), &callee, arguments)?;
        Ok(())
    } else {
        Err(PatternDefinitionError::UnresolvedFunction { function_name: function_name.to_owned() })
    }
}

fn assignment_pattern_to_variables(
    constraints: &mut ConstraintsBuilder<'_>,
    assignment: &AssignmentPattern,
) -> Result<Vec<Variable>, PatternDefinitionError> {
    match assignment {
        AssignmentPattern::Variables(vars) => {
            vars.iter().map(|variable| register_typeql_var(constraints, variable)).collect::<Result<Vec<_>, _>>()
        }
        AssignmentPattern::Deconstruct(struct_deconstruct) => {
            // If we do want to support this, introduce anonymous variables and another deconstruct IR.
            todo!()
        }
    }
}

pub(super) fn split_out_inline_expressions(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    expressions: &Vec<typeql::expression::Expression>,
) -> Result<Vec<Variable>, PatternDefinitionError> {
    expressions
        .iter()
        .map(|expr| {
            if let typeql::expression::Expression::Variable(typeql_variable) = expr {
                Ok(register_typeql_var(constraints, typeql_variable)?)
            } else {
                let variable = constraints.create_anonymous_variable()?;
                let expression = build_expression(function_index, constraints, expr)?;
                constraints.add_expression(variable, expression)?;
                Ok(variable)
            }
        })
        .collect::<Result<Vec<_>, _>>()
}

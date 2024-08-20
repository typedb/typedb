/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::label::Label;
use typeql::{
    expression::{Expression as TypeQLExpression, Expression, FunctionCall, FunctionName},
    statement::{AssignmentPattern, InIterable},
    type_::NamedType,
    ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    pattern::constraint::{ConstraintsBuilder, IsaKind},
    program::function_signature::FunctionSignatureIndex,
    translation::expression::build_expression,
    PatternDefinitionError,
};

pub(super) fn add_statement<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    stmt: &typeql::Statement,
) -> Result<(), PatternDefinitionError> {
    match stmt {
        typeql::Statement::Is(_) => todo!(),
        typeql::Statement::InStream(InIterable { lhs, rhs, .. }) => {
            let assigned = assignment_typeql_vars_to_variables(constraints, lhs)?;
            add_typeql_binding(function_index, constraints, assigned, rhs, true)?
        }
        typeql::Statement::Comparison(_) => todo!(),
        typeql::Statement::Assignment(assignment) => {
            let assigned: Vec<Variable> = assignment_pattern_to_variables(constraints, &assignment.lhs)?;
            add_typeql_binding(function_index, constraints, assigned, &assignment.rhs, false)?
        }
        typeql::Statement::Thing(thing) => add_thing_statement(function_index, constraints, thing)?,
        typeql::Statement::AttributeValue(attribute_value) => todo!(),
        typeql::Statement::AttributeComparison(_) => todo!(),
        typeql::Statement::Type(type_) => add_type_statement(constraints, type_)?,
    }
    Ok(())
}

fn add_thing_statement<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
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
            typeql::statement::thing::Constraint::Has(has) => add_typeql_has(function_index, constraints, var, has)?,
            typeql::statement::thing::Constraint::Links(links) => {
                add_typeql_relation(constraints, var, &links.relation)?
            }
        }
    }
    Ok(())
}

fn add_type_statement<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
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

fn extend_from_inline_typeql_expression<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    expression: &TypeQLExpression,
) -> Result<Variable, PatternDefinitionError> {
    if let TypeQLExpression::Variable(typeql_var) = expression {
        register_typeql_var(constraints, &typeql_var)
    } else {
        let expression = build_expression(function_index, constraints, expression)?;
        let assigned = constraints.create_anonymous_variable()?;
        constraints.add_expression(assigned, expression)?;
        Ok(assigned)
    }
}

pub(crate) fn register_typeql_var<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
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

fn register_typeql_type_var_any<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    type_: &typeql::TypeRefAny,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeRefAny::Type(type_) => register_typeql_type_var(constraints, type_),
        typeql::TypeRefAny::Optional(_) => todo!(),
        typeql::TypeRefAny::List(_) => todo!(),
    }
}

fn register_typeql_type_var<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    type_: &typeql::TypeRef,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeRef::Named(NamedType::Label(label)) => register_type_label_var(constraints, label),
        typeql::TypeRef::Named(NamedType::Role(scoped_label)) => {
            register_type_scoped_label_var(constraints, scoped_label)
        }
        typeql::TypeRef::Named(NamedType::BuiltinValueType(builtin)) => todo!(),
        typeql::TypeRef::Variable(var) => register_typeql_var(constraints, var),
    }
}

fn register_type_scoped_label_var<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    scoped_label: &ScopedLabel,
) -> Result<Variable, PatternDefinitionError> {
    let label = Label::build_scoped(scoped_label.name.ident.as_str(), scoped_label.scope.ident.as_str());
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_label(variable, label.scoped_name.as_str())?;
    Ok(variable)
}

fn register_type_label_var<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    label: &typeql::Label,
) -> Result<Variable, PatternDefinitionError> {
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_label(variable, label.ident.as_str())?;
    Ok(variable)
}

fn add_typeql_isa<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
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

fn add_typeql_has<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    owner: Variable,
    has: &typeql::statement::thing::Has,
) -> Result<(), PatternDefinitionError> {
    let attribute = match &has.value {
        typeql::statement::thing::HasValue::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::HasValue::Expression(expression) => {
            let assigned = extend_from_inline_typeql_expression(function_index, constraints, expression)?;
            let attribute = constraints.create_anonymous_variable()?;
            constraints.add_comparison(attribute, assigned)?; // TODO: I should probably not piggy back on comparison like this.
            attribute
        }
        typeql::statement::thing::HasValue::Comparison(_) => todo!("Same as above?"),
    };

    constraints.add_has(owner, attribute)?;
    if let Some(type_) = &has.type_ {
        let attribute_type = register_typeql_type_var_any(constraints, type_)?;
        constraints.add_isa(IsaKind::Subtype, attribute, attribute_type)?;
    }
    Ok(())
}

pub(super) fn add_typeql_relation<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    relation: Variable,
    roleplayers: &typeql::statement::thing::Relation,
) -> Result<(), PatternDefinitionError> {
    for role_player in &roleplayers.role_players {
        match role_player {
            typeql::statement::thing::RolePlayer::Typed(type_ref, player_var) => {
                let type_ = match type_ref {
                    TypeRefAny::Type(TypeRef::Named(NamedType::Label(name))) => {
                        let variable = constraints.create_anonymous_variable()?;
                        constraints.add_role_name(variable, name.ident.as_str())?;
                        variable
                    }
                    TypeRefAny::Type(TypeRef::Variable(var)) => register_typeql_var(constraints, var)?,
                    TypeRefAny::Type(TypeRef::Named(NamedType::Role(name))) => {
                        return Err(PatternDefinitionError::ScopedRoleNameInRelation {
                            role_player: role_player.clone(),
                        });
                    }
                    TypeRefAny::Optional(_) => todo!(),
                    TypeRefAny::List(_) => todo!(),
                    TypeRefAny::Type(TypeRef::Named(NamedType::BuiltinValueType(_))) => {
                        unreachable!("Why do we allow value types here?")
                    }
                };
                let player = register_typeql_var(constraints, player_var)?;
                constraints.add_links(relation, player, type_)?;
            }
            typeql::statement::thing::RolePlayer::Untyped(var) => {
                let player = register_typeql_var(constraints, var)?;
                let role_type = constraints.create_anonymous_variable()?;
                constraints.add_links(relation, player, role_type)?;
            }
        }
    }
    Ok(())
}

fn add_typeql_binding<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    assigned: Vec<Variable>,
    rhs: &TypeQLExpression,
    is_stream_binding: bool,
) -> Result<(), PatternDefinitionError> {
    match rhs {
        Expression::Function(FunctionCall { name: FunctionName::Identifier(identifier), args, .. }) => {
            let arguments: Vec<Variable> = split_out_inline_expressions(function_index, constraints, &args)?;
            add_function_call_binding_user(
                function_index,
                constraints,
                assigned,
                identifier.as_str(),
                arguments,
                is_stream_binding,
            )
        }
        Expression::Function(FunctionCall { name: FunctionName::Builtin(_), .. })
        | Expression::Variable(_)
        | Expression::ListIndex(_)
        | Expression::Value(_)
        | Expression::Operation(_)
        | Expression::Paren(_)
        | Expression::List(_)
        | Expression::ListIndexRange(_) => {
            let expression = build_expression(function_index, constraints, rhs)?;
            if assigned.len() != 1 {
                Err(PatternDefinitionError::ExpressionAssignmentMustOneVariable { assigned })
            } else {
                constraints.add_expression(*assigned.get(0).unwrap(), expression)?;
                Ok(())
            }
        }
    }
}

// Helpers
pub(super) fn add_function_call_binding_user<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    assigned: Vec<Variable>,
    function_name: &str,
    arguments: Vec<Variable>,
    must_be_stream: bool,
) -> Result<(), PatternDefinitionError> {
    let function_opt = function_index
        .get_function_signature(function_name)
        .map_err(|source| PatternDefinitionError::FunctionRead { source })?;
    if let Some(callee) = function_opt {
        match (must_be_stream, callee.return_is_stream) {
            (true, true) | (false, false) => {}
            (false, true) => {
                Err(PatternDefinitionError::ExpectedSingeReceivedStream { function_name: function_name.to_owned() })?
            }
            (true, false) => {
                Err(PatternDefinitionError::ExpectedStreamReceivedSingle { function_name: function_name.to_owned() })?
            }
        }
        constraints.add_function_binding(assigned, &callee, arguments)?;
        Ok(())
    } else {
        Err(PatternDefinitionError::UnresolvedFunction { function_name: function_name.to_owned() })
    }
}

fn assignment_pattern_to_variables<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    assignment: &AssignmentPattern,
) -> Result<Vec<Variable>, PatternDefinitionError> {
    match assignment {
        AssignmentPattern::Variables(vars) => assignment_typeql_vars_to_variables(constraints, vars),
        AssignmentPattern::Deconstruct(struct_deconstruct) => {
            Err(PatternDefinitionError::UnimplementedStructAssignment { deconstruct: struct_deconstruct.clone() })
        }
    }
}

fn assignment_typeql_vars_to_variables<'cx, 'reg>(
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
    vars: &Vec<typeql::Variable>,
) -> Result<Vec<Variable>, PatternDefinitionError> {
    vars.iter().map(|variable| register_typeql_var(constraints, variable)).collect::<Result<Vec<_>, _>>()
}

pub(super) fn split_out_inline_expressions<'cx, 'reg>(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'cx, 'reg>,
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

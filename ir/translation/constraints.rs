/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::label::Label;
use typeql::{
    expression::{FunctionCall, FunctionName},
    statement::{
        comparison::ComparisonStatement, thing::AttributeComparisonStatement, Assignment, AssignmentPattern,
        InIterable, Is,
    },
    token::Kind,
    type_::NamedType,
    ScopedLabel, TypeRef, TypeRefAny,
};
use typeql::statement::type_::ValueType as TypeQLValueType;
use typeql::type_::BuiltinValueType;

use crate::{
    pattern::{
        conjunction::ConjunctionBuilder,
        constraint::{Comparator, ConstraintsBuilder, IsaKind},
        Vertex,
    },
    pipeline::function_signature::FunctionSignatureIndex,
    translation::{
        expression::{add_typeql_expression, add_user_defined_function_call, build_expression},
        literal::translate_literal,
    },
    RepresentationError,
};
use crate::pattern::ValueType;
use crate::translation::tokens::translate_value_type;

pub(super) fn add_statement(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    stmt: &typeql::Statement,
) -> Result<(), RepresentationError> {
    let constraints = &mut conjunction.constraints_mut();
    match stmt {
        typeql::Statement::Is(Is { lhs, rhs, .. }) => {
            let lhs = register_typeql_var(constraints, lhs)?;
            let rhs = register_typeql_var(constraints, rhs)?;
            constraints.add_is(lhs, rhs)?;
        }
        typeql::Statement::InIterable(InIterable { lhs, rhs, .. }) => {
            let assigned = assignment_typeql_vars_to_variables(constraints, lhs)?;
            add_typeql_iterable_binding(function_index, constraints, assigned, rhs)?
        }
        typeql::Statement::Comparison(ComparisonStatement { lhs, comparison, .. }) => {
            let lhs_var = add_typeql_expression(function_index, constraints, lhs)?;
            let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
            constraints.add_comparison(lhs_var, rhs_var, comparison.comparator.into())?;
        }
        typeql::Statement::Assignment(Assignment { lhs, rhs, .. }) => {
            let assigned = assignment_pattern_to_variables(constraints, lhs)?;
            let [assigned] = *assigned else {
                return Err(RepresentationError::ExpressionAssignmentMustOneVariable {
                    assigned_count: assigned.len(),
                });
            };
            let expression = build_expression(function_index, constraints, rhs)?;
            constraints.add_assignment(assigned, expression)?;
        }
        typeql::Statement::Thing(thing) => add_thing_statement(function_index, constraints, thing)?,
        typeql::Statement::AttributeValue(attribute_value) => {
            let attribute = register_typeql_var(constraints, &attribute_value.var)?;
            add_typeql_isa(constraints, attribute, &attribute_value.isa)?;

            let value = translate_literal(&attribute_value.value).map_err(|source| {
                RepresentationError::LiteralParseError { source, literal: attribute_value.value.to_string().clone() }
            })?;
            let value_id = constraints.parameters().register_value(value);

            constraints.add_comparison(Vertex::Variable(attribute), Vertex::Parameter(value_id), Comparator::Equal)?;
        }
        typeql::Statement::AttributeComparison(AttributeComparisonStatement { var, comparison, isa, .. }) => {
            let attribute = register_typeql_var(constraints, var)?;
            add_typeql_isa(constraints, attribute, isa)?;
            let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
            constraints.add_comparison(Vertex::Variable(attribute), rhs_var, comparison.comparator.into())?;
        }
        typeql::Statement::Type(type_) => add_type_statement(constraints, type_)?,
    }
    Ok(())
}

fn add_thing_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: &typeql::statement::Thing,
) -> Result<(), RepresentationError> {
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

fn add_type_statement(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_statement: &typeql::statement::Type,
) -> Result<(), RepresentationError> {
    let type_ = register_typeql_type_any(constraints, &type_statement.type_)?;
    if let Some(kind) = type_statement.kind {
        let Vertex::Variable(var) = type_ else {
            return Err(RepresentationError::LabelWithKind { declaration: type_statement.clone() });
        };
        add_typeql_kind(constraints, var, kind)?;
    }
    for constraint in &type_statement.constraints {
        assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
        match &constraint.base {
            typeql::statement::type_::ConstraintBase::Sub(sub) => add_typeql_sub(constraints, type_.clone(), sub)?,
            typeql::statement::type_::ConstraintBase::Label(label) => match label {
                typeql::statement::type_::LabelConstraint::Name(label) => {
                    let &Vertex::Variable(var) = &type_ else {
                        return Err(RepresentationError::LabelWithLabel { declaration: label.clone() });
                    };
                    constraints.add_label(var, Label::build(label.ident.as_str()))?;
                }
                typeql::statement::type_::LabelConstraint::Scoped(scoped_label) => {
                    let &Vertex::Variable(var) = &type_ else {
                        return Err(RepresentationError::ScopedLabelWithLabel { declaration: scoped_label.clone() });
                    };
                    constraints.add_label(
                        var,
                        Label::build_scoped(scoped_label.name.ident.as_str(), scoped_label.scope.ident.as_str()),
                    )?;
                }
            },
            typeql::statement::type_::ConstraintBase::ValueType(value_type) => add_typeql_value(constraints, type_.clone(), value_type)?,
            typeql::statement::type_::ConstraintBase::Owns(owns) => add_typeql_owns(constraints, type_.clone(), owns)?,
            typeql::statement::type_::ConstraintBase::Relates(relates) => {
                add_typeql_relates(constraints, type_.clone(), relates)?
            }
            typeql::statement::type_::ConstraintBase::Plays(plays) => {
                add_typeql_plays(constraints, type_.clone(), plays)?
            }
        }
    }
    Ok(())
}

fn extend_from_inline_typeql_expression(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expression: &typeql::Expression,
) -> Result<Variable, RepresentationError> {
    if let typeql::Expression::Variable(typeql_var) = expression {
        register_typeql_var(constraints, typeql_var)
    } else {
        let expression = build_expression(function_index, constraints, expression)?;
        let assigned = constraints.create_anonymous_variable()?;
        constraints.add_assignment(assigned, expression)?;
        Ok(assigned)
    }
}

pub(crate) fn register_typeql_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    var: &typeql::Variable,
) -> Result<Variable, RepresentationError> {
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

fn register_typeql_type_any(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::TypeRefAny,
) -> Result<Vertex<Variable>, RepresentationError> {
    match type_ {
        typeql::TypeRefAny::Type(type_) => register_typeql_type(constraints, type_),
        typeql::TypeRefAny::Optional(_) => todo!(),
        typeql::TypeRefAny::List(_) => todo!(),
    }
}

fn register_typeql_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::TypeRef,
) -> Result<Vertex<Variable>, RepresentationError> {
    match type_ {
        typeql::TypeRef::Named(NamedType::Label(label)) => Ok(Vertex::Label(Label::build(label.ident.as_str()))),
        typeql::TypeRef::Named(NamedType::Role(scoped_label)) => {
            Ok(Vertex::Label(register_type_scoped_label(constraints, scoped_label)?))
        }
        typeql::TypeRef::Named(NamedType::BuiltinValueType(builtin)) => todo!(),
        typeql::TypeRef::Variable(var) => Ok(Vertex::Variable(register_typeql_var(constraints, var)?)),
    }
}

fn register_typeql_role_type_any(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::TypeRefAny,
) -> Result<Vertex<Variable>, RepresentationError> {
    match type_ {
        typeql::TypeRefAny::Type(type_) => register_typeql_role_type(constraints, type_),
        typeql::TypeRefAny::Optional(_) => todo!(),
        typeql::TypeRefAny::List(_) => todo!(),
    }
}

fn register_typeql_role_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::TypeRef,
) -> Result<Vertex<Variable>, RepresentationError> {
    match type_ {
        typeql::TypeRef::Named(NamedType::Label(label)) => {
            Ok(Vertex::Variable(register_type_role_name_var(constraints, label)?))
        }
        typeql::TypeRef::Named(NamedType::Role(scoped_label)) => {
            Ok(Vertex::Label(register_type_scoped_label(constraints, scoped_label)?))
        }
        typeql::TypeRef::Named(NamedType::BuiltinValueType(builtin)) => todo!(),
        typeql::TypeRef::Variable(var) => Ok(Vertex::Variable(register_typeql_var(constraints, var)?)),
    }
}

fn register_type_scoped_label(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    scoped_label: &ScopedLabel,
) -> Result<Label<'static>, RepresentationError> {
    Ok(Label::build_scoped(scoped_label.name.ident.as_str(), scoped_label.scope.ident.as_str()))
}

fn register_type_label(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    label: &typeql::Label,
) -> Result<Label<'static>, RepresentationError> {
    Ok(Label::build(label.ident.as_str()))
}

fn register_type_role_name_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    label: &typeql::Label,
) -> Result<Variable, RepresentationError> {
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_role_name(variable, label.ident.as_str())?;
    Ok(variable)
}

fn register_typeql_value_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    value_type: &TypeQLValueType,
) -> Result<ValueType, RepresentationError> {
    match &value_type.value_type {
        NamedType::Role(scoped_label) => Err(RepresentationError::ScopedValueTypeName {
            scope: scoped_label.scope.ident.as_str().to_owned(),
            name: scoped_label.name.ident.as_str().to_owned(),
        }),
        NamedType::Label(label) => Ok(ValueType::Struct(label.ident.as_str().clone().to_owned())),
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ValueType::Builtin(translate_value_type(token)))
        }
    }
}

fn add_typeql_kind(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: Variable,
    kind: Kind,
) -> Result<(), RepresentationError> {
    constraints.add_kind(kind, type_)?;
    Ok(())
}

fn add_typeql_sub(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    subtype: Vertex<Variable>,
    sub: &typeql::statement::type_::Sub,
) -> Result<(), RepresentationError> {
    let kind = sub.kind.into();
    let type_ = register_typeql_type_any(constraints, &sub.supertype)?;
    constraints.add_sub(kind, subtype, type_)?;
    Ok(())
}

fn add_typeql_owns(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner_type: Vertex<Variable>,
    owns: &typeql::statement::type_::Owns,
) -> Result<(), RepresentationError> {
    let attribute_type = register_typeql_type_any(constraints, &owns.owned)?;
    constraints.add_owns(owner_type, attribute_type)?;
    Ok(())
}

fn add_typeql_relates(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    relation_type: Vertex<Variable>,
    relates: &typeql::statement::type_::Relates,
) -> Result<(), RepresentationError> {
    let role_type = register_typeql_role_type_any(constraints, &relates.related)?;
    constraints.add_relates(relation_type, role_type)?;
    Ok(())
}

fn add_typeql_plays(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    player_type: Vertex<Variable>,
    plays: &typeql::statement::type_::Plays,
) -> Result<(), RepresentationError> {
    let role_type = register_typeql_role_type(constraints, &plays.role)?;
    constraints.add_plays(player_type, role_type)?;
    Ok(())
}

fn add_typeql_value(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    attribute_type: Vertex<Variable>,
    value_type: &TypeQLValueType,
) -> Result<(), RepresentationError> {
    let value_type = register_typeql_value_type(constraints, value_type)?;
    constraints.add_value(attribute_type, value_type)?;
    Ok(())
}

fn add_typeql_isa(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: Variable,
    isa: &typeql::statement::thing::isa::Isa,
) -> Result<(), RepresentationError> {
    let kind = isa.kind.into();
    let type_ = register_typeql_type(constraints, &isa.type_)?;
    constraints.add_isa(kind, thing, type_)?;
    Ok(())
}

fn add_typeql_has(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner: Variable,
    has: &typeql::statement::thing::Has,
) -> Result<(), RepresentationError> {
    let attribute = match &has.value {
        typeql::statement::thing::HasValue::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::HasValue::Expression(expression) => {
            let expression = add_typeql_expression(function_index, constraints, expression)?;
            let attribute = constraints.create_anonymous_variable()?;
            constraints.add_comparison(Vertex::Variable(attribute), expression, Comparator::Equal)?;
            attribute
        }
        typeql::statement::thing::HasValue::Comparison(comparison) => {
            let attribute = constraints.create_anonymous_variable()?;
            let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
            constraints.add_comparison(Vertex::Variable(attribute), rhs_var, comparison.comparator.into())?;
            attribute
        }
    };

    constraints.add_has(owner, attribute)?;
    if let Some(type_) = &has.type_ {
        let attribute_type = register_typeql_type_any(constraints, type_)?;
        constraints.add_isa(IsaKind::Subtype, attribute, attribute_type)?;
    }
    Ok(())
}

pub(super) fn add_typeql_relation(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    relation: Variable,
    roleplayers: &typeql::statement::thing::Relation,
) -> Result<(), RepresentationError> {
    for role_player in &roleplayers.role_players {
        match role_player {
            typeql::statement::thing::RolePlayer::Typed(type_ref, player_var) => {
                let type_ = match type_ref {
                    TypeRefAny::Type(TypeRef::Named(NamedType::Label(name))) => {
                        register_type_role_name_var(constraints, name)?
                    }
                    TypeRefAny::Type(TypeRef::Variable(var)) => register_typeql_var(constraints, var)?,
                    TypeRefAny::Type(TypeRef::Named(NamedType::Role(name))) => {
                        return Err(RepresentationError::ScopedRoleNameInRelation { declaration: role_player.clone() });
                    }
                    TypeRefAny::Optional(_) => todo!(),
                    TypeRefAny::List(_) => todo!(),
                    TypeRefAny::Type(TypeRef::Named(NamedType::BuiltinValueType(_))) => {
                        todo!("throw error")
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

fn add_typeql_iterable_binding(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assigned: Vec<Variable>,
    rhs: &typeql::Expression,
) -> Result<(), RepresentationError> {
    match rhs {
        typeql::Expression::Function(FunctionCall { name: FunctionName::Identifier(identifier), args, .. }) => {
            add_user_defined_function_call(function_index, constraints, identifier, assigned, args)
        }
        typeql::Expression::Function(FunctionCall { name: FunctionName::Builtin(_), .. }) => {
            todo!("builtin function returning list (e.g. list(stream_func()))")
        }
        typeql::Expression::List(_) | typeql::Expression::ListIndexRange(_) => todo!("iter in list or range slice"),
        | typeql::Expression::Variable(_)
        | typeql::Expression::ListIndex(_)
        | typeql::Expression::Value(_)
        | typeql::Expression::Operation(_)
        | typeql::Expression::Paren(_) => unreachable!(),
    }
}

// Helpers
pub(super) fn add_function_call_binding_user(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assigned: Vec<Variable>,
    function_name: &str,
    arguments: Vec<Variable>,
    must_be_stream: bool,
) -> Result<(), RepresentationError> {
    let function_opt = function_index
        .get_function_signature(function_name)
        .map_err(|source| RepresentationError::FunctionReadError { source })?;
    if let Some(callee) = function_opt {
        match (must_be_stream, callee.return_is_stream) {
            (true, true) | (false, false) => {}
            (false, true) => Err(RepresentationError::ExpectedSingleFunctionReturnsStream {
                function_name: function_name.to_owned(),
            })?,
            (true, false) => Err(RepresentationError::ExpectedStreamFunctionReturnsSingle {
                function_name: function_name.to_owned(),
            })?,
        }
        constraints.add_function_binding(assigned, &callee, arguments, function_name)?;
        Ok(())
    } else {
        Err(RepresentationError::UnresolvedFunction { function_name: function_name.to_owned() })
    }
}

fn assignment_pattern_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assignment: &AssignmentPattern,
) -> Result<Vec<Variable>, RepresentationError> {
    match assignment {
        AssignmentPattern::Variables(vars) => assignment_typeql_vars_to_variables(constraints, vars),
        AssignmentPattern::Deconstruct(struct_deconstruct) => {
            Err(RepresentationError::UnimplementedStructAssignment { declaration: struct_deconstruct.clone() })
        }
    }
}

fn assignment_typeql_vars_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    vars: &[typeql::Variable],
) -> Result<Vec<Variable>, RepresentationError> {
    vars.iter().map(|variable| register_typeql_var(constraints, variable)).collect()
}

pub(super) fn split_out_inline_expressions(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expressions: &[typeql::Expression],
) -> Result<Vec<Variable>, RepresentationError> {
    expressions
        .iter()
        .map(|expr| {
            if let typeql::Expression::Variable(typeql_variable) = expr {
                Ok(register_typeql_var(constraints, typeql_variable)?)
            } else {
                let variable = constraints.create_anonymous_variable()?;
                let expression = build_expression(function_index, constraints, expr)?;
                constraints.add_assignment(variable, expression)?;
                Ok(variable)
            }
        })
        .collect()
}

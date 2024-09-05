/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use encoding::value::label::Label;
use typeql::{
    expression::{FunctionCall, FunctionName},
    statement::{comparison::ComparisonStatement, Assignment, AssignmentPattern, InIterable},
    type_::NamedType,
    ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    pattern::{
        conjunction::ConjunctionBuilder,
        constraint::{Comparator, ConstraintsBuilder, IsaKind, SubKind},
    },
    program::function_signature::FunctionSignatureIndex,
    translation::expression::{add_typeql_expression, add_user_defined_function_call, build_expression},
    PatternDefinitionError,
};

pub(super) fn add_statement(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    stmt: &typeql::Statement,
) -> Result<(), PatternDefinitionError> {
    let constraints = &mut conjunction.constraints_mut();
    match stmt {
        typeql::Statement::Is(_) => todo!(),
        typeql::Statement::InIterable(InIterable { lhs, rhs, .. }) => {
            let assigned = assignment_typeql_vars_to_variables(constraints, lhs)?;
            add_typeql_iterable_binding(function_index, constraints, assigned, rhs)?
        }
        typeql::Statement::Comparison(ComparisonStatement { lhs, comparison, .. }) => {
            let lhs_var = constraints.create_anonymous_variable()?;
            add_typeql_expression(function_index, constraints, lhs_var, lhs)?;

            let rhs_var = constraints.create_anonymous_variable()?;
            add_typeql_expression(function_index, constraints, rhs_var, &comparison.rhs)?;

            constraints.add_comparison(lhs_var, rhs_var, comparison.comparator.into())?;
        }
        typeql::Statement::Assignment(Assignment { lhs, rhs, .. }) => {
            let assigned = assignment_pattern_to_variables(constraints, lhs)?;
            let [assigned] = *assigned else {
                return Err(PatternDefinitionError::ExpressionAssignmentMustOneVariable { assigned });
            };
            add_typeql_expression(function_index, constraints, assigned, rhs)?
        }
        typeql::Statement::Thing(thing) => add_thing_statement(function_index, constraints, thing)?,
        typeql::Statement::AttributeValue(attribute_value) => todo!(),
        typeql::Statement::AttributeComparison(_) => todo!(),
        typeql::Statement::Type(type_) => add_type_statement(constraints, type_)?,
    }
    Ok(())
}

fn add_thing_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
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

fn add_type_statement(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::statement::Type,
) -> Result<(), PatternDefinitionError> {
    let var = register_typeql_type_var_any(constraints, &type_.type_)?;
    for constraint in &type_.constraints {
        assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
        match &constraint.base {
            typeql::statement::type_::ConstraintBase::Sub(sub) => add_typeql_sub(constraints, var, sub)?,
            typeql::statement::type_::ConstraintBase::Label(label) => match label {
                typeql::statement::type_::LabelConstraint::Name(label) => {
                    constraints.add_label(var, label.ident.as_str())?;
                }
                typeql::statement::type_::LabelConstraint::Scoped(_) => todo!(),
            },
            typeql::statement::type_::ConstraintBase::ValueType(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Owns(owns) => add_typeql_owns(constraints, var, owns)?,
            typeql::statement::type_::ConstraintBase::Relates(relates) => {
                add_typeql_relates(constraints, var, relates)?
            }
            typeql::statement::type_::ConstraintBase::Plays(plays) => add_typeql_plays(constraints, var, plays)?,
        }
    }
    Ok(())
}

fn extend_from_inline_typeql_expression(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expression: &typeql::Expression,
) -> Result<Variable, PatternDefinitionError> {
    if let typeql::Expression::Variable(typeql_var) = expression {
        register_typeql_var(constraints, typeql_var)
    } else {
        let expression = build_expression(function_index, constraints, expression)?;
        let assigned = constraints.create_anonymous_variable()?;
        constraints.add_expression(assigned, expression)?;
        Ok(assigned)
    }
}

pub(crate) fn register_typeql_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
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
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &typeql::TypeRefAny,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeRefAny::Type(type_) => register_typeql_type_var(constraints, type_),
        typeql::TypeRefAny::Optional(_) => todo!(),
        typeql::TypeRefAny::List(_) => todo!(),
    }
}

fn register_typeql_type_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
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

fn register_type_scoped_label_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    scoped_label: &ScopedLabel,
) -> Result<Variable, PatternDefinitionError> {
    let label = Label::build_scoped(scoped_label.name.ident.as_str(), scoped_label.scope.ident.as_str());
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_label(variable, label.scoped_name.as_str())?;
    Ok(variable)
}

fn register_type_label_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    label: &typeql::Label,
) -> Result<Variable, PatternDefinitionError> {
    let variable = constraints.create_anonymous_variable()?;
    constraints.add_label(variable, label.ident.as_str())?;
    Ok(variable)
}

fn add_typeql_sub(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: Variable,
    sub: &typeql::statement::type_::Sub,
) -> Result<(), PatternDefinitionError> {
    let kind = match sub.kind {
        typeql::statement::type_::SubKind::Direct => SubKind::Exact,
        typeql::statement::type_::SubKind::Transitive => SubKind::Subtype,
    };
    let type_ = register_typeql_type_var_any(constraints, &sub.supertype)?;
    constraints.add_sub(kind, thing, type_)?;
    Ok(())
}

fn add_typeql_owns(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner_type: Variable,
    owns: &typeql::statement::type_::Owns,
) -> Result<(), PatternDefinitionError> {
    let attribute_type = register_typeql_type_var_any(constraints, &owns.owned)?;
    constraints.add_owns(owner_type, attribute_type)?;
    Ok(())
}

fn add_typeql_relates(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    relation_type: Variable,
    relates: &typeql::statement::type_::Relates,
) -> Result<(), PatternDefinitionError> {
    let role_type = register_typeql_type_var_any(constraints, &relates.related)?;
    constraints.add_relates(relation_type, role_type)?;
    Ok(())
}

fn add_typeql_plays(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    player_type: Variable,
    plays: &typeql::statement::type_::Plays,
) -> Result<(), PatternDefinitionError> {
    let role_type = register_typeql_type_var(constraints, &plays.role)?;
    constraints.add_plays(player_type, role_type)?;
    Ok(())
}

fn add_typeql_isa(
    constraints: &mut ConstraintsBuilder<'_, '_>,
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
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner: Variable,
    has: &typeql::statement::thing::Has,
) -> Result<(), PatternDefinitionError> {
    let attribute = match &has.value {
        typeql::statement::thing::HasValue::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::HasValue::Expression(expression) => {
            let assigned = extend_from_inline_typeql_expression(function_index, constraints, expression)?;
            let attribute = constraints.create_anonymous_variable()?;
            constraints.add_comparison(attribute, assigned, Comparator::Equal)?; // TODO: I should probably not piggy back on comparison like this.
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

pub(super) fn add_typeql_relation(
    constraints: &mut ConstraintsBuilder<'_, '_>,
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

fn add_typeql_iterable_binding(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assigned: Vec<Variable>,
    rhs: &typeql::Expression,
) -> Result<(), PatternDefinitionError> {
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

fn assignment_pattern_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assignment: &AssignmentPattern,
) -> Result<Vec<Variable>, PatternDefinitionError> {
    match assignment {
        AssignmentPattern::Variables(vars) => assignment_typeql_vars_to_variables(constraints, vars),
        AssignmentPattern::Deconstruct(struct_deconstruct) => {
            Err(PatternDefinitionError::UnimplementedStructAssignment { deconstruct: struct_deconstruct.clone() })
        }
    }
}

fn assignment_typeql_vars_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    vars: &[typeql::Variable],
) -> Result<Vec<Variable>, PatternDefinitionError> {
    vars.iter().map(|variable| register_typeql_var(constraints, variable)).collect()
}

pub(super) fn split_out_inline_expressions(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expressions: &[typeql::Expression],
) -> Result<Vec<Variable>, PatternDefinitionError> {
    expressions
        .iter()
        .map(|expr| {
            if let typeql::Expression::Variable(typeql_variable) = expr {
                Ok(register_typeql_var(constraints, typeql_variable)?)
            } else {
                let variable = constraints.create_anonymous_variable()?;
                let expression = build_expression(function_index, constraints, expr)?;
                constraints.add_expression(variable, expression)?;
                Ok(variable)
            }
        })
        .collect()
}

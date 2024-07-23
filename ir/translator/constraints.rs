/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use typeql::expression::FunctionName;

use crate::{
    pattern::constraint::{ConstraintsBuilder, IsaKind},
    program::function_signature::FunctionSignatureIndex,
    PatternDefinitionError,
};

pub(super) fn add_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    stmt: &typeql::Statement,
) -> Result<(), PatternDefinitionError> {
    match stmt {
        typeql::Statement::Is(_) => todo!(),
        typeql::Statement::InStream(in_stream) => add_in_stream_statement(function_index, constraints, in_stream)?,
        typeql::Statement::Comparison(_) => todo!(),
        typeql::Statement::Assignment(_) => todo!(),
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
    let var = register_typeql_type_var(constraints, &type_.type_)?;
    for constraint in &type_.constraints {
        assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
        match &constraint.base {
            typeql::statement::type_::ConstraintBase::Sub(_) => todo!(),
            typeql::statement::type_::ConstraintBase::Label(label) => match label {
                typeql::statement::type_::LabelConstraint::Name(label) => {
                    constraints.add_label(var, label.as_str())?;
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
fn add_in_stream_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_>,
    in_stream: &typeql::statement::InStream,
) -> Result<(), PatternDefinitionError> {
    let (function_name, function_opt) = match &in_stream.rhs.name {
        FunctionName::Builtin(_) => todo!("Is this legal?"),
        FunctionName::Identifier(identifier) => {
            let function_signature_opt = function_index
                .get_function_signature(identifier.as_str())
                .map_err(|source| PatternDefinitionError::FunctionRead { source })?;
            (identifier.as_str().to_owned(), function_signature_opt)
        }
    };
    if function_opt.is_none() {
        Err(PatternDefinitionError::UnresolvedFunction { function_name: function_name.clone() })?
    }
    let callee_function = function_opt.unwrap();
    if !callee_function.return_is_stream {
        Err(PatternDefinitionError::FunctionDoesNotReturnStream { function_name: function_name.clone() })?
    }

    let mut assigned: Vec<Variable> = Vec::with_capacity(in_stream.lhs.len());
    for (index, typeql_var) in in_stream.lhs.iter().enumerate() {
        assigned.push(register_typeql_var(constraints, typeql_var)?);
    }
    let arguments: Vec<Variable> = in_stream
        .rhs
        .args
        .iter()
        .map(|arg| {
            extend_from_inline_typeql_expression(constraints, &arg) // Inline expressions must be converted to anonymous variables
        })
        .collect::<Result<Vec<_>, PatternDefinitionError>>()?;

    let as_ref = constraints.add_function_call(assigned, &callee_function, arguments)?;
    Ok(())
}

fn extend_from_inline_typeql_expression(
    constraints: &mut ConstraintsBuilder<'_>,
    expression: &typeql::expression::Expression,
) -> Result<Variable, PatternDefinitionError> {
    let var = match expression {
        typeql::expression::Expression::Variable(typeql_var) => register_typeql_var(constraints, &typeql_var)?,
        _ => todo!(),
    };
    Ok(var)
}

fn register_typeql_var(
    constraints: &mut ConstraintsBuilder<'_>,
    var: &typeql::Variable,
) -> Result<Variable, PatternDefinitionError> {
    match var {
        typeql::Variable::Named(_, name) => constraints.get_or_declare_variable(name.as_str()),
        typeql::Variable::Anonymous(_) => constraints.create_anonymous_variable(),
    }
}

fn register_typeql_type_var_any(
    constraints: &mut ConstraintsBuilder<'_>,
    type_: &typeql::TypeAny,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::TypeAny::Type(type_) => register_typeql_type_var(constraints, type_),
        typeql::TypeAny::Optional(_) => todo!(),
        typeql::TypeAny::List(_) => todo!(),
    }
}

fn register_typeql_type_var(
    constraints: &mut ConstraintsBuilder<'_>,
    type_: &typeql::Type,
) -> Result<Variable, PatternDefinitionError> {
    match type_ {
        typeql::Type::Label(label) => register_type_label_var(constraints, label),
        typeql::Type::ScopedLabel(_) => todo!(),
        typeql::Type::Variable(var) => register_typeql_var(constraints, var),
        typeql::Type::BuiltinValue(_) => todo!(),
    }
}

fn register_type_label_var(
    constraints: &mut ConstraintsBuilder<'_>,
    label: &typeql::Label,
) -> Result<Variable, PatternDefinitionError> {
    let var = constraints.create_anonymous_variable()?;
    match label {
        typeql::Label::Identifier(ident) => constraints.add_label(var, ident.as_str())?,
        typeql::Label::Reserved(reserved) => todo!("Unhandled builtin type: {reserved}"),
    };
    Ok(var)
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
    let attribute = constraints.get_or_declare_variable(attr.name().unwrap())?;
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
                constraints.add_role_player(relation, player, Some(type_))?;
            }
            typeql::statement::thing::RolePlayer::Untyped(var) => {
                let player = register_typeql_var(constraints, var)?;
                constraints.add_role_player(relation, player, None)?;
            }
        }
    }
    Ok(())
}

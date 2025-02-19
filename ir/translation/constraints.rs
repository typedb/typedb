/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use bytes::byte_array::ByteArray;
use encoding::{graph::thing::THING_VERTEX_MAX_LENGTH, value::label::Label};
use error::UnimplementedFeature;
use itertools::Itertools;
use typeql::{
    common::Spanned,
    expression::{FunctionCall, FunctionName},
    statement::{
        comparison::ComparisonStatement, thing::isa::IsaInstanceConstraint, type_::ValueType as TypeQLValueType,
        Assignment, AssignmentPattern, InIterable, Is,
    },
    token::Kind,
    type_::{BuiltinValueType, NamedType},
    ScopedLabel, TypeRef, TypeRefAny,
};

use crate::{
    pattern::{
        conjunction::ConjunctionBuilder,
        constraint::{Comparator, ConstraintsBuilder, IsaKind, SubKind},
        ValueType, Vertex,
    },
    pipeline::function_signature::FunctionSignatureIndex,
    translation::{
        expression::{add_typeql_expression, add_user_defined_function_call, build_expression},
        literal::translate_literal,
        tokens::{checked_identifier, translate_value_type},
    },
    RepresentationError,
};

pub(super) fn add_statement(
    function_index: &impl FunctionSignatureIndex,
    conjunction: &mut ConjunctionBuilder<'_, '_>,
    stmt: &typeql::Statement,
) -> Result<(), Box<RepresentationError>> {
    let constraints = &mut conjunction.constraints_mut();
    match stmt {
        typeql::Statement::Is(Is { lhs, rhs, span }) => {
            let lhs = register_typeql_var(constraints, lhs)?;
            let rhs = register_typeql_var(constraints, rhs)?;
            constraints.add_is(lhs, rhs, *span)?;
        }
        typeql::Statement::InIterable(InIterable { lhs, rhs, span }) => {
            let assigned = assignment_typeql_vars_to_variables(constraints, lhs)?;
            add_typeql_iterable_binding(function_index, constraints, assigned, rhs)?
        }
        typeql::Statement::Comparison(ComparisonStatement { lhs, comparison, span }) => {
            let lhs_var = add_typeql_expression(function_index, constraints, lhs)?;
            let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
            let comparator = comparison.comparator.try_into().map_err(|typedb_source| {
                Box::new(RepresentationError::LiteralParseError {
                    literal: comparison.comparator.to_string(),
                    source_span: *span,
                    typedb_source,
                })
            })?;
            constraints.add_comparison(lhs_var, rhs_var, comparator, *span)?;
        }
        typeql::Statement::Assignment(Assignment { lhs, rhs, span }) => {
            let assigned = assignment_pattern_to_variables(constraints, lhs)?;
            if let typeql::Expression::Function(FunctionCall { name: FunctionName::Identifier(id), args, span }) = rhs {
                add_user_defined_function_call(
                    function_index,
                    constraints,
                    id.as_str_unchecked(),
                    assigned,
                    args,
                    *span,
                )?;
            } else {
                let [assigned] = *assigned else {
                    return Err(Box::new(RepresentationError::ExpressionAssignmentMustOneVariable {
                        assigned_count: assigned.len(),
                        source_span: *span,
                    }));
                };
                let expression = build_expression(function_index, constraints, rhs)?;
                constraints.add_assignment(assigned, expression, *span)?;
            }
        }
        typeql::Statement::Thing(thing) => add_thing_statement(function_index, constraints, thing)?,
        typeql::Statement::Type(type_) => add_type_statement(constraints, type_)?,
    }
    Ok(())
}

fn add_thing_statement(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: &typeql::statement::Thing,
) -> Result<(), Box<RepresentationError>> {
    let var = match &thing.head {
        typeql::statement::thing::Head::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::Head::Relation(type_opt, rel) => {
            let relation = constraints.create_anonymous_variable(thing.span())?;
            if let Some(type_ref) = type_opt {
                let type_ = register_typeql_type(constraints, type_ref)?;
                constraints.add_isa(IsaKind::Subtype, relation, type_, thing.span())?;
            }
            add_typeql_relation(constraints, relation, rel)?;
            relation
        }
    };
    for constraint in &thing.constraints {
        match constraint {
            typeql::statement::thing::Constraint::Isa(isa) => add_typeql_isa(function_index, constraints, var, isa)?,
            typeql::statement::thing::Constraint::Iid(iid) => add_typeql_iid(constraints, var, iid)?,
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
) -> Result<(), Box<RepresentationError>> {
    let type_ = register_typeql_type(constraints, &type_statement.type_)?;
    if let Some(kind) = type_statement.kind {
        let Vertex::Variable(var) = type_ else {
            return Err(Box::new(RepresentationError::LabelWithKind { source_span: type_statement.span() }));
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
                        return Err(Box::new(RepresentationError::LabelWithLabel { source_span: label.span() }));
                    };
                    let as_label = register_type_label(constraints, label)?;
                    constraints.add_label(var, as_label)?;
                }
                typeql::statement::type_::LabelConstraint::Scoped(scoped_label) => {
                    let &Vertex::Variable(var) = &type_ else {
                        return Err(Box::new(RepresentationError::ScopedLabelWithLabel {
                            source_span: scoped_label.span(),
                        }));
                    };
                    let as_label = register_type_scoped_label(constraints, scoped_label)?;
                    constraints.add_label(var, as_label)?;
                }
            },
            typeql::statement::type_::ConstraintBase::ValueType(value_type) => {
                add_typeql_value(constraints, type_.clone(), value_type)?
            }
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
    typeql_expression: &typeql::Expression,
) -> Result<Variable, Box<RepresentationError>> {
    if let typeql::Expression::Variable(typeql_var) = typeql_expression {
        register_typeql_var(constraints, typeql_var)
    } else {
        let expression = build_expression(function_index, constraints, typeql_expression)?;
        let assigned = constraints.create_anonymous_variable(typeql_expression.span())?;
        constraints.add_assignment(assigned, expression, typeql_expression.span())?;
        Ok(assigned)
    }
}

pub(crate) fn register_typeql_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    var: &typeql::Variable,
) -> Result<Variable, Box<RepresentationError>> {
    match var {
        typeql::Variable::Named { ident, optional, .. } => {
            let var = constraints.get_or_declare_variable(ident.as_str_unchecked(), var.span())?;
            Ok(var)
        }
        typeql::Variable::Anonymous { optional, .. } => {
            let var = constraints.create_anonymous_variable(var.span())?;
            Ok(var)
        }
    }
}

fn register_typeql_type_any(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &TypeRefAny,
) -> Result<Vertex<Variable>, Box<RepresentationError>> {
    match type_ {
        TypeRefAny::Type(type_) => register_typeql_type(constraints, type_),
        TypeRefAny::List(list) => Err(Box::new(RepresentationError::UnimplementedListType {
            source_span: list.span(),
            feature: UnimplementedFeature::Lists,
        })),
    }
}

fn register_typeql_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &TypeRef,
) -> Result<Vertex<Variable>, Box<RepresentationError>> {
    match type_ {
        TypeRef::Label(label) => Ok(Vertex::Label(register_type_label(constraints, label)?)),
        TypeRef::Scoped(scoped_label) => Ok(Vertex::Label(register_type_scoped_label(constraints, scoped_label)?)),
        TypeRef::Variable(var) => Ok(Vertex::Variable(register_typeql_var(constraints, var)?)),
    }
}

fn register_typeql_role_type_any(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &TypeRefAny,
) -> Result<Vertex<Variable>, Box<RepresentationError>> {
    match type_ {
        TypeRefAny::Type(type_) => register_typeql_role_type(constraints, type_),
        TypeRefAny::List(list) => Err(Box::new(RepresentationError::UnimplementedListType {
            source_span: list.span(),
            feature: error::UnimplementedFeature::Lists,
        })),
    }
}

fn register_typeql_role_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: &TypeRef,
) -> Result<Vertex<Variable>, Box<RepresentationError>> {
    match type_ {
        TypeRef::Label(label) => Ok(Vertex::Variable(register_type_role_name_var(constraints, label)?)),
        TypeRef::Scoped(scoped_label) => Ok(Vertex::Label(register_type_scoped_label(constraints, scoped_label)?)),
        TypeRef::Variable(var) => Ok(Vertex::Variable(register_typeql_var(constraints, var)?)),
    }
}

fn register_type_scoped_label(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    scoped_label: &ScopedLabel,
) -> Result<Label, Box<RepresentationError>> {
    let checked_scope = checked_identifier(&scoped_label.scope.ident)?;
    let checked_name = checked_identifier(&scoped_label.name.ident)?;
    Ok(Label::build_scoped(checked_name, checked_scope, scoped_label.span()))
}

fn register_type_label(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    label: &typeql::Label,
) -> Result<Label, Box<RepresentationError>> {
    Ok(Label::build(checked_identifier(&label.ident)?, label.span()))
}

fn register_type_role_name_var(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    label: &typeql::Label,
) -> Result<Variable, Box<RepresentationError>> {
    let variable = constraints.create_anonymous_variable(label.span())?;
    constraints.add_role_name(variable, checked_identifier(&label.ident)?, label.span())?;
    Ok(variable)
}

fn register_typeql_value_type(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    value_type: &TypeQLValueType,
) -> Result<ValueType, Box<RepresentationError>> {
    match &value_type.value_type {
        NamedType::Label(label) => Ok(ValueType::Struct(checked_identifier(&label.ident)?.to_owned())),
        NamedType::BuiltinValueType(BuiltinValueType { token, .. }) => {
            Ok(ValueType::Builtin(translate_value_type(token)))
        }
    }
}

fn add_typeql_kind(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    type_: Variable,
    kind: Kind,
) -> Result<(), Box<RepresentationError>> {
    constraints.add_kind(kind, type_)?;
    Ok(())
}

fn add_typeql_sub(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    subtype: Vertex<Variable>,
    sub: &typeql::statement::type_::Sub,
) -> Result<(), Box<RepresentationError>> {
    let kind = sub.kind.into();
    let type_ = register_typeql_type(constraints, &sub.supertype)?;
    constraints.add_sub(kind, subtype, type_, sub.span())?;
    Ok(())
}

fn add_typeql_owns(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner_type: Vertex<Variable>,
    owns: &typeql::statement::type_::Owns,
) -> Result<(), Box<RepresentationError>> {
    let attribute_type = register_typeql_type_any(constraints, &owns.owned)?;
    constraints.add_owns(owner_type, attribute_type, owns.span())?;
    Ok(())
}

fn add_typeql_relates(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    relation_type: Vertex<Variable>,
    relates: &typeql::statement::type_::Relates,
) -> Result<(), Box<RepresentationError>> {
    let role_type = register_typeql_role_type_any(constraints, &relates.related)?;
    constraints.add_relates(relation_type, role_type.clone(), relates.span())?;

    if let Some(specialised) = &relates.specialised {
        add_typeql_as(constraints, role_type, specialised)?;
    }

    Ok(())
}

fn add_typeql_plays(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    player_type: Vertex<Variable>,
    plays: &typeql::statement::type_::Plays,
) -> Result<(), Box<RepresentationError>> {
    let role_type = register_typeql_role_type(constraints, &plays.role)?;
    constraints.add_plays(player_type, role_type, plays.span())?;
    Ok(())
}

fn add_typeql_as(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    registered_specialising: Vertex<Variable>,
    specialised: &TypeRef,
) -> Result<(), Box<RepresentationError>> {
    let kind = SubKind::Subtype; // will read from the IR when "as!" is introduced
    let registered_specialised = register_typeql_role_type(constraints, specialised)?;
    constraints.add_sub(kind, registered_specialising, registered_specialised, specialised.span())?;
    Ok(())
}

fn add_typeql_value(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    attribute_type: Vertex<Variable>,
    typeql_value_type: &TypeQLValueType,
) -> Result<(), Box<RepresentationError>> {
    let value_type = register_typeql_value_type(constraints, typeql_value_type)?;
    constraints.add_value(attribute_type, value_type, typeql_value_type.span())?;
    Ok(())
}

fn add_typeql_isa(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: Variable,
    isa: &typeql::statement::thing::isa::Isa,
) -> Result<(), Box<RepresentationError>> {
    let kind = isa.kind.into();
    let type_ = register_typeql_type(constraints, &isa.type_)?;
    constraints.add_isa(kind, thing, type_, isa.span())?;
    if let Some(instance_constraint) = &isa.constraint {
        match instance_constraint {
            IsaInstanceConstraint::Relation(relation) => {
                add_typeql_relation(constraints, thing, relation)?;
            }
            IsaInstanceConstraint::Value(literal_value) => {
                let value = translate_literal(literal_value).map_err(|typedb_source| {
                    RepresentationError::LiteralParseError {
                        typedb_source,
                        literal: literal_value.to_string().clone(),
                        source_span: literal_value.span(),
                    }
                })?;
                let value_id = constraints
                    .parameters()
                    .register_value(value, literal_value.span().expect("Parser did not provide text range of value"));
                constraints.add_comparison(
                    Vertex::Variable(thing),
                    Vertex::Parameter(value_id),
                    Comparator::Equal,
                    literal_value.span(),
                )?;
            }
            IsaInstanceConstraint::Expression(expression) => {
                let assigned_to = add_typeql_expression(function_index, constraints, expression)?;
                constraints.add_comparison(
                    Vertex::Variable(thing),
                    assigned_to,
                    Comparator::Equal,
                    expression.span(),
                )?;
            }
            IsaInstanceConstraint::Comparison(comparison) => {
                let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
                let comparator = comparison.comparator.try_into().map_err(|typedb_source| {
                    Box::new(RepresentationError::LiteralParseError {
                        literal: comparison.comparator.to_string(),
                        source_span: comparison.span(),
                        typedb_source,
                    })
                })?;
                constraints.add_comparison(Vertex::Variable(thing), rhs_var, comparator, comparison.span())?;
            }
            IsaInstanceConstraint::Struct(_) => {
                return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                    feature: error::UnimplementedFeature::Structs,
                }));
            }
        }
    }
    Ok(())
}

fn parse_iid(mut iid: &str) -> ByteArray<THING_VERTEX_MAX_LENGTH> {
    fn from_hex(c: u8) -> u8 {
        // relying on the fact that typeql ensures only hex digits
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => unreachable!(),
        }
    }

    iid = &iid["0x".len()..];

    let mut bytes = [0u8; THING_VERTEX_MAX_LENGTH];
    for (i, (hi, lo)) in iid.bytes().tuples().enumerate() {
        bytes[i] = (from_hex(hi) << 4) + from_hex(lo);
    }
    let len = iid.as_bytes().len() / 2;
    ByteArray::inline(bytes, len)
}

fn add_typeql_iid(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    thing: Variable,
    iid: &typeql::statement::thing::Iid,
) -> Result<(), Box<RepresentationError>> {
    let iid_parameter = constraints
        .parameters()
        .register_iid(parse_iid(&iid.iid), iid.span().expect("Parser did not provide IID text range"));
    constraints.add_iid(thing, iid_parameter, iid.span())?;
    Ok(())
}

fn add_typeql_has(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    owner: Variable,
    has: &typeql::statement::thing::Has,
) -> Result<(), Box<RepresentationError>> {
    let attribute = match &has.value {
        typeql::statement::thing::HasValue::Variable(var) => register_typeql_var(constraints, var)?,
        typeql::statement::thing::HasValue::Expression(typeql_expression) => {
            let expression = add_typeql_expression(function_index, constraints, typeql_expression)?;
            let attribute = constraints.create_anonymous_variable(typeql_expression.span())?;
            constraints.add_comparison(
                Vertex::Variable(attribute),
                expression,
                Comparator::Equal,
                typeql_expression.span(),
            )?;
            attribute
        }
        typeql::statement::thing::HasValue::Comparison(comparison) => {
            let attribute = constraints.create_anonymous_variable(comparison.rhs.span())?;
            let rhs_var = add_typeql_expression(function_index, constraints, &comparison.rhs)?;
            let comparator = comparison.comparator.try_into().map_err(|typedb_source| {
                Box::new(RepresentationError::LiteralParseError {
                    literal: comparison.comparator.to_string(),
                    source_span: comparison.span(),
                    typedb_source,
                })
            })?;
            constraints.add_comparison(Vertex::Variable(attribute), rhs_var, comparator, comparison.span())?;
            attribute
        }
    };

    constraints.add_has(owner, attribute, has.span())?;
    if let Some(type_) = &has.type_ {
        let attribute_type = register_typeql_type_any(constraints, type_)?;
        constraints.add_isa(IsaKind::Subtype, attribute, attribute_type, type_.span())?;
    }
    Ok(())
}

pub(super) fn add_typeql_relation(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    relation: Variable,
    roleplayers: &typeql::statement::thing::Relation,
) -> Result<(), Box<RepresentationError>> {
    let mut links_constraints = Vec::new();
    for role_player in &roleplayers.role_players {
        match role_player {
            typeql::statement::thing::RolePlayer::Typed(type_ref, player_var) => {
                let type_ = match type_ref {
                    TypeRefAny::Type(TypeRef::Label(name)) => register_type_role_name_var(constraints, name)?,
                    TypeRefAny::Type(TypeRef::Variable(var)) => register_typeql_var(constraints, var)?,
                    TypeRefAny::Type(TypeRef::Scoped(name)) => {
                        return Err(Box::new(RepresentationError::ScopedRoleNameInRelation {
                            source_span: name.span(),
                        }));
                    }
                    TypeRefAny::List(_) => {
                        return Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                            feature: error::UnimplementedFeature::Lists,
                        }));
                    }
                };
                let player = register_typeql_var(constraints, player_var)?;
                let links = constraints.add_links(relation, player, type_, type_ref.span())?;
                links_constraints.push(links.clone());
            }
            typeql::statement::thing::RolePlayer::Untyped(var) => {
                let player = register_typeql_var(constraints, var)?;
                let role_type = constraints.create_anonymous_variable(var.span())?;
                let links = constraints.add_links(relation, player, role_type, var.span())?;
                links_constraints.push(links.clone());
            }
        }
    }
    for i in 0..links_constraints.len() {
        for j in (i + 1)..links_constraints.len() {
            constraints.as_links_deduplication(links_constraints[i].clone(), links_constraints[j].clone())?;
        }
    }

    Ok(())
}

fn add_typeql_iterable_binding(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assigned: Vec<Variable>,
    rhs: &typeql::Expression,
) -> Result<(), Box<RepresentationError>> {
    match rhs {
        typeql::Expression::Function(FunctionCall { name: FunctionName::Identifier(identifier), args, span }) => {
            add_user_defined_function_call(
                function_index,
                constraints,
                checked_identifier(identifier)?,
                assigned,
                args,
                *span,
            )
        }
        typeql::Expression::Function(FunctionCall { name: FunctionName::Builtin(_), .. }) => {
            Err(Box::new(RepresentationError::UnimplementedLanguageFeature {
                feature: UnimplementedFeature::LetInBuiltinCall,
            }))
        }
        typeql::Expression::List(_) | typeql::Expression::ListIndexRange(_) => {
            Err(Box::new(RepresentationError::UnimplementedLanguageFeature { feature: UnimplementedFeature::Lists }))
        }
        | typeql::Expression::Variable(_)
        | typeql::Expression::ListIndex(_)
        | typeql::Expression::Value(_)
        | typeql::Expression::Operation(_)
        | typeql::Expression::Paren(_) => unreachable!(),
    }
}
//
// // Helpers
// pub(super) fn add_function_call_binding_user(
//     function_index: &(impl FunctionSignatureIndex + std::fmt::Debug),
//     constraints: &mut ConstraintsBuilder<'_, '_>,
//     assigned: Vec<Variable>,
//     function_name: &str,
//     arguments: Vec<Variable>,
//     must_be_stream: bool,
// ) -> Result<(), Box<RepresentationError>> {
//     let function_opt = function_index
//         .get_function_signature(function_name)
//         .map_err(|typedb_source| RepresentationError::FunctionReadError { typedb_source })?;
//     if let Some(callee) = function_opt {
//         match (must_be_stream, callee.return_is_stream) {
//             (true, true) | (false, false) => {}
//             (false, true) => Err(RepresentationError::ExpectedSingleFunctionReturnsStream {
//                 function_name: function_name.to_owned(),
//                 source_span:
//             })?,
//             (true, false) => Err(RepresentationError::ExpectedStreamFunctionReturnsSingle {
//                 function_name: function_name.to_owned(),
//             })?,
//         }
//         constraints.add_function_binding(assigned, &callee, arguments, function_name)?;
//         Ok(())
//     } else {
//         Err(Box::new(RepresentationError::UnresolvedFunction { function_name: function_name.to_owned() }))
//     }
// }

fn assignment_pattern_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    assignment: &AssignmentPattern,
) -> Result<Vec<Variable>, Box<RepresentationError>> {
    match assignment {
        AssignmentPattern::Variables(vars) => assignment_typeql_vars_to_variables(constraints, vars),
        AssignmentPattern::Deconstruct(struct_deconstruct) => {
            Err(Box::new(RepresentationError::UnimplementedStructAssignment { source_span: struct_deconstruct.span() }))
        }
    }
}

fn assignment_typeql_vars_to_variables(
    constraints: &mut ConstraintsBuilder<'_, '_>,
    vars: &[typeql::Variable],
) -> Result<Vec<Variable>, Box<RepresentationError>> {
    vars.iter().map(|variable| register_typeql_var(constraints, variable)).collect()
}

pub(super) fn split_out_inline_expressions(
    function_index: &impl FunctionSignatureIndex,
    constraints: &mut ConstraintsBuilder<'_, '_>,
    expressions: &[typeql::Expression],
) -> Result<Vec<Variable>, Box<RepresentationError>> {
    expressions
        .iter()
        .map(|expr| {
            if let typeql::Expression::Variable(typeql_variable) = expr {
                Ok(register_typeql_var(constraints, typeql_variable)?)
            } else {
                let variable = constraints.create_anonymous_variable(expr.span())?;
                let expression = build_expression(function_index, constraints, expr)?;
                constraints.add_assignment(variable, expression, expr.span())?;
                Ok(variable)
            }
        })
        .collect()
}

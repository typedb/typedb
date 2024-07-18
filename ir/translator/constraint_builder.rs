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

pub struct TypeQLConstraintBuilder<'cx, FunctionIndex> {
    constraints: ConstraintsBuilder<'cx>,
    function_index: &'cx FunctionIndex,
}

impl<'cx, FunctionIndex: FunctionSignatureIndex> TypeQLConstraintBuilder<'cx, FunctionIndex> {
    pub(crate) fn new<'func>(
        constraints: ConstraintsBuilder<'cx>,
        function_index: &'cx FunctionIndex,
    ) -> TypeQLConstraintBuilder<'cx, FunctionIndex>
    where
        'func: 'cx,
    {
        Self { constraints, function_index }
    }

    pub(super) fn add_statement(&mut self, stmt: &typeql::Statement) -> Result<(), PatternDefinitionError> {
        match stmt {
            typeql::Statement::Is(_) => todo!(),
            typeql::Statement::InStream(in_stream) => self.add_in_stream_statement(in_stream)?,
            typeql::Statement::Comparison(_) => todo!(),
            typeql::Statement::Assignment(_) => todo!(),
            typeql::Statement::Thing(thing) => self.add_thing_statement(thing)?,
            typeql::Statement::AttributeValue(_) => todo!(),
            typeql::Statement::AttributeComparison(_) => todo!(),
            typeql::Statement::Type(type_) => self.add_type_statement(type_)?,
        }
        Ok(())
    }

    fn add_thing_statement(&mut self, thing: &typeql::statement::Thing) -> Result<(), PatternDefinitionError> {
        let var = match &thing.head {
            typeql::statement::thing::Head::Variable(var) => self.register_typeql_var(var)?,
            typeql::statement::thing::Head::Relation(rel) => {
                let relation = self.constraints.create_anonymous_variable()?;
                self.add_typeql_relation(relation, rel)?;
                relation
            }
        };
        for constraint in &thing.constraints {
            match constraint {
                typeql::statement::thing::Constraint::Isa(isa) => self.add_typeql_isa(var, isa)?,
                typeql::statement::thing::Constraint::Iid(_) => todo!(),
                typeql::statement::thing::Constraint::Has(has) => self.add_typeql_has(var, has)?,
                typeql::statement::thing::Constraint::Links(links) => self.add_typeql_relation(var, &links.relation)?,
            }
        }
        Ok(())
    }

    fn add_type_statement(&mut self, type_: &typeql::statement::Type) -> Result<(), PatternDefinitionError> {
        let var = self.register_typeql_type_var(&type_.type_)?;
        for constraint in &type_.constraints {
            assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
            match &constraint.base {
                typeql::statement::type_::ConstraintBase::Sub(_) => todo!(),
                typeql::statement::type_::ConstraintBase::Label(label) => match label {
                    typeql::statement::type_::LabelConstraint::Name(label) => {
                        self.constraints.add_label(var, label.as_str())?;
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
        &mut self,
        in_stream: &typeql::statement::InStream,
    ) -> Result<(), PatternDefinitionError> {
        let (function_name, function_opt) = match &in_stream.rhs.name {
            FunctionName::Builtin(_) => todo!("Is this legal?"),
            FunctionName::Identifier(identifier) => {
                let function_signature_opt = self
                    .function_index
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
            assigned.push(self.register_typeql_var(typeql_var)?);
        }
        let arguments: Vec<Variable> = in_stream
            .rhs
            .args
            .iter()
            .map(|arg| {
                self.extend_from_inline_typeql_expression(&arg) // Inline expressions must be converted to anonymous variables
            })
            .collect::<Result<Vec<_>, PatternDefinitionError>>()?;

        let as_ref = self.constraints.add_function_call(assigned, &callee_function, arguments)?;
        Ok(())
    }

    fn extend_from_inline_typeql_expression(
        &mut self,
        expression: &typeql::expression::Expression,
    ) -> Result<Variable, PatternDefinitionError> {
        let var = match expression {
            typeql::expression::Expression::Variable(typeql_var) => self.register_typeql_var(&typeql_var)?,
            _ => todo!(),
        };
        Ok(var)
    }

    fn register_typeql_var(&mut self, var: &typeql::Variable) -> Result<Variable, PatternDefinitionError> {
        match var {
            typeql::Variable::Named(_, name) => self.constraints.get_or_declare_variable_named(name.as_str()),
            typeql::Variable::Anonymous(_) => self.constraints.create_anonymous_variable(),
        }
    }

    fn register_typeql_type_var_any(&mut self, type_: &typeql::TypeAny) -> Result<Variable, PatternDefinitionError> {
        match type_ {
            typeql::TypeAny::Type(type_) => self.register_typeql_type_var(type_),
            typeql::TypeAny::Optional(_) => todo!(),
            typeql::TypeAny::List(_) => todo!(),
        }
    }

    fn register_typeql_type_var(&mut self, type_: &typeql::Type) -> Result<Variable, PatternDefinitionError> {
        match type_ {
            typeql::Type::Label(label) => self.register_type_label_var(label),
            typeql::Type::ScopedLabel(_) => todo!(),
            typeql::Type::Variable(var) => self.register_typeql_var(var),
            typeql::Type::BuiltinValue(_) => todo!(),
        }
    }

    fn register_type_label_var(&mut self, label: &typeql::Label) -> Result<Variable, PatternDefinitionError> {
        let var = self.constraints.create_anonymous_variable()?;
        match label {
            typeql::Label::Identifier(ident) => self.constraints.add_label(var, ident.as_str())?,
            typeql::Label::Reserved(reserved) => todo!("Unhandled builtin type: {reserved}"),
        };
        Ok(var)
    }

    fn add_typeql_isa(
        &mut self,
        thing: Variable,
        isa: &typeql::statement::thing::isa::Isa,
    ) -> Result<(), PatternDefinitionError> {
        let kind = match isa.kind {
            typeql::statement::thing::isa::IsaKind::Exact => IsaKind::Exact,
            typeql::statement::thing::isa::IsaKind::Subtype => IsaKind::Subtype,
        };
        let type_ = self.register_typeql_type_var(&isa.type_)?;
        self.constraints.add_isa(kind, thing, type_)?;
        Ok(())
    }

    fn add_typeql_has(
        &mut self,
        owner: Variable,
        has: &typeql::statement::thing::Has,
    ) -> Result<(), PatternDefinitionError> {
        let attr = match &has.value {
            typeql::statement::thing::HasValue::Variable(var) => var,
            typeql::statement::thing::HasValue::Expression(_) => todo!(),
            typeql::statement::thing::HasValue::Comparison(_) => todo!(),
        };
        let attribute = self.constraints.get_or_declare_variable_named(attr.name().unwrap())?;
        self.constraints.add_has(owner, attribute)?;
        if let Some(type_) = &has.type_ {
            let attribute_type = self.register_typeql_type_var_any(type_)?;
            self.constraints.add_isa(IsaKind::Subtype, attribute, attribute_type)?;
        }
        Ok(())
    }

    fn add_typeql_relation(
        &mut self,
        relation: Variable,
        roleplayers: &typeql::statement::thing::Relation,
    ) -> Result<(), PatternDefinitionError> {
        for role_player in &roleplayers.role_players {
            match role_player {
                typeql::statement::thing::RolePlayer::Typed(type_, var) => {
                    let player = self.register_typeql_var(var)?;
                    let type_ = self.register_typeql_type_var_any(type_)?;
                    self.constraints.add_role_player(relation, player, Some(type_))?;
                }
                typeql::statement::thing::RolePlayer::Untyped(var) => {
                    let player = self.register_typeql_var(var)?;
                    self.constraints.add_role_player(relation, player, None)?;
                }
            }
        }
        Ok(())
    }
}

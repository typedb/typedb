/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, iter::empty};

use answer::variable::Variable;
use itertools::Itertools;

use crate::{
    pattern::{
        expression::Expression,
        function_call::FunctionCall,
        variable_category::{VariableCategory, VariableOptionality},
        IrID, ScopeId,
    },
    program::block::BlockContext,
    PatternDefinitionError,
};

#[derive(Debug)]
pub struct Constraints {
    scope: ScopeId,
    constraints: Vec<Constraint<Variable>>,

    // TODO: could also store indexes into the Constraints vec? Depends how expensive Constraints are and if we delete
    left_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
    right_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
    filter_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
}

impl Constraints {
    pub(crate) fn new(scope: ScopeId) -> Self {
        Self {
            scope,
            constraints: Vec::new(),
            left_constrained_index: HashMap::new(),
            right_constrained_index: HashMap::new(),
            filter_constrained_index: HashMap::new(),
        }
    }

    pub(crate) fn constraints(&self) -> &[Constraint<Variable>] {
        &self.constraints
    }

    fn add_constraint(&mut self, constraint: impl Into<Constraint<Variable>>) -> &Constraint<Variable> {
        let constraint = constraint.into();
        constraint.ids_foreach(|var, side| match side {
            ConstraintIDSide::Left => self.left_constrained_index.entry(var).or_default().push(constraint.clone()),
            ConstraintIDSide::Right => self.right_constrained_index.entry(var).or_default().push(constraint.clone()),
            ConstraintIDSide::Filter => self.filter_constrained_index.entry(var).or_default().push(constraint.clone()),
        });
        self.constraints.push(constraint);
        self.constraints.last().unwrap()
    }

    pub fn add_label(
        &mut self,
        context: &mut BlockContext,
        variable: Variable,
        type_: &str,
    ) -> Result<&Label<Variable>, PatternDefinitionError> {
        debug_assert!(context.is_variable_available(self.scope, variable));
        let type_ = Label::new(variable, type_.to_string());
        context.set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.add_constraint(type_);
        Ok(as_ref.as_label().unwrap())
    }

    pub fn add_sub(
        &mut self,
        context: &mut BlockContext,
        subtype: Variable,
        supertype: Variable,
    ) -> Result<&Sub<Variable>, PatternDefinitionError> {
        debug_assert!(context.is_variable_available(self.scope, subtype));
        debug_assert!(context.is_variable_available(self.scope, supertype));
        let sub = Sub::new(subtype, supertype);
        context.set_variable_category(subtype, VariableCategory::Type, sub.clone().into())?;
        context.set_variable_category(supertype, VariableCategory::Type, sub.clone().into())?;
        let as_ref = self.add_constraint(sub);
        Ok(as_ref.as_sub().unwrap())
    }

    pub fn add_isa(
        &mut self,
        context: &mut BlockContext,
        kind: IsaKind,
        thing: Variable,
        type_: Variable,
    ) -> Result<&Isa<Variable>, PatternDefinitionError> {
        debug_assert!(
            context.is_variable_available(self.scope, thing) && context.is_variable_available(self.scope, type_)
        );
        let isa = Isa::new(kind, thing, type_);
        context.set_variable_category(thing, VariableCategory::Thing, isa.clone().into())?;
        context.set_variable_category(type_, VariableCategory::Type, isa.clone().into())?;
        let constraint = self.add_constraint(isa);
        Ok(constraint.as_isa().unwrap())
    }

    pub fn add_has(
        &mut self,
        context: &mut BlockContext,
        owner: Variable,
        attribute: Variable,
    ) -> Result<&Has<Variable>, PatternDefinitionError> {
        debug_assert!(
            context.is_variable_available(self.scope, owner) && context.is_variable_available(self.scope, attribute)
        );
        let has = Constraint::from(Has::new(owner, attribute));
        context.set_variable_category(owner, VariableCategory::Object, has.clone())?;
        context.set_variable_category(attribute, VariableCategory::Attribute, has.clone())?;
        let constraint = self.add_constraint(has);
        Ok(constraint.as_has().unwrap())
    }

    pub fn add_role_player_(
        &mut self,
        context: &mut BlockContext,
        relation: Variable,
        player: Variable,
        role_type: Option<Variable>,
    ) -> Result<&RolePlayer<Variable>, PatternDefinitionError> {
        debug_assert!(
            context.is_variable_available(self.scope, relation)
                && context.is_variable_available(self.scope, player)
                && !role_type.is_some_and(|role_type| !context.is_variable_available(self.scope, role_type))
        );
        let role_player = Constraint::from(RolePlayer::new(relation, player, role_type));
        context.set_variable_category(relation, VariableCategory::Object, role_player.clone())?;
        context.set_variable_category(player, VariableCategory::Object, role_player.clone())?;
        if let Some(role_type) = role_type {
            context.set_variable_category(role_type, VariableCategory::Type, role_player.clone())?;
        }
        let constraint = self.add_constraint(role_player);
        Ok(constraint.as_role_player().unwrap())
    }

    pub fn add_role_player(
        &mut self,
        context: &mut BlockContext,
        relation: Variable,
        player: Variable,
        role: Option<Variable>,
    ) -> Result<&RolePlayer<Variable>, PatternDefinitionError> {
        debug_assert!(
            context.is_variable_available(self.scope, relation)
                && context.is_variable_available(self.scope, player)
                && (role.is_none() || context.is_variable_available(self.scope, role.unwrap()))
        );
        let role_player = RolePlayer::new(relation, player, role);
        // TODO: Introduce relation category
        context.set_variable_category(relation, VariableCategory::Object, role_player.clone().into())?;
        context.set_variable_category(player, VariableCategory::Object, role_player.clone().into())?;
        if let Some(role_type) = role {
            context.set_variable_category(role_type, VariableCategory::RoleType, role_player.clone().into())?;
        }
        let as_ref = self.add_constraint(role_player);
        Ok(as_ref.as_role_player().unwrap())
    }

    pub fn add_comparison(
        &mut self,
        context: &mut BlockContext,
        lhs: Variable,
        rhs: Variable,
    ) -> Result<&Comparison<Variable>, PatternDefinitionError> {
        debug_assert!(context.is_variable_available(self.scope, lhs) && context.is_variable_available(self.scope, rhs));
        let comparison = Comparison::new(lhs, rhs);
        context.set_variable_category(lhs, VariableCategory::Value, comparison.clone().into())?;
        context.set_variable_category(rhs, VariableCategory::Value, comparison.clone().into())?;

        let as_ref = self.add_constraint(comparison);
        Ok(as_ref.as_comparison().unwrap())
    }

    pub fn add_function_call(
        &mut self,
        context: &mut BlockContext,
        assigned: Vec<Variable>,
        function_call: FunctionCall<Variable>,
    ) -> Result<&FunctionCallBinding<Variable>, PatternDefinitionError> {
        use PatternDefinitionError::FunctionCallReturnArgCountMismatch;
        debug_assert!(assigned.iter().all(|var| context.is_variable_available(self.scope, *var)));

        if assigned.len() != function_call.returns().len() {
            Err(FunctionCallReturnArgCountMismatch {
                assigned_var_count: assigned.len(),
                function_return_count: function_call.returns().len(),
            })?
        }

        let binding = FunctionCallBinding::new(assigned, function_call);

        for (index, var) in binding.ids_assigned().enumerate() {
            context.set_variable_category(var, binding.function_call().returns()[index].0, binding.clone().into())?;
            match binding.function_call.returns()[index].1 {
                VariableOptionality::Required => {}
                VariableOptionality::Optional => context.set_variable_is_optional(var),
            }
        }
        let as_ref = self.add_constraint(binding);
        Ok(as_ref.as_function_call_binding().unwrap())
    }

    pub fn add_expression(
        &mut self,
        context: &mut BlockContext,
        variable: Variable,
        expression: Expression<Variable>,
    ) -> Result<&ExpressionBinding<Variable>, PatternDefinitionError> {
        debug_assert!(context.is_variable_available(self.scope, variable));
        let binding = ExpressionBinding::new(variable, expression);
        context.set_variable_category(variable, VariableCategory::Value, binding.clone().into())?;
        let as_ref = self.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }
}

impl Constraints {
    pub(super) fn extend_from_typeql_statement(
        &mut self,
        context: &mut BlockContext,
        stmt: &typeql::Statement,
    ) -> Result<(), PatternDefinitionError> {
        match stmt {
            typeql::Statement::Is(_) => todo!(),
            typeql::Statement::InStream(_) => todo!(),
            typeql::Statement::Comparison(_) => todo!(),
            typeql::Statement::Assignment(_) => todo!(),
            typeql::Statement::Thing(thing) => self.extend_from_typeql_thing_statement(context, thing)?,
            typeql::Statement::AttributeValue(_) => todo!(),
            typeql::Statement::AttributeComparison(_) => todo!(),
            typeql::Statement::Type(type_) => self.extend_from_typeql_type_statement(context, type_)?,
        }
        Ok(())
    }

    fn extend_from_typeql_thing_statement(
        &mut self,
        context: &mut BlockContext,
        thing: &typeql::statement::Thing,
    ) -> Result<(), PatternDefinitionError> {
        let var = match &thing.head {
            typeql::statement::thing::Head::Variable(var) => self.register_typeql_var(context, var)?,
            typeql::statement::thing::Head::Relation(rel) => {
                let relation = context.create_anonymous_variable(self.scope)?;
                self.add_typeql_relation(context, relation, rel)?;
                relation
            }
        };
        for constraint in &thing.constraints {
            match constraint {
                typeql::statement::thing::Constraint::Isa(isa) => self.add_typeql_isa(context, var, isa)?,
                typeql::statement::thing::Constraint::Iid(_) => todo!(),
                typeql::statement::thing::Constraint::Has(has) => self.add_typeql_has(context, var, has)?,
                typeql::statement::thing::Constraint::Links(links) => {
                    self.add_typeql_relation(context, var, &links.relation)?
                }
            }
        }
        Ok(())
    }

    fn extend_from_typeql_type_statement(
        &mut self,
        context: &mut BlockContext,
        type_: &typeql::statement::Type,
    ) -> Result<(), PatternDefinitionError> {
        let var = self.register_typeql_type_var(context, &type_.type_)?;
        for constraint in &type_.constraints {
            assert!(constraint.annotations.is_empty(), "TODO: handle type statement annotations");
            match &constraint.base {
                typeql::statement::type_::ConstraintBase::Sub(_) => todo!(),
                typeql::statement::type_::ConstraintBase::Label(label) => match label {
                    typeql::statement::type_::LabelConstraint::Name(label) => {
                        self.add_label(context, var, label.as_str())?;
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

    fn register_typeql_var(
        &mut self,
        context: &mut BlockContext,
        var: &typeql::Variable,
    ) -> Result<Variable, PatternDefinitionError> {
        match var {
            typeql::Variable::Named(_, name) => context.get_or_declare_variable_named(name.as_str(), self.scope),
            typeql::Variable::Anonymous(_) => context.create_anonymous_variable(self.scope),
        }
    }

    fn register_typeql_type_var_any(
        &mut self,
        context: &mut BlockContext,
        type_: &typeql::TypeAny,
    ) -> Result<Variable, PatternDefinitionError> {
        match type_ {
            typeql::TypeAny::Type(type_) => self.register_typeql_type_var(context, type_),
            typeql::TypeAny::Optional(_) => todo!(),
            typeql::TypeAny::List(_) => todo!(),
        }
    }

    fn register_typeql_type_var(
        &mut self,
        context: &mut BlockContext,
        type_: &typeql::Type,
    ) -> Result<Variable, PatternDefinitionError> {
        match type_ {
            typeql::Type::Label(label) => self.register_type_label_var(context, label),
            typeql::Type::ScopedLabel(_) => todo!(),
            typeql::Type::Variable(var) => self.register_typeql_var(context, var),
            typeql::Type::BuiltinValue(_) => todo!(),
        }
    }

    fn register_type_label_var(
        &mut self,
        context: &mut BlockContext,
        label: &typeql::Label,
    ) -> Result<Variable, PatternDefinitionError> {
        let var = context.create_anonymous_variable(self.scope)?;
        match label {
            typeql::Label::Identifier(ident) => self.add_label(context, var, ident.as_str())?,
            typeql::Label::Reserved(reserved) => todo!("Unhandled builtin type: {reserved}"),
        };
        Ok(var)
    }

    fn add_typeql_isa(
        &mut self,
        context: &mut BlockContext,
        thing: Variable,
        isa: &typeql::statement::thing::isa::Isa,
    ) -> Result<(), PatternDefinitionError> {
        let kind = match isa.kind {
            typeql::statement::thing::isa::IsaKind::Exact => IsaKind::Exact,
            typeql::statement::thing::isa::IsaKind::Subtype => IsaKind::Subtype,
        };
        let type_ = self.register_typeql_type_var(context, &isa.type_)?;
        self.add_isa(context, kind, thing, type_)?;
        Ok(())
    }

    fn add_typeql_has(
        &mut self,
        context: &mut BlockContext,
        owner: Variable,
        has: &typeql::statement::thing::Has,
    ) -> Result<(), PatternDefinitionError> {
        let attr = match &has.value {
            typeql::statement::thing::HasValue::Variable(var) => var,
            typeql::statement::thing::HasValue::Expression(_) => todo!(),
            typeql::statement::thing::HasValue::Comparison(_) => todo!(),
        };
        let attribute = context.get_or_declare_variable_named(attr.name().unwrap(), self.scope)?;
        self.add_has(context, owner, attribute)?;
        if let Some(type_) = &has.type_ {
            let attribute_type = self.register_typeql_type_var_any(context, type_)?;
            self.add_isa(context, IsaKind::Subtype, attribute, attribute_type)?;
        }
        Ok(())
    }

    fn add_typeql_relation(
        &mut self,
        context: &mut BlockContext,
        relation: Variable,
        roleplayers: &typeql::statement::thing::Relation,
    ) -> Result<(), PatternDefinitionError> {
        for role_player in &roleplayers.role_players {
            match role_player {
                typeql::statement::thing::RolePlayer::Typed(type_, var) => {
                    let player = self.register_typeql_var(context, var)?;
                    let type_ = self.register_typeql_type_var_any(context, type_)?;
                    self.add_role_player(context, relation, player, Some(type_))?;
                }
                typeql::statement::thing::RolePlayer::Untyped(var) => {
                    let player = self.register_typeql_var(context, var)?;
                    self.add_role_player(context, relation, player, None)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for Constraints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for constraint in &self.constraints {
            let indent = (0..f.width().unwrap_or(0)).map(|_| " ").join("");
            writeln!(f, "{}{}", indent, constraint)?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Constraint<ID: IrID> {
    Label(Label<ID>),
    Sub(Sub<ID>),
    Isa(Isa<ID>),
    RolePlayer(RolePlayer<ID>),
    Has(Has<ID>),
    ExpressionBinding(ExpressionBinding<ID>),
    FunctionCallBinding(FunctionCallBinding<ID>),
    Comparison(Comparison<ID>),
}

impl<ID: IrID> Constraint<ID> {
    pub fn ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Label(label) => Box::new(label.ids()),
            Constraint::Sub(sub) => Box::new(sub.ids()),
            Constraint::Isa(isa) => Box::new(isa.ids()),
            Constraint::RolePlayer(rp) => Box::new(rp.ids()),
            Constraint::Has(has) => Box::new(has.ids()),
            Constraint::ExpressionBinding(binding) => todo!(),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::Comparison(comparison) => Box::new(comparison.ids()),
        }
    }

    pub fn ids_foreach<F>(&self, function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        match self {
            Constraint::Label(label) => label.ids_foreach(function),
            Constraint::Sub(sub) => sub.ids_foreach(function),
            Constraint::Isa(isa) => isa.ids_foreach(function),
            Constraint::RolePlayer(rp) => rp.ids_foreach(function),
            Constraint::Has(has) => has.ids_foreach(function),
            Constraint::ExpressionBinding(binding) => todo!(),
            Constraint::FunctionCallBinding(binding) => binding.ids_foreach(function),
            Constraint::Comparison(comparison) => comparison.ids_foreach(function),
        }
    }

    pub(crate) fn as_label(&self) -> Option<&Label<ID>> {
        match self {
            Constraint::Label(label) => Some(label),
            _ => None,
        }
    }

    pub(crate) fn as_sub(&self) -> Option<&Sub<ID>> {
        match self {
            Constraint::Sub(sub) => Some(sub),
            _ => None,
        }
    }

    pub(crate) fn as_isa(&self) -> Option<&Isa<ID>> {
        match self {
            Constraint::Isa(isa) => Some(isa),
            _ => None,
        }
    }

    pub(crate) fn as_role_player(&self) -> Option<&RolePlayer<ID>> {
        match self {
            Constraint::RolePlayer(rp) => Some(rp),
            _ => None,
        }
    }

    pub(crate) fn as_has(&self) -> Option<&Has<ID>> {
        match self {
            Constraint::Has(has) => Some(has),
            _ => None,
        }
    }

    pub(crate) fn as_comparison(&self) -> Option<&Comparison<ID>> {
        match self {
            Constraint::Comparison(cmp) => Some(cmp),
            _ => None,
        }
    }

    pub(crate) fn as_function_call_binding(&self) -> Option<&FunctionCallBinding<ID>> {
        match self {
            Constraint::FunctionCallBinding(binding) => Some(binding),
            _ => None,
        }
    }

    pub(crate) fn as_expression_binding(&self) -> Option<&ExpressionBinding<ID>> {
        match self {
            Constraint::ExpressionBinding(binding) => Some(binding),
            _ => None,
        }
    }
}

impl<ID: IrID> fmt::Display for Constraint<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Label(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Sub(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Isa(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::RolePlayer(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Has(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::ExpressionBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::FunctionCallBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Comparison(constraint) => fmt::Display::fmt(constraint, f),
        }
    }
}

pub enum ConstraintIDSide {
    Left,
    Right,
    Filter,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Label<ID: IrID> {
    pub(crate) left: ID,
    pub(crate) type_: String,
}

impl<ID: IrID> Label<ID> {
    fn new(identifier: ID, type_: String) -> Self {
        Self { left: identifier, type_ }
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.left, ConstraintIDSide::Left)
    }
}

impl<ID: IrID> From<Label<ID>> for Constraint<ID> {
    fn from(val: Label<ID>) -> Self {
        Constraint::Label(val)
    }
}

impl<ID: IrID> fmt::Display for Label<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: implement indentation without rewriting it everywhere
        // write!(f, "{: >width$} {} type {}", "", self.left, self.type_, width=f.width().unwrap_or(0))
        write!(f, "{} label {}", self.left, self.type_)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Sub<ID: IrID> {
    subtype: ID,
    supertype: ID,
}

impl<ID: IrID> Sub<ID> {
    fn new(subtype: ID, supertype: ID) -> Self {
        Sub { subtype, supertype }
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.subtype, self.supertype].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.subtype, ConstraintIDSide::Left);
        function(self.supertype, ConstraintIDSide::Right)
    }

    pub(crate) fn subtype(&self) -> ID {
        self.subtype
    }

    pub(crate) fn supertype(&self) -> ID {
        self.supertype
    }
}

impl<ID: IrID> From<Sub<ID>> for Constraint<ID> {
    fn from(val: Sub<ID>) -> Self {
        Constraint::Sub(val)
    }
}

impl<ID: IrID> fmt::Display for Sub<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} isa {}", self.subtype, self.supertype)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Isa<ID: IrID> {
    kind: IsaKind,
    thing: ID,
    type_: ID,
}

impl<ID: IrID> Isa<ID> {
    fn new(kind: IsaKind, thing: ID, type_: ID) -> Self {
        Self { kind, thing, type_ }
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.thing, self.type_].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.thing, ConstraintIDSide::Left);
        function(self.type_, ConstraintIDSide::Right)
    }

    pub(crate) fn thing(&self) -> ID {
        self.thing
    }

    pub(crate) fn type_(&self) -> ID {
        self.type_
    }
}

impl<ID: IrID> From<Isa<ID>> for Constraint<ID> {
    fn from(val: Isa<ID>) -> Self {
        Constraint::Isa(val)
    }
}

impl<ID: IrID> fmt::Display for Isa<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} isa {}", self.thing, self.type_)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum IsaKind {
    Exact,
    Subtype,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RolePlayer<ID: IrID> {
    pub(crate) relation: ID,
    pub(crate) player: ID,
    pub(crate) role_type: Option<ID>,
}

impl<ID: IrID> RolePlayer<ID> {
    pub fn new(relation: ID, player: ID, role_type: Option<ID>) -> Self {
        Self { relation, player, role_type }
    }

    pub fn relation(&self) -> ID {
        self.relation
    }

    pub fn player(&self) -> ID {
        self.player
    }

    pub fn role_type(&self) -> Option<ID> {
        self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.relation, self.player].into_iter().chain(self.role_type)
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.relation, ConstraintIDSide::Left);
        function(self.player, ConstraintIDSide::Right);
        if let Some(role) = self.role_type {
            function(role, ConstraintIDSide::Filter);
        }
    }

    pub fn into_ids<T: IrID>(self, mapping: &HashMap<ID, T>) -> RolePlayer<T> {
        RolePlayer::new(
            *mapping.get(&self.relation).unwrap(),
            *mapping.get(&self.player).unwrap(),
            self.role_type.map(|rt| *mapping.get(&rt).unwrap()),
        )
    }
}

impl<ID: IrID> From<RolePlayer<ID>> for Constraint<ID> {
    fn from(role_player: RolePlayer<ID>) -> Self {
        Constraint::RolePlayer(role_player)
    }
}

impl<ID: IrID> fmt::Display for RolePlayer<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.role_type {
            None => {
                write!(f, "{} rp {} (role: )", self.relation, self.player)
            }
            Some(role) => {
                write!(f, "{} rp {} (role: {})", self.relation, self.player, role)
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Has<ID: IrID> {
    owner: ID,
    attribute: ID,
}

impl<ID: IrID> Has<ID> {
    pub fn new(owner: ID, attribute: ID) -> Self {
        Has { owner, attribute }
    }

    pub fn owner(&self) -> ID {
        self.owner
    }

    pub fn attribute(&self) -> ID {
        self.attribute
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.owner, self.attribute].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.owner, ConstraintIDSide::Left);
        function(self.attribute, ConstraintIDSide::Right);
    }

    pub fn into_ids<T: IrID>(self, mapping: &HashMap<ID, T>) -> Has<T> {
        Has::new(*mapping.get(&self.owner).unwrap(), *mapping.get(&self.attribute).unwrap())
    }
}

impl<ID: IrID> From<Has<ID>> for Constraint<ID> {
    fn from(has: Has<ID>) -> Self {
        Constraint::Has(has)
    }
}

impl<ID: IrID> fmt::Display for Has<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} has {}", self.owner, self.attribute)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionBinding<ID: IrID> {
    left: ID,
    expression: Expression<ID>,
}

impl<ID: IrID> ExpressionBinding<ID> {
    fn new(left: ID, expression: Expression<ID>) -> Self {
        Self { left, expression }
    }

    fn ids(&self) -> impl Iterator<Item = ID> {
        panic!("Unimplemented");
        empty()
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> {
        [self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        todo!()
    }
}

impl<ID: IrID> From<ExpressionBinding<ID>> for Constraint<ID> {
    fn from(val: ExpressionBinding<ID>) -> Self {
        Constraint::ExpressionBinding(val)
    }
}

impl<ID: IrID> fmt::Display for ExpressionBinding<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.left, self.expression)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FunctionCallBinding<ID: IrID> {
    assigned: Vec<ID>,
    function_call: FunctionCall<ID>,
}

impl<ID: IrID> FunctionCallBinding<ID> {
    fn new(left: Vec<ID>, function_call: FunctionCall<ID>) -> Self {
        Self { assigned: left, function_call }
    }

    pub(crate) fn assigned(&self) -> &Vec<ID> {
        &self.assigned
    }

    pub(crate) fn function_call(&self) -> &FunctionCall<ID> {
        &self.function_call
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        panic!("Unimplemented");
        empty()
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> + '_ {
        self.assigned.iter().cloned()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        for id in &self.assigned {
            function(*id, ConstraintIDSide::Left)
        }

        for id in self.function_call.call_id_mapping().keys() {
            function(*id, ConstraintIDSide::Right)
        }
    }
}

impl<ID: IrID> From<FunctionCallBinding<ID>> for Constraint<ID> {
    fn from(val: FunctionCallBinding<ID>) -> Self {
        Constraint::FunctionCallBinding(val)
    }
}

impl<ID: IrID> fmt::Display for FunctionCallBinding<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.function_call.return_is_stream() {
            write!(f, "{} in {}", self.ids_assigned().map(|i| i.to_string()).join(", "), self.function_call())
        } else {
            write!(f, "{} = {}", self.ids_assigned().map(|i| i.to_string()).join(", "), self.function_call())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Comparison<ID: IrID> {
    lhs: ID,
    rhs: ID,
    // comparator: Comparator,
}

impl<ID: IrID> Comparison<ID> {
    fn new(lhs: ID, rhs: ID) -> Self {
        Self { lhs, rhs }
    }

    pub fn lhs(&self) -> ID {
        self.lhs
    }

    pub fn rhs(&self) -> ID {
        self.rhs
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.lhs, self.rhs].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.lhs, ConstraintIDSide::Left);
        function(self.rhs, ConstraintIDSide::Right);
    }
}

impl<ID: IrID> From<Comparison<ID>> for Constraint<ID> {
    fn from(val: Comparison<ID>) -> Self {
        Constraint::Comparison(val)
    }
}

impl<ID: IrID> fmt::Display for Comparison<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

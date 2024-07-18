/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt,
    iter::{empty, zip},
};

use answer::variable::Variable;
use itertools::Itertools;
use typeql;

use crate::{
    pattern::{
        expression::Expression,
        function_call::FunctionCall,
        variable_category::{VariableCategory, VariableOptionality},
        IrID, ScopeId,
    },
    program::{block::BlockContext, function_signature::FunctionSignature},
    PatternDefinitionError,
    PatternDefinitionError::FunctionCallArgumentCountMismatch,
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

    pub(crate) fn scope(&self) -> ScopeId {
        self.scope
    }

    pub fn constraints(&self) -> &[Constraint<Variable>] {
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

pub struct ConstraintsBuilder<'cx> {
    context: &'cx mut BlockContext,
    constraints: &'cx mut Constraints,
}

impl<'cx> ConstraintsBuilder<'cx> {
    pub fn new(context: &'cx mut BlockContext, constraints: &'cx mut Constraints) -> Self {
        Self { context, constraints }
    }

    pub fn add_label(&mut self, variable: Variable, type_: &str) -> Result<&Label<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let type_ = Label::new(variable, type_.to_string());
        self.context.set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.constraints.add_constraint(type_);
        Ok(as_ref.as_label().unwrap())
    }

    pub fn add_sub(
        &mut self,
        subtype: Variable,
        supertype: Variable,
    ) -> Result<&Sub<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, subtype));
        debug_assert!(self.context.is_variable_available(self.constraints.scope, supertype));
        let sub = Sub::new(subtype, supertype);
        self.context.set_variable_category(subtype, VariableCategory::Type, sub.clone().into())?;
        self.context.set_variable_category(supertype, VariableCategory::Type, sub.clone().into())?;
        let as_ref = self.constraints.add_constraint(sub);
        Ok(as_ref.as_sub().unwrap())
    }

    pub fn add_isa(
        &mut self,
        kind: IsaKind,
        thing: Variable,
        type_: Variable,
    ) -> Result<&Isa<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, thing)
                && self.context.is_variable_available(self.constraints.scope, type_)
        );
        let isa = Isa::new(kind, thing, type_);
        self.context.set_variable_category(thing, VariableCategory::Thing, isa.clone().into())?;
        self.context.set_variable_category(type_, VariableCategory::Type, isa.clone().into())?;
        let constraint = self.constraints.add_constraint(isa);
        Ok(constraint.as_isa().unwrap())
    }

    pub fn add_has(&mut self, owner: Variable, attribute: Variable) -> Result<&Has<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, owner)
                && self.context.is_variable_available(self.constraints.scope, attribute)
        );
        let has = Constraint::from(Has::new(owner, attribute));
        self.context.set_variable_category(owner, VariableCategory::Object, has.clone())?;
        self.context.set_variable_category(attribute, VariableCategory::Attribute, has.clone())?;
        let constraint = self.constraints.add_constraint(has);
        Ok(constraint.as_has().unwrap())
    }

    pub fn add_role_player(
        &mut self,
        relation: Variable,
        player: Variable,
        role_type: Option<Variable>,
    ) -> Result<&RolePlayer<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, relation)
                && self.context.is_variable_available(self.constraints.scope, player)
                && !role_type
                    .is_some_and(|role_type| !self.context.is_variable_available(self.constraints.scope, role_type))
        );
        let role_player = Constraint::from(RolePlayer::new(relation, player, role_type));
        self.context.set_variable_category(relation, VariableCategory::Object, role_player.clone())?;
        self.context.set_variable_category(player, VariableCategory::Object, role_player.clone())?;
        if let Some(role_type) = role_type {
            self.context.set_variable_category(role_type, VariableCategory::Type, role_player.clone())?;
        }
        let constraint = self.constraints.add_constraint(role_player);
        Ok(constraint.as_role_player().unwrap())
    }

    pub fn add_comparison(
        &mut self,
        lhs: Variable,
        rhs: Variable,
    ) -> Result<&Comparison<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, lhs)
                && self.context.is_variable_available(self.constraints.scope, rhs)
        );
        let comparison = Comparison::new(lhs, rhs);
        self.context.set_variable_category(lhs, VariableCategory::Value, comparison.clone().into())?;
        self.context.set_variable_category(rhs, VariableCategory::Value, comparison.clone().into())?;

        let as_ref = self.constraints.add_constraint(comparison);
        Ok(as_ref.as_comparison().unwrap())
    }

    pub fn add_function_call(
        &mut self,
        assigned: Vec<Variable>,
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
    ) -> Result<&FunctionCallBinding<Variable>, PatternDefinitionError> {
        use PatternDefinitionError::{
            FunctionCallReturnArgCountMismatch, FunctionRequiredArgumentReceivedOptionalVariable,
        };
        debug_assert!(assigned.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));
        debug_assert!(arguments.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));

        // Validate
        if assigned.len() != callee_signature.returns.len() {
            Err(FunctionCallReturnArgCountMismatch {
                assigned_var_count: assigned.len(),
                function_return_count: callee_signature.returns.len(),
            })?
        }
        if arguments.len() != callee_signature.arguments.len() {
            Err(FunctionCallArgumentCountMismatch {
                expected: callee_signature.arguments.len(),
                actual: arguments.len(),
            })?
        }
        let unsatisfied_required_argument =
            zip(arguments.iter(), callee_signature.arguments.iter()).find(|(caller_var, callee_argument)| {
                self.context.is_variable_optional(**caller_var) && VariableOptionality::Required == callee_argument.1
            });
        if let Some((variable, _)) = unsatisfied_required_argument {
            Err(FunctionRequiredArgumentReceivedOptionalVariable { variable: *variable })?
        }

        // Construct
        let call_variable_mapping =
            arguments.iter().enumerate().map(|(index, variable)| (variable.clone(), index)).collect();
        let function_call = FunctionCall::new(
            callee_signature.function_id.clone(),
            call_variable_mapping,
            callee_signature.return_is_stream,
        );
        let binding = FunctionCallBinding::new(assigned, function_call);

        for (index, var) in binding.ids_assigned().enumerate() {
            self.context.set_variable_category(var, callee_signature.returns[index].0, binding.clone().into())?;
        }

        for (caller_var, callee_arg_index) in binding.function_call.call_id_mapping() {
            self.context.set_variable_category(
                *caller_var,
                callee_signature.arguments[*callee_arg_index].0,
                binding.clone().into(),
            )?;
        }

        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_function_call_binding().unwrap())
    }

    pub fn add_expression(
        &mut self,
        variable: Variable,
        expression: Expression<Variable>,
    ) -> Result<&ExpressionBinding<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let binding = ExpressionBinding::new(variable, expression);
        self.context.set_variable_category(variable, VariableCategory::Value, binding.clone().into())?;
        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }

    pub(crate) fn create_anonymous_variable(&mut self) -> Result<Variable, PatternDefinitionError> {
        self.context.create_anonymous_variable(self.constraints.scope)
    }

    pub(crate) fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, PatternDefinitionError> {
        self.context.get_or_declare_variable(name, self.constraints.scope)
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

    pub fn thing(&self) -> ID {
        self.thing
    }

    pub fn type_(&self) -> ID {
        self.type_
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

    pub fn into_ids<T: IrID>(self, mapping: &HashMap<ID, T>) -> Isa<T> {
        Isa::new(self.kind, *mapping.get(&self.thing).unwrap(), *mapping.get(&self.type_).unwrap())
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

    pub fn expression(&self) -> &Expression<ID> {
        &self.expression
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> {
        [self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        self.ids_assigned().for_each(|id| function(id, ConstraintIDSide::Left));
        self.expression().ids().for_each(|id| function(id, ConstraintIDSide::Right));
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

    pub fn function_call(&self) -> &FunctionCall<ID> {
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

        for id in self.function_call.argument_ids() {
            function(id, ConstraintIDSide::Right)
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

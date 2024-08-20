/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use itertools::Itertools;

use crate::{
    pattern::{
        expression::{ExpressionDefinitionError, ExpressionTree},
        function_call::FunctionCall,
        variable_category::VariableCategory,
        IrID, ScopeId,
    },
    program::{block::BlockContext, function_signature::FunctionSignature},
    PatternDefinitionError,
    PatternDefinitionError::FunctionCallArgumentCountMismatch,
};

#[derive(Debug, Clone)]
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
        // TODO: ids_foreach is only used here, and ids is unused. Do we need these methods?
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
            let indent = " ".repeat(f.width().unwrap_or(0));
            writeln!(f, "{}{}", indent, constraint)?
        }
        Ok(())
    }
}

pub struct ConstraintsBuilder<'cx, 'reg> {
    context: &'cx mut BlockContext<'reg>,
    constraints: &'cx mut Constraints,
}

impl<'cx, 'reg> ConstraintsBuilder<'cx, 'reg> {
    pub fn new(context: &'cx mut BlockContext<'reg>, constraints: &'cx mut Constraints) -> Self {
        Self { context, constraints }
    }

    pub fn add_label(&mut self, variable: Variable, type_: &str) -> Result<&Label<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let type_ = Label::new(variable, type_.to_string());
        self.context.set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.constraints.add_constraint(type_);
        Ok(as_ref.as_label().unwrap())
    }

    pub fn add_role_name(
        &mut self,
        variable: Variable,
        name: &str,
    ) -> Result<&RoleName<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let role_name = RoleName::new(variable, name.to_owned());
        self.context.set_variable_category(variable, VariableCategory::Type, role_name.clone().into())?;
        let as_ref = self.constraints.add_constraint(role_name);
        Ok(as_ref.as_role_name().unwrap())
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

    pub fn add_links(
        &mut self,
        relation: Variable,
        player: Variable,
        role_type: Variable,
    ) -> Result<&Links<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, relation)
                && self.context.is_variable_available(self.constraints.scope, player)
                && self.context.is_variable_available(self.constraints.scope, role_type)
        );
        let links = Constraint::from(Links::new(relation, player, role_type));
        self.context.set_variable_category(relation, VariableCategory::Object, links.clone())?;
        self.context.set_variable_category(player, VariableCategory::Object, links.clone())?;
        self.context.set_variable_category(role_type, VariableCategory::RoleType, links.clone())?;
        let constraint = self.constraints.add_constraint(links);
        Ok(constraint.as_links().unwrap())
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
        // todo!("The above lines were the two lines below");
        // self.context.set_variable_category(lhs, VariableCategory::AttributeOrValue, comparison.clone().into())?;
        // self.context.set_variable_category(rhs, VariableCategory::AttributeOrValue, comparison.clone().into())?;

        let as_ref = self.constraints.add_constraint(comparison);
        Ok(as_ref.as_comparison().unwrap())
    }

    pub fn add_function_binding(
        &mut self,
        assigned: Vec<Variable>,
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
    ) -> Result<&FunctionCallBinding<Variable>, PatternDefinitionError> {
        let function_call = self.create_function_call(&assigned, callee_signature, arguments)?;
        let binding = FunctionCallBinding::new(assigned, function_call, callee_signature.return_is_stream);
        for (index, var) in binding.ids_assigned().enumerate() {
            self.context.set_variable_category(var, callee_signature.returns[index].0, binding.clone().into())?;
        }
        for (caller_var, callee_arg_index) in binding.function_call.call_id_mapping() {
            self.context.set_variable_category(
                *caller_var,
                callee_signature.arguments[*callee_arg_index],
                binding.clone().into(),
            )?;
        }
        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_function_call_binding().unwrap())
    }

    fn create_function_call(
        &mut self,
        assigned: &[Variable],
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
    ) -> Result<FunctionCall<Variable>, PatternDefinitionError> {
        use PatternDefinitionError::FunctionCallReturnCountMismatch;
        debug_assert!(assigned.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));
        debug_assert!(arguments.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));

        // Validate
        if assigned.len() != callee_signature.returns.len() {
            Err(FunctionCallReturnCountMismatch {
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

        // Construct
        let call_variable_mapping = arguments.iter().enumerate().map(|(index, variable)| (*variable, index)).collect();
        Ok(FunctionCall::new(callee_signature.function_id.clone(), call_variable_mapping))
    }

    pub fn add_expression(
        &mut self,
        variable: Variable,
        expression: ExpressionTree<Variable>,
    ) -> Result<&ExpressionBinding<Variable>, PatternDefinitionError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let binding = ExpressionBinding::new(variable, expression);
        binding
            .validate(&mut self.context)
            .map_err(|source| PatternDefinitionError::ExpressionDefinition { source })?;
        // WARNING: we can't set a variable category here, since we don't know if the instruction will produce a
        //          Value, a ValueList, or a ThingList! We will know this at compilation time
        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }

    pub fn add_owns(
        &mut self,
        owner_type: Variable,
        attribute_type: Variable,
    ) -> Result<&Owns<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, owner_type)
                && self.context.is_variable_available(self.constraints.scope, attribute_type)
        );
        let has = Constraint::from(Owns::new(owner_type, attribute_type));
        self.context.set_variable_category(owner_type, VariableCategory::ThingType, has.clone())?;
        self.context.set_variable_category(attribute_type, VariableCategory::ThingType, has.clone())?;
        let constraint = self.constraints.add_constraint(has);
        Ok(constraint.as_owns().unwrap())
    }

    pub fn add_relates(
        &mut self,
        relation_type: Variable,
        role_type: Variable,
    ) -> Result<&Relates<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, relation_type)
                && self.context.is_variable_available(self.constraints.scope, role_type)
        );
        let relates = Constraint::from(Relates::new(relation_type, role_type));
        self.context.set_variable_category(relation_type, VariableCategory::ThingType, relates.clone())?;
        self.context.set_variable_category(role_type, VariableCategory::RoleType, relates.clone())?;
        let constraint = self.constraints.add_constraint(relates);
        Ok(constraint.as_relates().unwrap())
    }

    pub fn add_plays(
        &mut self,
        player_type: Variable,
        role_type: Variable,
    ) -> Result<&Plays<Variable>, PatternDefinitionError> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, player_type)
                && self.context.is_variable_available(self.constraints.scope, role_type)
        );
        let relates = Constraint::from(Plays::new(player_type, role_type));
        self.context.set_variable_category(player_type, VariableCategory::ThingType, relates.clone())?;
        self.context.set_variable_category(role_type, VariableCategory::RoleType, relates.clone())?;
        let constraint = self.constraints.add_constraint(relates);
        Ok(constraint.as_plays().unwrap())
    }

    pub(crate) fn create_anonymous_variable(&mut self) -> Result<Variable, PatternDefinitionError> {
        self.context.create_anonymous_variable(self.constraints.scope)
    }

    pub(crate) fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, PatternDefinitionError> {
        self.context.get_or_declare_variable(name, self.constraints.scope)
    }

    pub(crate) fn set_variable_optionality(&mut self, variable: Variable, optional: bool) {
        self.context.set_variable_is_optional(variable, optional)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Constraint<ID: IrID> {
    Label(Label<ID>),
    RoleName(RoleName<ID>),
    Sub(Sub<ID>),
    Isa(Isa<ID>),
    Links(Links<ID>),
    Has(Has<ID>),
    ExpressionBinding(ExpressionBinding<ID>),
    FunctionCallBinding(FunctionCallBinding<ID>),
    Comparison(Comparison<ID>),
    Owns(Owns<ID>),
    Relates(Relates<ID>),
    Plays(Plays<ID>),
}

impl<ID: IrID> Constraint<ID> {
    pub fn ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Label(label) => Box::new(label.ids()),
            Constraint::RoleName(role_name) => Box::new(role_name.ids()),
            Constraint::Sub(sub) => Box::new(sub.ids()),
            Constraint::Isa(isa) => Box::new(isa.ids()),
            Constraint::Links(rp) => Box::new(rp.ids()),
            Constraint::Has(has) => Box::new(has.ids()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::Comparison(comparison) => Box::new(comparison.ids()),
            Constraint::Owns(owns) => Box::new(owns.ids()),
            Constraint::Relates(relates) => Box::new(relates.ids()),
            Constraint::Plays(plays) => Box::new(plays.ids()),
        }
    }

    pub fn ids_foreach<F>(&self, function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        match self {
            Constraint::Label(label) => label.ids_foreach(function),
            Constraint::RoleName(role_name) => role_name.ids_foreach(function),
            Constraint::Sub(sub) => sub.ids_foreach(function),
            Constraint::Isa(isa) => isa.ids_foreach(function),
            Constraint::Links(rp) => rp.ids_foreach(function),
            Constraint::Has(has) => has.ids_foreach(function),
            Constraint::ExpressionBinding(binding) => binding.ids_foreach(function),
            Constraint::FunctionCallBinding(binding) => binding.ids_foreach(function),
            Constraint::Comparison(comparison) => comparison.ids_foreach(function),
            Constraint::Owns(owns) => owns.ids_foreach(function),
            Constraint::Relates(relates) => relates.ids_foreach(function),
            Constraint::Plays(plays) => plays.ids_foreach(function),
        }
    }

    pub fn ids_count(&self) -> usize {
        let mut count = 0;
        self.ids_foreach(|_, _| count += 1);
        count
    }

    pub fn left_id(&self) -> ID {
        let mut id = None;
        self.ids_foreach(|constraint_id, side| {
            if side == ConstraintIDSide::Left {
                id = Some(constraint_id);
            }
        });
        id.unwrap()
    }

    pub fn right_id(&self) -> ID {
        let mut id = None;
        self.ids_foreach(|constraint_id, side| {
            if side == ConstraintIDSide::Right {
                id = Some(constraint_id);
            }
        });
        id.unwrap()
    }

    pub(crate) fn as_label(&self) -> Option<&Label<ID>> {
        match self {
            Constraint::Label(label) => Some(label),
            _ => None,
        }
    }

    pub(crate) fn as_role_name(&self) -> Option<&RoleName<ID>> {
        match self {
            Constraint::RoleName(role_name) => Some(role_name),
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

    pub(crate) fn as_links(&self) -> Option<&Links<ID>> {
        match self {
            Constraint::Links(rp) => Some(rp),
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

    pub fn as_expression_binding(&self) -> Option<&ExpressionBinding<ID>> {
        match self {
            Constraint::ExpressionBinding(binding) => Some(binding),
            _ => None,
        }
    }

    pub(crate) fn as_owns(&self) -> Option<&Owns<ID>> {
        match self {
            Constraint::Owns(owns) => Some(owns),
            _ => None,
        }
    }

    pub(crate) fn as_relates(&self) -> Option<&Relates<ID>> {
        match self {
            Constraint::Relates(relates) => Some(relates),
            _ => None,
        }
    }

    pub(crate) fn as_plays(&self) -> Option<&Plays<ID>> {
        match self {
            Constraint::Plays(plays) => Some(plays),
            _ => None,
        }
    }
}

impl<ID: IrID> fmt::Display for Constraint<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Label(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::RoleName(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Sub(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Isa(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Links(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Has(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::ExpressionBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::FunctionCallBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Comparison(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Owns(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Relates(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Plays(constraint) => fmt::Display::fmt(constraint, f),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ConstraintIDSide {
    Left,
    Right,
    Filter,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Label<ID: IrID> {
    left: ID,
    type_label: String,
}

impl<ID: IrID> Label<ID> {
    fn new(identifier: ID, type_: String) -> Self {
        Self { left: identifier, type_label: type_ }
    }

    pub fn left(&self) -> ID {
        self.left
    }

    pub fn type_label(&self) -> &str {
        &self.type_label
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
        write!(f, "{} label {}", self.left, self.type_label)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RoleName<ID> {
    left: ID,
    name: String,
}

impl<ID: IrID> RoleName<ID> {
    pub fn new(left: ID, name: String) -> Self {
        Self { left, name }
    }

    pub fn left(&self) -> ID {
        self.left
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
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

impl<ID: IrID> From<RoleName<ID>> for Constraint<ID> {
    fn from(value: RoleName<ID>) -> Self {
        Constraint::RoleName(value)
    }
}

impl<ID: IrID> fmt::Display for RoleName<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} role-name {}", self.left, &self.name)
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

    pub fn subtype(&self) -> ID {
        self.subtype
    }

    pub fn supertype(&self) -> ID {
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
pub struct Isa<ID> {
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

    pub fn isa_kind(&self) -> IsaKind {
        self.kind
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.thing, self.type_].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.thing, ConstraintIDSide::Left);
        function(self.type_, ConstraintIDSide::Right);
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Isa<T> {
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
pub struct Links<ID> {
    pub(crate) relation: ID,
    pub(crate) player: ID,
    pub(crate) role_type: ID,
}

impl<ID: IrID> Links<ID> {
    pub fn new(relation: ID, player: ID, role_type: ID) -> Self {
        Self { relation, player, role_type }
    }

    pub fn relation(&self) -> ID {
        self.relation
    }

    pub fn player(&self) -> ID {
        self.player
    }

    pub fn role_type(&self) -> ID {
        self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.relation, self.player, self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.relation, ConstraintIDSide::Left);
        function(self.player, ConstraintIDSide::Right);
        function(self.role_type, ConstraintIDSide::Filter);
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Links<T> {
        Links::new(
            *mapping.get(&self.relation).unwrap(),
            *mapping.get(&self.player).unwrap(),
            *mapping.get(&self.role_type).unwrap(),
        )
    }
}

impl<ID: IrID> From<Links<ID>> for Constraint<ID> {
    fn from(links: Links<ID>) -> Self {
        Constraint::Links(links)
    }
}

impl<ID: IrID> fmt::Display for Links<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} rp {} (role: {})", self.relation, self.player, self.role_type)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Has<ID> {
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

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Has<T> {
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
    expression: ExpressionTree<ID>,
}

impl<ID: IrID> ExpressionBinding<ID> {
    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> {
        [self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        self.ids_assigned().for_each(|id| function(id, ConstraintIDSide::Left));
        // TODO
        // todo!("Do we really need positions here?")
        // self.instruction().ids().for_each(|id| function(id, ConstraintIDSide::Right));
    }
}

impl ExpressionBinding<Variable> {
    fn new(left: Variable, expression: ExpressionTree<Variable>) -> Self {
        Self { left, expression }
    }

    pub fn left(&self) -> Variable {
        self.left
    }

    pub fn expression(&self) -> &ExpressionTree<Variable> {
        &self.expression
    }

    pub(crate) fn validate<'cx>(&self, context: &mut BlockContext<'cx>) -> Result<(), ExpressionDefinitionError> {
        if self.expression().is_empty() {
            Err(ExpressionDefinitionError::EmptyExpressionTree {})
        } else {
            Ok(())
        }
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
    is_stream: bool,
}

impl<ID: IrID> FunctionCallBinding<ID> {
    fn new(left: Vec<ID>, function_call: FunctionCall<ID>, is_stream: bool) -> Self {
        Self { assigned: left, function_call, is_stream }
    }

    pub fn assigned(&self) -> &Vec<ID> {
        &self.assigned
    }

    pub fn function_call(&self) -> &FunctionCall<ID> {
        &self.function_call
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.ids_assigned().chain(self.function_call.argument_ids())
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
        if self.is_stream {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Owns<ID> {
    owner: ID,
    attribute: ID,
}

impl<ID: IrID> Owns<ID> {
    fn new(owner: ID, attribute: ID) -> Self {
        Self { owner, attribute }
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
}

impl<ID: IrID> From<Owns<ID>> for Constraint<ID> {
    fn from(val: Owns<ID>) -> Self {
        Constraint::Owns(val)
    }
}

impl<ID: IrID> fmt::Display for Owns<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Relates<ID> {
    relation: ID,
    role_type: ID,
}

impl<ID: IrID> Relates<ID> {
    fn new(relation: ID, role: ID) -> Self {
        Self { relation, role_type: role }
    }

    pub fn relation(&self) -> ID {
        self.relation
    }

    pub fn role_type(&self) -> ID {
        self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.relation, self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.relation, ConstraintIDSide::Left);
        function(self.role_type, ConstraintIDSide::Right);
    }
}

impl<ID: IrID> From<Relates<ID>> for Constraint<ID> {
    fn from(val: Relates<ID>) -> Self {
        Constraint::Relates(val)
    }
}

impl<ID: IrID> fmt::Display for Relates<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Plays<ID> {
    player: ID,
    role_type: ID,
}

impl<ID: IrID> Plays<ID> {
    fn new(player: ID, role: ID) -> Self {
        Self { player, role_type: role }
    }

    pub fn player(&self) -> ID {
        self.player
    }

    pub fn role_type(&self) -> ID {
        self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.player, self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        function(self.player, ConstraintIDSide::Left);
        function(self.role_type, ConstraintIDSide::Right);
    }
}

impl<ID: IrID> From<Plays<ID>> for Constraint<ID> {
    fn from(val: Plays<ID>) -> Self {
        Constraint::Plays(val)
    }
}

impl<ID: IrID> fmt::Display for Plays<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

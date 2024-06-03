/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
    iter::empty,
    sync::{Arc, Mutex},
};

use itertools::Itertools;

use crate::{
    pattern::{
        context::PatternContext,
        expression::Expression,
        function_call::FunctionCall,
        variable::{Variable, VariableCategory, VariableOptionality},
        ScopeId,
    },
    PatternDefinitionError,
};

#[derive(Debug)]
pub struct Constraints {
    scope: ScopeId,
    context: Arc<Mutex<PatternContext>>,
    constraints: Vec<Constraint>,

    // TODO: could also store indexes into the Constraints vec? Depends how expensive Constraints are and if we delete
    left_constrained_index: HashMap<Variable, Vec<Constraint>>,
    right_constrained_index: HashMap<Variable, Vec<Constraint>>,
    filter_constrained_index: HashMap<Variable, Vec<Constraint>>,
}

impl Constraints {
    pub(crate) fn new(scope: ScopeId, context: Arc<Mutex<PatternContext>>) -> Self {
        Self {
            scope,
            context,
            constraints: Vec::new(),
            left_constrained_index: HashMap::new(),
            right_constrained_index: HashMap::new(),
            filter_constrained_index: HashMap::new(),
        }
    }

    fn add_constraint(&mut self, constraint: impl Into<Constraint> + Clone) -> &Constraint {
        let constraint = constraint.into();
        self.constraints.push(constraint.clone());
        constraint.variables_foreach(|var, side| match side {
            ConstraintVariableSide::Left => {
                self.left_constrained_index.entry(var).or_insert_with(|| Vec::new()).push(constraint.clone());
            }
            ConstraintVariableSide::Right => {
                self.right_constrained_index.entry(var).or_insert_with(|| Vec::new()).push(constraint.clone());
            }
            ConstraintVariableSide::Filter => {
                self.filter_constrained_index.entry(var).or_insert_with(|| Vec::new()).push(constraint.clone());
            }
        });
        self.constraints.last().unwrap()
    }

    pub fn add_type(&mut self, variable: Variable, type_: &str) -> Result<&Type, PatternDefinitionError> {
        debug_assert!(self.context.lock().unwrap().is_variable_available(self.scope, variable));
        let type_ = Type::new(variable, type_.to_string());
        self.context.lock().unwrap().set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.add_constraint(type_);
        Ok(as_ref.as_type().unwrap())
    }

    pub fn add_isa(&mut self, thing: Variable, type_: Variable) -> Result<&Isa, PatternDefinitionError> {
        debug_assert!(
            self.context.lock().unwrap().is_variable_available(self.scope, thing)
                && self.context.lock().unwrap().is_variable_available(self.scope, type_)
        );
        let isa = Isa::new(thing, type_);
        self.context.lock().unwrap().set_variable_category(thing, VariableCategory::Thing, isa.clone().into())?;
        self.context.lock().unwrap().set_variable_category(type_, VariableCategory::Type, isa.clone().into())?;
        let as_ref = self.add_constraint(isa);
        Ok(as_ref.as_isa().unwrap())
    }

    pub fn add_has(&mut self, owner: Variable, attribute: Variable) -> Result<&Has, PatternDefinitionError> {
        debug_assert!(
            self.context.lock().unwrap().is_variable_available(self.scope, owner)
                && self.context.lock().unwrap().is_variable_available(self.scope, attribute)
        );
        let has = Has::new(owner, attribute);
        self.context.lock().unwrap().set_variable_category(owner, VariableCategory::Object, has.clone().into())?;
        self.context.lock().unwrap().set_variable_category(
            attribute,
            VariableCategory::Attribute,
            has.clone().into(),
        )?;
        let as_ref = self.add_constraint(has);
        Ok(as_ref.as_has().unwrap())
    }

    pub fn add_function_call(
        &mut self,
        assigned: Vec<Variable>,
        function_call: FunctionCall,
    ) -> Result<&FunctionCallBinding, PatternDefinitionError> {
        use PatternDefinitionError::FunctionCallReturnArgCountMismatch;
        debug_assert!(assigned.iter().all(|var| self.context.lock().unwrap().is_variable_available(self.scope, *var)));

        if assigned.len() != function_call.returns().len() {
            Err(FunctionCallReturnArgCountMismatch {
                assigned_var_count: assigned.len(),
                function_return_count: function_call.returns().len(),
            })?
        }

        let binding = FunctionCallBinding::new(assigned, function_call);

        for (index, var) in binding.variables().enumerate() {
            self.context.lock().unwrap().set_variable_category(
                var,
                binding.function_call().returns()[index].0,
                binding.clone().into(),
            )?;
            match binding.function_call.returns()[index].1 {
                VariableOptionality::Required => {}
                VariableOptionality::Optional => self.context.lock().unwrap().set_variable_is_optional(var),
            }
        }

        let as_ref = self.add_constraint(binding);
        Ok(as_ref.as_function_call_binding().unwrap())
    }

    pub fn add_expression(
        &mut self,
        variable: Variable,
        expression: Expression,
    ) -> Result<&ExpressionBinding, PatternDefinitionError> {
        debug_assert!(self.context.lock().unwrap().is_variable_available(self.scope, variable));
        let binding = ExpressionBinding::new(variable, expression);
        self.context.lock().unwrap().set_variable_category(
            variable,
            VariableCategory::Value,
            binding.clone().into(),
        )?;
        let as_ref = self.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }
}

impl Display for Constraints {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for constraint in &self.constraints {
            let indent = (0..f.width().unwrap_or(0)).map(|_| " ").join("");
            writeln!(f, "{}{}", indent, constraint)?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    Type(Type),
    Isa(Isa),
    RolePlayer(RolePlayer),
    Has(Has),
    ExpressionBinding(ExpressionBinding),
    FunctionCallBinding(FunctionCallBinding),
    Comparison(Comparison),
}

impl Constraint {
    pub fn variables(&self) -> Box<dyn Iterator<Item = Variable> + '_> {
        match self {
            Constraint::Type(type_) => Box::new(type_.variables()),
            Constraint::Isa(isa) => Box::new(isa.variables()),
            Constraint::RolePlayer(rp) => rp.variables(),
            Constraint::Has(has) => Box::new(has.variables()),
            Constraint::ExpressionBinding(binding) => todo!(),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.variables()),
            Constraint::Comparison(comparison) => todo!(),
        }
    }

    pub fn variables_foreach<F>(&self, function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        match self {
            Constraint::Type(type_) => type_.variables_foreach(function),
            Constraint::Isa(isa) => isa.variables_foreach(function),
            Constraint::RolePlayer(rp) => rp.variables_foreach(function),
            Constraint::Has(has) => has.variables_foreach(function),
            Constraint::ExpressionBinding(binding) => todo!(),
            Constraint::FunctionCallBinding(binding) => binding.variables_foreach(function),
            Constraint::Comparison(comparison) => todo!(),
        }
    }

    fn as_type(&self) -> Option<&Type> {
        match self {
            Constraint::Type(type_) => Some(type_),
            _ => None,
        }
    }

    fn as_isa(&self) -> Option<&Isa> {
        match self {
            Constraint::Isa(isa) => Some(isa),
            _ => None,
        }
    }

    fn as_role_player(&self) -> Option<&RolePlayer> {
        match self {
            Constraint::RolePlayer(rp) => Some(rp),
            _ => None,
        }
    }

    fn as_has(&self) -> Option<&Has> {
        match self {
            Constraint::Has(has) => Some(has),
            _ => None,
        }
    }

    fn as_function_call_binding(&self) -> Option<&FunctionCallBinding> {
        match self {
            Constraint::FunctionCallBinding(binding) => Some(binding),
            _ => None,
        }
    }

    fn as_expression_binding(&self) -> Option<&ExpressionBinding> {
        match self {
            Constraint::ExpressionBinding(binding) => Some(binding),
            _ => None,
        }
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Constraint::Type(constraint) => Display::fmt(constraint, f),
            Constraint::Isa(constraint) => Display::fmt(constraint, f),
            Constraint::RolePlayer(constraint) => Display::fmt(constraint, f),
            Constraint::Has(constraint) => Display::fmt(constraint, f),
            Constraint::ExpressionBinding(constraint) => Display::fmt(constraint, f),
            Constraint::FunctionCallBinding(constraint) => Display::fmt(constraint, f),
            Constraint::Comparison(constraint) => Display::fmt(constraint, f),
        }
    }
}

enum ConstraintVariableSide {
    Left,
    Right,
    Filter,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Type {
    var: Variable,
    type_: String,
}

impl Type {
    fn new(var: Variable, type_: String) -> Self {
        Self { var, type_ }
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + Sized {
        [self.var].into_iter()
    }

    pub fn variables_foreach<F>(&self, mut function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        function(self.var, ConstraintVariableSide::Left)
    }
}

impl Into<Constraint> for Type {
    fn into(self) -> Constraint {
        Constraint::Type(self)
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: implement indentation without rewriting it everywhere
        // write!(f, "{: >width$} {} type {}", "", self.var, self.type_, width=f.width().unwrap_or(0))
        write!(f, "{} type {}", self.var, self.type_)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Isa {
    thing: Variable,
    type_: Variable,
}

impl Isa {
    fn new(thing: Variable, type_: Variable) -> Self {
        Isa { thing, type_ }
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + Sized {
        [self.thing, self.type_].into_iter()
    }

    pub fn variables_foreach<F>(&self, mut function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        function(self.thing, ConstraintVariableSide::Left);
        function(self.type_, ConstraintVariableSide::Right)
    }
}

impl Into<Constraint> for Isa {
    fn into(self) -> Constraint {
        Constraint::Isa(self)
    }
}

impl Display for Isa {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} isa {}", self.thing, self.type_)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RolePlayer {
    relation: Variable,
    player: Variable,
    role_type: Option<Variable>,
}

impl RolePlayer {
    pub fn variables(&self) -> Box<dyn Iterator<Item = Variable>> {
        match self.role_type {
            None => Box::new([self.relation, self.player].into_iter()),
            Some(role_type) => Box::new([self.relation, self.player, role_type].into_iter()),
        }
    }

    pub fn variables_foreach<F>(&self, mut function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        function(self.relation, ConstraintVariableSide::Left);
        function(self.player, ConstraintVariableSide::Right);
        match self.role_type.clone() {
            None => {}
            Some(role) => function(role, ConstraintVariableSide::Filter),
        };
    }
}

impl Into<Constraint> for RolePlayer {
    fn into(self) -> Constraint {
        Constraint::RolePlayer(self)
    }
}

impl Display for RolePlayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Has {
    owner: Variable,
    attribute: Variable,
}

impl Has {
    fn new(owner: Variable, attribute: Variable) -> Self {
        Has { owner, attribute }
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> {
        [self.owner, self.attribute].into_iter()
    }

    pub fn variables_foreach<F>(&self, mut function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        function(self.owner, ConstraintVariableSide::Left);
        function(self.attribute, ConstraintVariableSide::Right);
    }
}

impl Into<Constraint> for Has {
    fn into(self) -> Constraint {
        Constraint::Has(self)
    }
}

impl Display for Has {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} has {}", self.owner, self.attribute)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionBinding {
    variable: Variable,
    expression: Expression,
}

impl ExpressionBinding {
    fn new(variable: Variable, expression: Expression) -> Self {
        Self { variable, expression }
    }

    fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        panic!("Unimplemented");
        empty()
    }

    pub fn variables_foreach<F>(&self, function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        todo!()
    }
}

impl Into<Constraint> for ExpressionBinding {
    fn into(self) -> Constraint {
        Constraint::ExpressionBinding(self)
    }
}

impl Display for ExpressionBinding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.variable, self.expression)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunctionCallBinding {
    variables: Vec<Variable>,
    function_call: FunctionCall,
}

impl FunctionCallBinding {
    fn new(variables: Vec<Variable>, function_call: FunctionCall) -> Self {
        Self { variables, function_call }
    }

    pub fn variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.variables.iter().cloned()
    }

    pub fn variables_foreach<F>(&self, mut function: F)
    where
        F: FnMut(Variable, ConstraintVariableSide) -> (),
    {
        for var in &self.variables {
            function(*var, ConstraintVariableSide::Left)
        }

        for var in self.function_call.call_variable_mapping().keys() {
            function(*var, ConstraintVariableSide::Right)
        }
    }

    pub(crate) fn function_call(&self) -> &FunctionCall {
        &self.function_call
    }
}

impl Into<Constraint> for FunctionCallBinding {
    fn into(self) -> Constraint {
        Constraint::FunctionCallBinding(self)
    }
}

impl Display for FunctionCallBinding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.function_call.return_is_stream() {
            write!(f, "{} in {}", self.variables().map(|i| i.to_string()).join(", "), self.function_call())
        } else {
            write!(f, "{} = {}", self.variables().map(|i| i.to_string()).join(", "), self.function_call())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Comparison {
    lhs: Variable,
    rhs: Variable,
    // comparator: Comparator,
}

impl Display for Comparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

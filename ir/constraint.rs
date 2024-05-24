/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex};
use crate::context::PatternContext;
use crate::expression::Expression;
use crate::function::FunctionCall;
use crate::ScopeId;
use crate::variable::Variable;

#[derive(Debug)]
pub(crate) struct Constraints {
    scope: ScopeId,
    context: Arc<Mutex<PatternContext>>,
    constraints: Vec<Constraint>,
}

impl Constraints {
    pub(crate) fn new(scope: ScopeId, context: Arc<Mutex<PatternContext>>) -> Self {
        Self {
            scope,
            context,
            constraints: Vec::new(),
        }
    }

    pub fn add_type(&mut self, variable: Variable, type_: &str) -> &Type {
        debug_assert!(self.context.lock().unwrap().is_variable_available(self.scope, variable));
        let type_ = Type::new(variable, type_.to_string());
        self.constraints.push(type_.into());
        self.constraints.last().unwrap().as_type().unwrap()
    }

    pub fn add_isa(&mut self, thing: Variable, type_: Variable) -> &Isa {
        debug_assert!(
            self.context.lock().unwrap().is_variable_available(self.scope, thing) &&
            self.context.lock().unwrap().is_variable_available(self.scope, type_)
        );
        let isa = Isa::new(thing, type_);
        self.constraints.push(isa.into());
        self.constraints.last().unwrap().as_isa().unwrap()
    }

    pub fn add_has(&mut self, owner: Variable, attribute: Variable) -> &Has {
        debug_assert!(
            self.context.lock().unwrap().is_variable_available(self.scope, owner) &&
                self.context.lock().unwrap().is_variable_available(self.scope, attribute)
        );
        let has = Has::new(owner, attribute);
        self.constraints.push(has.into());
        self.constraints.last().unwrap().as_has().unwrap()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Constraint {
    Type(Type),
    Isa(Isa),
    RolePlayer(RolePlayer),
    Has(Has),
    ExpressionAssignment(ExpressionAssignment),
    InAssignment(InAssignment),
    Comparison(Comparison),
}

impl Constraint {
    pub fn variables(&self) -> Box<dyn Iterator<Item=Variable>> {
        match self {
            Constraint::Type(type_) => {
                Box::new(type_.variables())
            }
            Constraint::Isa(isa) => {
                Box::new(isa.variables())
            }
            Constraint::RolePlayer(rp) => {
                rp.variables()
            }
            Constraint::Has(has) => {
                Box::new(has.variables())
            }
            Constraint::ExpressionAssignment(assign) => todo!(),
            Constraint::InAssignment(assign) => todo!(),
            Constraint::Comparison(comparison) => todo!(),
        }
    }

    fn as_type(&self) -> Option<&Type> {
        match self {
            Constraint::Type(type_) => Some(type_),
            _ => None
        }
    }

    fn as_isa(&self) -> Option<&Isa> {
        match self {
            Constraint::Isa(isa) => Some(isa),
            _ => None
        }
    }

    fn as_role_player(&self) -> Option<&RolePlayer> {
        match self {
            Constraint::RolePlayer(rp) => Some(rp),
            _ => None
        }
    }

    fn as_has(&self) -> Option<&Has> {
        match self {
            Constraint::Has(has) => Some(has),
            _ => None
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Type {
    var: Variable,
    type_: String
}

impl Type {
    fn new(var: Variable, type_: String) -> Self {
        Self { var, type_ }
    }

    pub fn variables(&self) -> impl Iterator<Item=Variable> + Sized {
        [self.var].into_iter()
    }
}

impl Into<Constraint> for Type {
    fn into(self) -> Constraint {
        Constraint::Type(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Isa {
    thing: Variable,
    type_: Variable,
}

impl Isa {
    fn new(thing: Variable, type_: Variable) -> Self {
        Isa { thing, type_ }
    }

    pub fn variables(&self) -> impl Iterator<Item=Variable> + Sized {
        [self.thing, self.type_].into_iter()
    }
}

impl Into<Constraint> for Isa {
    fn into(self) -> Constraint {
        Constraint::Isa(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RolePlayer {
    relation: Variable,
    player: Variable,
    role_type: Option<Variable>
}

impl RolePlayer {
    pub fn variables(&self) -> Box<dyn Iterator<Item=Variable>> {
        match self.role_type {
            None => Box::new([self.relation, self.player].into_iter()),
            Some(role_type) => Box::new([self.relation, self.player, role_type].into_iter())
        }
    }
}

impl Into<Constraint> for RolePlayer {
    fn into(self) -> Constraint {
        Constraint::RolePlayer(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Has {
    owner: Variable,
    attribute: Variable
}

impl Has {
    fn new(owner: Variable, attribute: Variable) -> Self {
        Has { owner , attribute }
    }

    pub fn variables(&self) -> impl Iterator<Item=Variable> {
        [self.owner, self.attribute].into_iter()
    }
}

impl Into<Constraint> for Has {
    fn into(self) -> Constraint {
        Constraint::Has(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExpressionAssignment {
    variables: Vec<Variable>,
    expression: Expression,
}

#[derive(Debug, Eq, PartialEq)]
pub struct InAssignment {
    variables: Vec<Variable>,
    function: Arc<FunctionCall>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Comparison {
    lhs: Variable,
    rhs: Variable,
    // comparator: Comparator,
}

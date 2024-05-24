/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use crate::{PatternDefinitionError, Scope, ScopeId};
use crate::constraint::Constraints;
use crate::context::PatternContext;
use crate::pattern::Patterns;
use crate::variable::Variable;

#[derive(Debug)]
pub struct Conjunction {
    scope_id: ScopeId,
    context: Arc<Mutex<PatternContext>>,

    constraints: Constraints,
    patterns: Patterns,
}

impl Conjunction {
    pub fn new_root() -> Self {
        let context = Arc::new(Mutex::from(PatternContext::new()));
        let scope_id = context.lock().unwrap().create_root_scope();

        Conjunction {
            scope_id,
            context: context.clone(),
            constraints: Constraints::new(scope_id, context.clone()),
            patterns: Patterns::new(scope_id, context),
        }
    }

    pub(crate) fn new_child(parent_scope_id: ScopeId, context: Arc<Mutex<PatternContext>>) -> Self {
        let scope_id = context.lock().unwrap().create_child_scope(parent_scope_id);
        Conjunction {
            scope_id,
            context: context.clone(),
            constraints: Constraints::new(scope_id, context.clone()),
            patterns: Patterns::new(scope_id, context),
        }
    }

    pub fn constraints(&mut self) -> &mut Constraints {
        &mut self.constraints
    }

    pub fn patterns(&mut self) -> &mut Patterns {
        &mut self.patterns
    }

    pub fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, PatternDefinitionError> {
        self.context.lock().unwrap()
            .get_or_declare_variable_named(name, self)
    }

    // pub fn add_negation(&mut self, mut negation: Negation) {
    //     self.set_parent_variables(&mut negation);
    //     self.negations.push(negation)
    // }
    //
    // pub fn add_disjunction(&mut self, mut disjunction: Disjunction) {
    //     for var in disjunction.variables() {
    //         self.add_optional_var(var);
    //     }
    //     self.set_parent_variables(&mut disjunction);
    //     self.disjunctions.push(disjunction)
    // }
    //
    // fn set_parent_variables(&self, pattern: &mut impl Scope) {
    //     for var in &self.declared_variables {
    //         pattern.add_parent_variable(*var);
    //     }
    //     for var in &self.parent_variable {
    //         pattern.add_parent_variable(*var);
    //     }
    // }
    //
    // fn set_child_parent_variable(&mut self, variable: Variable) {
    //     for optional in &mut self.optionals {
    //         optional.add_parent_variable(variable);
    //     }
    //     for disjunction in &mut self.disjunctions {
    //         disjunction.add_parent_variable(variable);
    //     }
    //     for negation in &mut self.negations {
    //         negation.add_parent_variable(variable);
    //     }
    // }
}

impl Scope for Conjunction {
    fn scope_id(&self) -> ScopeId {
        self.scope_id
    }

    // fn add_parent_variable(&mut self, variable: Variable) {
    //     self.parent_variable.insert(variable);
    //     self.optional_variables.remove(&variable);
    //     self.set_child_parent_variable(variable);
    // }
}

impl Display for Conjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let current_width = f.width().unwrap_or(0);
        write!(f, "{{")?;

        write!(f, "{}{{", self.scope_id)?;

        // for optional in &self.optionals {
        //     write!(f, "{{")?;
        //     write!(f, "{:width$}", optional, width = current_width + 2)?;
        //     write!(f, "}}")?
        // }

        write!(f, "}}")?;

        /*
        Constraint
        Constarint
        Constraint
        {
          optional.
        }
         */
        Ok(())
    }
}
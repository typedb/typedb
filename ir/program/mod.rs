/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::{Arc, Mutex, MutexGuard};

use crate::pattern::conjunction::Conjunction;
use crate::pattern::context::PatternContext;
use crate::program::modifier::{Filter, Limit, Modifier, Offset, Sort};
use crate::program::modifier::ModifierDefinitionError;

pub mod function;
pub mod modifier;
pub mod program;

// A functional block is exactly 1 Pattern + any number of modifiers
pub struct FunctionalBlock {
    conjunction: Conjunction,
    modifiers: Vec<Modifier>,
    context: Arc<Mutex<PatternContext>>,
}

impl FunctionalBlock {
    pub fn new() -> Self {
        let context = Arc::new(Mutex::new(PatternContext::new()));
        Self {
            conjunction: Conjunction::new(context.clone()),
            modifiers: Vec::new(),
            context: context,
        }
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub(crate) fn context(&self) -> MutexGuard<PatternContext> {
        self.context.lock().unwrap()
    }

    pub fn add_limit(&mut self, limit: u64) {
        self.modifiers.push(Modifier::Limit(Limit::new(limit)));
    }

    pub fn add_offset(&mut self, offset: u64) {
        self.modifiers.push(Modifier::Offset(Offset::new(offset)))
    }

    pub fn add_sort(&mut self, sort_variables: Vec<(&str, bool)>) -> Result<(), ModifierDefinitionError> {
        let sort = Sort::new(sort_variables, &self.context())?;
        self.modifiers.push(Modifier::Sort(sort));
        Ok(())
    }

    pub fn add_filter(&mut self, variables: Vec<&str>) -> Result<(), ModifierDefinitionError> {
        let filter = Filter::new(variables, &self.context())?;
        self.modifiers.push(Modifier::Filter(filter));
        Ok(())
    }
}

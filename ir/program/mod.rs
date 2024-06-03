/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{pattern::pattern::Pattern, program::modifier::ModifierDefinitionError};

pub mod function;
pub mod modifier;
pub mod program;

// A functional block is exactly 1 Pattern + any number of modifiers
pub trait FunctionalBlock {
    fn pattern(&self) -> &Pattern;

    fn add_limit(&mut self, limit: u64);

    fn add_offset(&mut self, offset: u64);

    fn add_sort(&mut self, sort_variables: Vec<(&str, bool)>) -> Result<(), ModifierDefinitionError>;

    fn add_filter(&mut self, variables: Vec<&str>) -> Result<(), ModifierDefinitionError>;
}

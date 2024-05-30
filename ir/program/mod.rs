/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::program::modifier::ModifierDefinitionError;

pub mod program;
pub mod function;
pub mod modifier;

pub trait FunctionalBlock {

    fn add_limit(&mut self, limit: u64);

    fn add_offset(&mut self, offset: u64);

    fn add_sort(&mut self, sort_variables: Vec<(&str, bool)>) -> Result<(), ModifierDefinitionError>;

    fn add_filter(&mut self, variables: Vec<&str>) -> Result<(), ModifierDefinitionError>;
}

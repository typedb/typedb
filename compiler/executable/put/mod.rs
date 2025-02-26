/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::sync::Arc;
use answer::variable::Variable;
use crate::executable::insert::executable::InsertExecutable;
use crate::executable::match_::planner::match_executable::MatchExecutable;
use crate::executable::next_executable_id;
use crate::VariablePosition;

#[derive(Debug)]
pub struct PutExecutable {
    pub executable_id: u64,
    pub match_: MatchExecutable,
    pub insert: InsertExecutable,
}

impl PutExecutable {
    pub(crate) fn new(match_: MatchExecutable, insert: InsertExecutable) -> PutExecutable {
        debug_assert!(match_.variable_positions() == &insert
                .output_row_schema
                .iter()
                .enumerate()
                .filter_map(|(i, opt)| opt.map(|(v, _)| (i, v)))
                .map(|(i, v)| (v, VariablePosition::new(i as u32)))
                .collect::<HashMap<_,_>>());
        Self { executable_id: next_executable_id(), match_, insert }
    }

    pub(crate) fn output_row_mapping(&self) -> &HashMap<Variable, VariablePosition> {
        self.match_.variable_positions()
    }
}

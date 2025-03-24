/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;

use crate::{
    executable::{
        insert::executable::InsertExecutable, match_::planner::match_executable::MatchExecutable, next_executable_id,
    },
    VariablePosition,
};

#[derive(Debug)]
pub struct PutExecutable {
    pub executable_id: u64,
    pub match_: MatchExecutable,
    pub insert: InsertExecutable,
}

impl PutExecutable {
    pub(crate) fn new(match_: MatchExecutable, insert: InsertExecutable) -> PutExecutable {
        debug_assert!({
            let match_positions = match_
                .variable_positions()
                .clone()
                .into_iter()
                .filter(|(_, pos)| match_.selected_variables().contains(pos))
                .collect::<HashMap<_, _>>();
            match_positions.iter().all(|(v, pos)| insert.output_row_schema[pos.as_usize()].unwrap().0 == *v)
        });
        Self { executable_id: next_executable_id(), match_, insert }
    }

    pub(crate) fn output_row_mapping(&self) -> &HashMap<Variable, VariablePosition> {
        self.match_.variable_positions()
    }

    pub fn output_width(&self) -> usize {
        self.insert.output_width()
    }
}

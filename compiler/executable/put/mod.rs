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
use crate::VariablePosition;

#[derive(Debug)]
pub struct PutExecutable {
    pub executable_id: u64,
    pub match_: MatchExecutable,
    pub insert: InsertExecutable,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

impl PutExecutable {
    pub(crate) fn new(
        match_executable: MatchExecutable,
        insert_executable: InsertExecutable,
        input_variables: &HashMap<Variable, VariablePosition>)
    -> PutExecutable {
        todo!()
    }
}

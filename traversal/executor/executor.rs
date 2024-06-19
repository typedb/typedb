/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::program::program::Program;
use crate::planner::plan::Plan;

struct Executor {
    program: Program,
    plan: Plan,
}

impl Executor {

    fn execute(&self) {

        //

    }
}
/*

Idea:
Sometimes we will iterate over constraints in a Canonical or Reverse direction
All Constraints in the IR are basically Edges.

We also can iterate canonical_all() or we can iterate canonical_from(concept), compare to reverse_all() or reverse_from()





*/
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
The planner should take a Program, and produce a Plan.

A Plan should have an order over the Variables in a Pattern's constraint, for each Functional Block.

We may need to be able to indicate which constraints are 'Seekable (+ ordered)' and therefore can be utilised in an intersection.
For example, function stream outputs are probably not seekable since we won't have traversals be seekable (at least to start!).
 */

pub mod function_plan;
pub mod pattern_plan;
pub mod program_plan;
mod vertex;

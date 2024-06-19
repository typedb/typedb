/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::pattern::variable::Variable;

pub(crate) struct Plan {
    ordering: Vec<Variable>
}

/*
Planning should order Plannables, which are either Variables or ScopeIDs

A plan should then be computed from the ordered plannables,
indicating which constraints should be used with a iterator/single, direction, and sortedness... eg
1. From $y, Intersect(C1, C2, C3).player -> $x
2. From $x, $y, Check(C4)


Plan should indicate direction and operation to use.
Plan should indicate whether returns iterator or single
Plan should indicate pre-sortedness of iterator (allows intersecting automatically)
 */

enum PlanElement {
    Iterator,
    Expression,
    Filter
}

enum

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::variable::Variable;
use crate::pattern::expression::Expression;
use crate::pattern::ParameterID;

enum FetchSome {}

struct FetchSingleVar {
    var: Variable,
}

struct FetchSingleExpression {
    expression: Expression<Variable>,
}

struct FetchSingleReturn {

}

struct FetchObjectPredefined {
    object: HashMap<ParameterID, FetchSome>,
}

struct FetchObjectAttributes {
    variable: Variable,
}




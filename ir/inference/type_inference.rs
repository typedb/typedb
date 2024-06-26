/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    pattern::{
        constraint::{Constraint, Type},
        variable::Variable,
    },
    program::program::Program,
};

/*
Design:
1. Assign a static, deterministic ordering over the functions in the Program.
2. Assign a static, deterministic ordering over the connected variables in each functional block's Pattern.
3. Set the possible types for each variable to all types of its category initially (note: function input and output variables can be restricted to subtypes of labelled types initially!)
4. For each variable in the ordering, go over each constraint and intersect types

Output data structure:
TypeAnnotations per FunctionalBlock

Note: On function call boundaries, can assume the current set of schema types per input and output variable.
      However, we should then recurse into the sub-function IRs and tighten their input/output types based on their type inference.

 */

pub fn infer_types(program: &Program) {
    let mut entry_type_annotations = TypeAnnotations::new();
    let mut function_type_annotations: HashMap<DefinitionKey<'static>, TypeAnnotations> = HashMap::new();
}

struct TypeAnnotations {
    variables: HashMap<Variable, HashSet<Type<Variable>>>,
    constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
}

impl TypeAnnotations {
    fn new() -> Self {
        TypeAnnotations { variables: HashMap::new(), constraints: HashMap::new() }
    }
}

enum ConstraintTypeAnnotations {
    LeftRight(LeftRightAnnotations),
    LeftRightFiltered(LeftRightFilteredAnnotations), // note: function calls, comparators, and value assignments are not stored here, since they do not actually co-constrain Schema types possible.
                                                     //       in other words, they are always right to left or deal only in value types.
}

struct LeftRightAnnotations {
    left_to_right: BTreeMap<Type<Variable>, BTreeSet<Type<Variable>>>,
    right_to_left: BTreeMap<Type<Variable>, BTreeSet<Type<Variable>>>,
}

struct LeftRightFilteredAnnotations {
    left_to_right: BTreeMap<Type<Variable>, (BTreeSet<Type<Variable>>, HashSet<Type<Variable>>)>,
    right_to_left: BTreeMap<Type<Variable>, (BTreeSet<Type<Variable>>, HashSet<Type<Variable>>)>,
    // TODO: I think we'll need to be able to traverse from the Filter variable to the left and right. example: `match $role sub friendship:friend; $r ($role: $x);`
    // filter_to_left
    // filter_to_right
}

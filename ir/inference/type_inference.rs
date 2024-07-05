/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::type_manager::TypeManager;
use storage::snapshot::ReadableSnapshot;

use crate::{
    inference::pattern_type_inference::{infer_types_for_conjunction, TypeInferenceGraph},
    pattern::{conjunction::Conjunction, constraint::Constraint, ScopeId},
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

pub(crate) type VertexAnnotations = BTreeMap<Variable, BTreeSet<Type>>;

pub fn infer_types(
    program: &Program,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> ProgramAnnotations {
    // let mut entry_type_annotations = TypeAnnotations::new(HashMap::new(), HashMap::new());
    // let mut function_type_annotations: HashMap<DefinitionKey<'static>, TypeAnnotations> = HashMap::new();
    // todo!()
    // TODO: Extend to functions when we implement them
    let root_tig = infer_types_for_conjunction(snapshot, type_manager, program.entry().conjunction()).unwrap();

    ProgramAnnotations::build(root_tig)
}

pub struct ProgramAnnotations {
    pub(crate) scoped_annotations: HashMap<ScopeId, TypeAnnotations>,
}

impl ProgramAnnotations {
    fn build(root_type_inference_graph: TypeInferenceGraph<'_>) -> Self {
        let mut scoped_annotations = HashMap::new();
        root_type_inference_graph.populate_scoped_annotations(&mut scoped_annotations);
        ProgramAnnotations { scoped_annotations }
    }

    fn get_annotations_for<'conj, 'this>(
        &'this self,
        conjunction: &'conj Conjunction,
    ) -> Option<&'this TypeAnnotations> {
        self.scoped_annotations.get(&conjunction.scope_id())
    }
}

pub struct TypeAnnotations {
    variables: HashMap<Variable, Arc<HashSet<Type>>>,
    constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
}

impl TypeAnnotations {
    pub fn new(
        variables: HashMap<Variable, Arc<HashSet<Type>>>,
        constraints: HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
    ) -> Self {
        TypeAnnotations { variables: variables, constraints: constraints }
    }

    pub fn variable_annotations(&self, variable: Variable) -> Option<Arc<HashSet<Type>>> {
        self.variables.get(&variable).map(|annotations| annotations.clone())
    }

    pub fn constraint_annotations(&self, constraint: Constraint<Variable>) -> Option<&ConstraintTypeAnnotations> {
        self.constraints.get(&constraint)
    }
}

pub enum ConstraintTypeAnnotations {
    LeftRight(LeftRightAnnotations),
    LeftRightFiltered(LeftRightFilteredAnnotations), // note: function calls, comparators, and value assignments are not stored here, since they do not actually co-constrain Schema types possible.
                                                     //       in other words, they are always right to left or deal only in value types.
}

impl ConstraintTypeAnnotations {
    pub fn get_left_right(&self) -> &LeftRightAnnotations {
        match self {
            ConstraintTypeAnnotations::LeftRight(annotations) => annotations,
            ConstraintTypeAnnotations::LeftRightFiltered(_) => panic!("Unexpected type."),
        }
    }
}

pub struct LeftRightAnnotations {
    left_to_right: Arc<BTreeMap<Type, Vec<Type>>>,
    right_to_left: Arc<BTreeMap<Type, Vec<Type>>>,
}

impl LeftRightAnnotations {
    pub fn new(left_to_right: BTreeMap<Type, Vec<Type>>, right_to_left: BTreeMap<Type, Vec<Type>>) -> Self {
        Self { left_to_right: Arc::new(left_to_right), right_to_left: Arc::new(right_to_left) }
    }

    pub(crate) fn build(
        left_to_right_set: BTreeMap<Type, BTreeSet<Type>>,
        right_to_left_set: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let mut left_to_right = BTreeMap::new();
        for (left, right_set) in left_to_right_set {
            left_to_right.insert(left, right_set.into_iter().collect());
        }
        let mut right_to_left = BTreeMap::new();
        for (right, left_set) in right_to_left_set {
            right_to_left.insert(right, left_set.into_iter().collect());
        }
        Self::new(left_to_right, right_to_left)
    }

    pub fn left_to_right(&self) -> Arc<BTreeMap<Type, Vec<Type>>> {
        self.left_to_right.clone()
    }
}

pub struct LeftRightFilteredAnnotations {
    // pub(crate) left_to_right: BTreeMap<Type, (BTreeSet<Type>, HashSet<Type>)>,
    // pub(crate) right_to_left: BTreeMap<Type, (BTreeSet<Type>, HashSet<Type>)>,
    // TODO: I think we'll need to be able to traverse from the Filter variable to the left and right. example: `match $role sub friendship:friend; $r ($role: $x);`
    // filter_to_left
    // filter_to_right

    // TODO: krishnan: ^ I don't know if I miss something, but this makes more sense to me:
    // Filtered edges are encoded as  (left,right,filter) and (right,left,filter).
    pub(crate) left_to_right: BTreeMap<Type, Vec<Type>>,
    pub(crate) filters_on_right: BTreeMap<Type, HashSet<Type>>, // The key is the type of the right variable

    pub(crate) right_to_left: BTreeMap<Type, Vec<Type>>,
    pub(crate) filters_on_left: BTreeMap<Type, HashSet<Type>>, // The key is the type of the left variable
}

impl LeftRightFilteredAnnotations {
    pub(crate) fn build(
        relation_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_relation: BTreeMap<Type, BTreeSet<Type>>,
        player_to_role: BTreeMap<Type, BTreeSet<Type>>,
        role_to_player: BTreeMap<Type, BTreeSet<Type>>,
    ) -> Self {
        let mut role_to_player = role_to_player;
        let mut role_to_relation = role_to_relation;
        let mut left_to_right = BTreeMap::new();
        let mut right_to_left = BTreeMap::new();
        let mut filters_on_right = BTreeMap::new();
        let mut filters_on_left = BTreeMap::new();
        for (relation, role_set) in relation_to_role {
            for role in &role_set {
                left_to_right.insert(relation.clone(), role_to_player.remove(role).unwrap().into_iter().collect());
            }
            filters_on_left.insert(relation, role_set.into_iter().collect());
        }

        for (player, role_set) in player_to_role {
            for role in &role_set {
                right_to_left.insert(player.clone(), role_to_relation.remove(&role).unwrap().into_iter().collect());
            }
            filters_on_right.insert(player, role_set.into_iter().collect());
        }
        Self { left_to_right, filters_on_right, right_to_left, filters_on_left }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt, iter};

use answer::{variable::Variable, Type};
use concept::thing::statistics::Statistics;
use ir::pattern::constraint::{Has, Isa, Kind, Label, Links, Owns, Plays, Relates, RoleName, Sub, SubKind};
use itertools::Itertools;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::{
        instructions::{type_::TypeListInstruction, CheckInstruction, ConstraintInstruction},
        planner::{
            plan::{Graph, VariableVertexId, VertexId},
            vertex::{
                type_count, Costed, Direction, ElementCost, Input, ADVANCE_ITERATOR_RELATIVE_COST,
                OPEN_ITERATOR_RELATIVE_COST,
            },
        },
    },
};

#[derive(Clone, Debug)]
pub(crate) enum ConstraintVertex<'a> {
    TypeList(TypeListPlanner<'a>),

    Isa(IsaPlanner<'a>),
    Has(HasPlanner<'a>),
    Links(LinksPlanner<'a>),

    Sub(SubPlanner<'a>),
    Owns(OwnsPlanner<'a>),
    Relates(RelatesPlanner<'a>),
    Plays(PlaysPlanner<'a>),
}

impl ConstraintVertex<'_> {
    pub(super) fn is_valid(&self, _: VertexId, _: &[VertexId], _: &Graph<'_>) -> bool {
        true // always valid
    }

    pub(crate) fn unbound_direction(&self, graph: &Graph<'_>) -> Direction {
        match self {
            Self::TypeList(_) => Direction::Canonical,
            Self::Isa(_) => Direction::Canonical,
            Self::Has(inner) => inner.unbound_direction(graph),
            Self::Links(inner) => inner.unbound_direction(graph),
            Self::Sub(inner) => inner.unbound_direction,
            Self::Owns(inner) => inner.unbound_direction,
            Self::Relates(inner) => inner.unbound_direction,
            Self::Plays(inner) => inner.unbound_direction,
        }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = VariableVertexId> + '_> {
        match self {
            Self::TypeList(inner) => Box::new(inner.variables()),

            Self::Isa(inner) => Box::new(inner.variables()),
            Self::Has(inner) => Box::new(inner.variables()),
            Self::Links(inner) => Box::new(inner.variables()),

            Self::Sub(inner) => Box::new(inner.variables()),
            Self::Owns(inner) => Box::new(inner.variables()),
            Self::Relates(inner) => Box::new(inner.variables()),
            Self::Plays(inner) => Box::new(inner.variables()),
        }
    }

    pub(crate) fn can_sort_on(&self, var: VariableVertexId) -> bool {
        match self {
            Self::Links(inner) => inner.relation == var || inner.player == var,
            _ => self.variables().contains(&var),
        }
    }
}

impl Costed for ConstraintVertex<'_> {
    fn cost(&self, inputs: &[VertexId], intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        match self {
            Self::TypeList(inner) => inner.cost(inputs, intersection, graph),

            Self::Isa(inner) => inner.cost(inputs, intersection, graph),
            Self::Has(inner) => inner.cost(inputs, intersection, graph),
            Self::Links(inner) => inner.cost(inputs, intersection, graph),

            Self::Sub(inner) => inner.cost(inputs, intersection, graph),
            Self::Owns(inner) => inner.cost(inputs, intersection, graph),
            Self::Relates(inner) => inner.cost(inputs, intersection, graph),
            Self::Plays(inner) => inner.cost(inputs, intersection, graph),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TypeListConstraint<'a> {
    Label(&'a Label<Variable>),
    RoleName(&'a RoleName<Variable>),
    Kind(&'a Kind<Variable>),
}

impl<'a> TypeListConstraint<'a> {
    pub(crate) fn var(self) -> Variable {
        match self {
            TypeListConstraint::Label(label) => label.type_(),
            TypeListConstraint::RoleName(role_name) => role_name.type_(),
            TypeListConstraint::Kind(kind) => kind.type_(),
        }
        .as_variable()
        .unwrap()
    }
}

#[derive(Clone)]
pub(crate) struct TypeListPlanner<'a> {
    constraint: TypeListConstraint<'a>,
    var: VariableVertexId,
    types: Vec<Type>,
}

impl<'a> fmt::Debug for TypeListPlanner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypeListPlanner").field("constraint", &self.constraint).finish()
    }
}

impl<'a> TypeListPlanner<'a> {
    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        iter::once(self.var)
    }

    pub(crate) fn from_label_constraint(
        label: &'a Label<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
    ) -> Self {
        let types = type_annotations
            .vertex_annotations_of(label.type_label())
            .into_iter()
            .flat_map(|annos| annos.iter())
            .cloned()
            .collect();
        Self {
            constraint: TypeListConstraint::Label(label),
            var: variable_index[&label.type_().as_variable().unwrap()],
            types,
        }
    }

    pub(crate) fn from_role_name_constraint(
        role_name: &'a RoleName<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
    ) -> Self {
        let types = type_annotations
            .vertex_annotations_of(role_name.type_())
            .into_iter()
            .flat_map(|annos| annos.iter())
            .cloned()
            .collect();
        Self {
            constraint: TypeListConstraint::RoleName(role_name),
            var: variable_index[&role_name.type_().as_variable().unwrap()],
            types,
        }
    }

    pub(crate) fn from_kind_constraint(
        kind: &'a Kind<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
    ) -> Self {
        let types = type_annotations
            .vertex_annotations_of(kind.type_())
            .into_iter()
            .flat_map(|annos| annos.iter())
            .cloned()
            .collect();
        Self {
            constraint: TypeListConstraint::Kind(kind),
            var: variable_index[&kind.type_().as_variable().unwrap()],
            types,
        }
    }

    pub(crate) fn constraint(&self) -> &TypeListConstraint<'a> {
        &self.constraint
    }

    pub(crate) fn lower(&self, type_annotations: &TypeAnnotations) -> ConstraintInstruction<Variable> {
        let var = self.constraint.var();
        ConstraintInstruction::TypeList(TypeListInstruction::new(var, type_annotations))
    }

    pub(crate) fn lower_check(&self) -> CheckInstruction<Variable> {
        CheckInstruction::TypeList { type_var: self.constraint.var(), types: self.types.clone() }
    }
}

impl Costed for TypeListPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(self.types.len() as f64)
    }
}

#[derive(Clone)]
pub(crate) struct IsaPlanner<'a> {
    isa: &'a Isa<Variable>,
    thing: VariableVertexId,
    type_: Input,
    unrestricted_expected_size: f64,
}

impl<'a> fmt::Debug for IsaPlanner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IsaPlanner").field("isa", self.isa).finish()
    }
}

impl<'a> IsaPlanner<'a> {
    pub(crate) fn from_constraint(
        isa: &'a Isa<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let thing = variable_index[&isa.thing().as_variable().unwrap()];
        let type_ = Input::from_vertex(isa.type_(), variable_index);
        let unrestricted_expected_size = type_annotations
            .vertex_annotations_of(isa.thing())
            .map(|thing_types| {
                thing_types.iter().map(|thing_type| type_count(thing_type, statistics)).sum::<u64>() as f64
            })
            .unwrap_or(0.0);
        Self { isa, thing, type_, unrestricted_expected_size }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [Some(self.thing), self.type_.as_variable()].into_iter().flatten()
    }

    pub(crate) fn isa(&self) -> &Isa<Variable> {
        self.isa
    }

    fn expected_output_size(&self, graph: &Graph<'_>) -> f64 {
        let thing = graph.elements()[&VertexId::Variable(self.thing)].as_variable().unwrap();
        self.unrestricted_expected_size * thing.selectivity()
    }
}

impl Costed for IsaPlanner<'_> {
    fn cost(&self, inputs: &[VertexId], _intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        let thing_id = VertexId::Variable(self.thing);
        let is_thing_bound = inputs.contains(&thing_id);
        let thing = graph.elements()[&thing_id].as_variable().unwrap();

        let is_type_bound = match &self.type_ {
            Input::Fixed => true,
            Input::Variable(var) => inputs.contains(&VertexId::Variable(*var)),
        };

        let (per_input, per_output) = match (is_thing_bound, is_type_bound) {
            (true, true) | (true, false) => (0.0, 0.0),
            (false, true) | (false, false) => {
                let per_input = OPEN_ITERATOR_RELATIVE_COST;
                // when the type is bound or unbound, we may reject multiple things until we find a matching one, depending on the selectivity of the thing
                let per_output = ADVANCE_ITERATOR_RELATIVE_COST / thing.selectivity();
                (per_input, per_output)
            }
        };

        let thing_size = thing.expected_output_size();
        let type_size = match &self.type_ {
            Input::Fixed => 1.0,
            Input::Variable(var) => {
                let type_id = VertexId::Variable(*var);
                let type_ = graph.elements()[&type_id].as_variable().unwrap();
                type_.expected_output_size()
            }
        };

        let branching_factor = match (is_thing_bound, is_type_bound) {
            (true, true) => self.expected_output_size(graph) / thing_size / type_size,
            (true, false) => self.expected_output_size(graph) / thing_size,
            (false, true) => self.expected_output_size(graph) / type_size,
            (false, false) => self.expected_output_size(graph),
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Clone)]
pub(crate) struct HasPlanner<'a> {
    has: &'a Has<Variable>,
    pub owner: VariableVertexId,
    pub attribute: VariableVertexId,
    unbound_typed_expected_size: f64,
    unbound_typed_expected_size_canonical: f64,
    unbound_typed_expected_size_reverse: f64,
}

impl<'a> fmt::Debug for HasPlanner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HasPlanner").field("has", &self.has).finish()
    }
}

impl<'a> HasPlanner<'a> {
    pub(crate) fn from_constraint(
        has: &'a Has<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let owner = has.owner();
        let attribute = has.attribute();

        let owner_types = &**type_annotations.vertex_annotations_of(owner).unwrap();
        let attribute_types = &**type_annotations.vertex_annotations_of(attribute).unwrap();

        let expected_size = itertools::iproduct!(owner_types, attribute_types)
            .filter_map(|(owner, attribute)| {
                statistics.has_attribute_counts.get(&owner.as_object_type())?.get(&attribute.as_attribute_type())
            })
            .sum::<u64>() as f64;

        // a better model, according to how we execute, is actually the sum(first type .. last types onwerships count)
        //  alternatively, we could also be doing multiple seeks() and and merge-sorting.
        //  in general, we assume the cardinality is small, so we just open 1 iterator and post-filter
        let unbound_forward_size = owner_types
            .iter()
            .filter_map(|owner| statistics.has_attribute_counts.get(&owner.as_object_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let unbound_backward_size = attribute_types
            .iter()
            .filter_map(|attribute| statistics.attribute_owner_counts.get(&attribute.as_attribute_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        Self {
            has,
            owner: variable_index[&owner.as_variable().unwrap()],
            attribute: variable_index[&attribute.as_variable().unwrap()],
            unbound_typed_expected_size: expected_size,
            unbound_typed_expected_size_canonical: unbound_forward_size,
            unbound_typed_expected_size_reverse: unbound_backward_size,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.owner, self.attribute].into_iter()
    }

    pub(crate) fn has(&self) -> &Has<Variable> {
        self.has
    }

    fn unbound_direction(&self, graph: &Graph<'_>) -> Direction {
        if self.unbound_expected_scan_size_canonical(graph) < self.unbound_expected_scan_size_reverse(graph) {
            Direction::Canonical
        } else {
            Direction::Reverse
        }
    }

    fn unbound_expected_scan_size(&self, graph: &Graph<'_>) -> f64 {
        f64::min(self.unbound_expected_scan_size_canonical(graph), self.unbound_expected_scan_size_reverse(graph))
    }

    fn unbound_expected_scan_size_canonical(&self, graph: &Graph<'_>) -> f64 {
        let owner = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        self.unbound_typed_expected_size_canonical * owner.selectivity()
    }

    fn unbound_expected_scan_size_reverse(&self, graph: &Graph<'_>) -> f64 {
        let attribute = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        self.unbound_typed_expected_size_reverse * attribute.selectivity()
    }

    fn expected_output_size(&self, graph: &Graph<'_>) -> f64 {
        // get selectivity of attribute and multiply it by the expected size based on types
        let attribute = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        self.unbound_typed_expected_size * attribute.selectivity()
    }
}

impl Costed for HasPlanner<'_> {
    fn cost(&self, inputs: &[VertexId], _intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        let owner_id = VertexId::Variable(self.owner);
        let attribute_id = VertexId::Variable(self.attribute);

        let is_owner_bound = inputs.contains(&owner_id);
        let is_attribute_bound = inputs.contains(&attribute_id);
        let owner = &graph.elements()[&owner_id].as_variable().unwrap();
        let attribute = &graph.elements()[&owner_id].as_variable().unwrap();

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_owner_bound, is_attribute_bound) {
            (true, true) => 0.0,
            (true, false) => {
                // when the owner is bound, we may reject multiple attributes until we find a matching one, depending on the selectivity of the attribute
                ADVANCE_ITERATOR_RELATIVE_COST / attribute.selectivity()
            }
            (false, true) => {
                // when the attribute is bound, we may reject multiple owners until we find a matching one, depending on the selectivity of the owner
                ADVANCE_ITERATOR_RELATIVE_COST / owner.selectivity()
            }
            (false, false) => {
                ADVANCE_ITERATOR_RELATIVE_COST * self.unbound_expected_scan_size(graph)
                    / self.expected_output_size(graph)
            }
        };

        let owner_size = owner.expected_output_size();
        let attribute_size = attribute.expected_output_size();

        let branching_factor = match (is_owner_bound, is_attribute_bound) {
            (true, true) => self.expected_output_size(graph) / owner_size / attribute_size,
            (true, false) => self.expected_output_size(graph) / owner_size,
            (false, true) => self.expected_output_size(graph) / attribute_size,
            (false, false) => self.expected_output_size(graph),
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Clone)]
pub(crate) struct LinksPlanner<'a> {
    links: &'a Links<Variable>,
    pub relation: VariableVertexId,
    pub player: VariableVertexId,
    pub role: VariableVertexId,
    unbound_typed_expected_size: f64,
    unbound_typed_expected_size_canonical: f64,
    unbound_typed_expected_size_reverse: f64,
}

impl<'a> fmt::Debug for LinksPlanner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinksPlanner").field("links", &self.links).finish()
    }
}

impl<'a> LinksPlanner<'a> {
    pub(crate) fn from_constraint(
        links: &'a Links<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let relation = links.relation();
        let player = links.player();
        let role = links.role_type();

        let relation_types = &**type_annotations.vertex_annotations_of(relation).unwrap();
        let player_types = &**type_annotations.vertex_annotations_of(player).unwrap();

        let constraint_types =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_left_right_filtered();

        let expected_size = constraint_types
            .filters_on_left()
            .iter()
            .flat_map(|(relation, roles)| {
                roles.iter().cartesian_product(player_types).flat_map(|(role, player)| {
                    statistics
                        .relation_role_player_counts
                        .get(&relation.as_relation_type())?
                        .get(&role.as_role_type())?
                        .get(&player.as_object_type())
                })
            })
            .sum::<u64>() as f64;

        let unbound_forward_size = relation_types
            .iter()
            .filter_map(|relation| {
                Some(statistics.relation_role_player_counts.get(&relation.as_relation_type())?.values().flat_map(
                    |player_to_count| {
                        player_types.iter().filter_map(|player| player_to_count.get(&player.as_object_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;

        let unbound_backward_size = player_types
            .iter()
            .filter_map(|player| {
                Some(statistics.player_role_relation_counts.get(&player.as_object_type())?.values().flat_map(
                    |relation_to_count| {
                        relation_types.iter().filter_map(|relation| relation_to_count.get(&relation.as_relation_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;
        let relation = relation.as_variable().unwrap();
        let player = player.as_variable().unwrap();
        let role = role.as_variable().unwrap();

        Self {
            links,
            relation: variable_index[&relation],
            player: variable_index[&player],
            role: variable_index[&role],
            unbound_typed_expected_size: expected_size,
            unbound_typed_expected_size_canonical: unbound_forward_size,
            unbound_typed_expected_size_reverse: unbound_backward_size,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.relation, self.player, self.role].into_iter()
    }

    pub(crate) fn links(&self) -> &Links<Variable> {
        self.links
    }

    pub(crate) fn unbound_direction(&self, graph: &Graph<'_>) -> Direction {
        if self.unbound_expected_scan_size_canonical(graph) < self.unbound_expected_scan_size_reverse(graph) {
            Direction::Canonical
        } else {
            Direction::Reverse
        }
    }

    fn unbound_expected_scan_size(&self, graph: &Graph<'_>) -> f64 {
        f64::min(self.unbound_expected_scan_size_canonical(graph), self.unbound_expected_scan_size_reverse(graph))
    }

    fn unbound_expected_scan_size_canonical(&self, graph: &Graph<'_>) -> f64 {
        let relation = &graph.elements()[&VertexId::Variable(self.relation)].as_variable().unwrap();
        self.unbound_typed_expected_size_canonical * relation.selectivity()
    }

    fn unbound_expected_scan_size_reverse(&self, graph: &Graph<'_>) -> f64 {
        let player = &graph.elements()[&VertexId::Variable(self.player)].as_variable().unwrap();
        self.unbound_typed_expected_size_reverse * player.selectivity()
    }

    fn expected_output_size(&self, graph: &Graph<'_>) -> f64 {
        // get selectivity of attribute and multiply it by the expected size based on types
        let player = &graph.elements()[&VertexId::Variable(self.player)].as_variable().unwrap();
        let relation = &graph.elements()[&VertexId::Variable(self.relation)].as_variable().unwrap();
        self.unbound_typed_expected_size * player.selectivity() * relation.selectivity()
    }
}

impl Costed for LinksPlanner<'_> {
    fn cost(&self, inputs: &[VertexId], _intersection: Option<VariableVertexId>, graph: &Graph<'_>) -> ElementCost {
        let relation_id = VertexId::Variable(self.relation);
        let player_id = VertexId::Variable(self.player);

        let is_relation_bound = inputs.contains(&relation_id);
        let is_player_bound = inputs.contains(&player_id);
        let relation = &graph.elements()[&relation_id].as_variable().unwrap();
        let player = &graph.elements()[&player_id].as_variable().unwrap();

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_relation_bound, is_player_bound) {
            (true, true) => 0.0,
            (true, false) => {
                // when the relation is bound, we may reject multiple players until we find a matching one, depending on the selectivity of the player
                ADVANCE_ITERATOR_RELATIVE_COST / player.selectivity()
            }
            (false, true) => {
                // when the player is bound, we may reject multiple relations until we find a matching one, depending on the selectivity of the relation
                ADVANCE_ITERATOR_RELATIVE_COST / relation.selectivity()
            }
            (false, false) => {
                ADVANCE_ITERATOR_RELATIVE_COST * self.unbound_expected_scan_size(graph)
                    / self.expected_output_size(graph)
            }
        };

        let relation_size = relation.expected_output_size();
        let player_size = player.expected_output_size();

        let branching_factor = match (is_relation_bound, is_player_bound) {
            (true, true) => self.expected_output_size(graph) / relation_size / player_size,
            (true, false) => self.expected_output_size(graph) / relation_size,
            (false, true) => self.expected_output_size(graph) / player_size,
            (false, false) => self.expected_output_size(graph),
        };

        ElementCost { per_input, per_output, branching_factor }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubPlanner<'a> {
    sub: &'a Sub<Variable>,
    type_: Input,
    supertype: Input,
    kind: SubKind,
    unbound_direction: Direction,
}

impl<'a> SubPlanner<'a> {
    pub(crate) fn from_constraint(
        sub: &'a Sub<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
    ) -> Self {
        Self {
            sub,
            type_: Input::from_vertex(sub.subtype(), variable_index),
            supertype: Input::from_vertex(sub.supertype(), variable_index),
            kind: sub.sub_kind(),
            unbound_direction: Direction::Reverse,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.type_.as_variable(), self.supertype.as_variable()].into_iter().flatten()
    }

    pub(crate) fn sub(&self) -> &Sub<Variable> {
        self.sub
    }
}

impl Costed for SubPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(1.0) // TODO branching
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OwnsPlanner<'a> {
    owns: &'a Owns<Variable>,
    owner: Input,
    attribute: Input,
    unbound_direction: Direction,
}

impl<'a> OwnsPlanner<'a> {
    pub(crate) fn from_constraint(
        owns: &'a Owns<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let owner = Input::from_vertex(owns.owner(), variable_index);
        let attribute = Input::from_vertex(owns.attribute(), variable_index);
        Self { owns, owner, attribute, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.owner.as_variable(), self.attribute.as_variable()].into_iter().flatten()
    }

    pub(crate) fn owns(&self) -> &Owns<Variable> {
        self.owns
    }
}

impl Costed for OwnsPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(1.0) // TODO branching
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RelatesPlanner<'a> {
    relates: &'a Relates<Variable>,
    relation: Input,
    role_type: Input,
    unbound_direction: Direction,
}

impl<'a> RelatesPlanner<'a> {
    pub(crate) fn from_constraint(
        relates: &'a Relates<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let relation = Input::from_vertex(relates.relation(), variable_index);
        let role_type = Input::from_vertex(relates.role_type(), variable_index);
        Self { relates, relation, role_type, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.relation.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }

    pub(crate) fn relates(&self) -> &Relates<Variable> {
        self.relates
    }
}

impl Costed for RelatesPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(1.0) // TODO branching
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlaysPlanner<'a> {
    plays: &'a Plays<Variable>,
    player: Input,
    role_type: Input,
    unbound_direction: Direction,
}

impl<'a> PlaysPlanner<'a> {
    pub(crate) fn from_constraint(
        plays: &'a Plays<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let player = Input::from_vertex(plays.player(), variable_index);
        let role_type = Input::from_vertex(plays.role_type(), variable_index);
        Self { plays, player, role_type, unbound_direction: Direction::Canonical }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.player.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }

    pub(crate) fn plays(&self) -> &Plays<Variable> {
        self.plays
    }
}

impl Costed for PlaysPlanner<'_> {
    fn cost(&self, _: &[VertexId], _intersection: Option<VariableVertexId>, _: &Graph<'_>) -> ElementCost {
        ElementCost::free_with_branching(1.0) // TODO branching
    }
}

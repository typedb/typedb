/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap},
    fmt, iter,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::thing::statistics::Statistics;
use ir::pattern::constraint::{
    Has, Iid, IndexedRelation, Isa, Kind, Label, Links, Owns, Plays, Relates, RoleName, Sub, Value,
};
use itertools::Itertools;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::{
        instructions::{type_::TypeListInstruction, CheckInstruction, ConstraintInstruction},
        planner::{
            plan::{Graph, VariableVertexId, VertexId},
            vertex::{
                instance_count, variable::VariableVertex, Cost, CostMetaData, Costed, Direction, Input,
                ADVANCE_ITERATOR_RELATIVE_COST, OPEN_ITERATOR_RELATIVE_COST,
            },
        },
    },
};

const MIN_SCAN_SIZE: f64 = 1.0;

#[derive(Clone, Debug)]
pub(crate) enum ConstraintVertex<'a> {
    TypeList(TypeListPlanner<'a>),
    Iid(IidPlanner<'a>),

    Isa(IsaPlanner<'a>),
    Has(HasPlanner<'a>),
    Links(LinksPlanner<'a>),
    IndexedRelation(IndexedRelationPlanner<'a>),

    Sub(SubPlanner<'a>),
    Owns(OwnsPlanner<'a>),
    Relates(RelatesPlanner<'a>),
    Plays(PlaysPlanner<'a>),
}

impl ConstraintVertex<'_> {
    pub(super) fn is_valid(&self, _: &[VertexId], _: &Graph<'_>) -> bool {
        true // always valid
    }

    pub(crate) fn unbound_direction(&self, graph: &Graph<'_>) -> Direction {
        match self {
            Self::TypeList(_) => Direction::Canonical,
            Self::Iid(_) => Direction::Canonical,
            Self::Isa(_) => Direction::Canonical,
            Self::Has(inner) => inner.unbound_direction(graph, &[]),
            Self::Links(inner) => inner.unbound_direction(graph, &[]),
            Self::IndexedRelation(inner) => inner.unbound_direction(graph, &[]),
            Self::Sub(inner) => inner.unbound_direction,
            Self::Owns(inner) => inner.unbound_direction,
            Self::Relates(inner) => inner.unbound_direction,
            Self::Plays(inner) => inner.unbound_direction,
        }
    }

    pub(crate) fn variables(&self) -> Box<dyn Iterator<Item = VariableVertexId> + '_> {
        match self {
            Self::TypeList(inner) => Box::new(inner.variables()),
            Self::Iid(inner) => Box::new(inner.variables()),

            Self::Isa(inner) => Box::new(inner.variables()),
            Self::Has(inner) => Box::new(inner.variables()),
            Self::Links(inner) => Box::new(inner.variables()),
            Self::IndexedRelation(inner) => Box::new(inner.variables()),

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
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        match self {
            Self::TypeList(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Iid(inner) => inner.cost_and_metadata(vertex_ordering, graph),

            Self::Isa(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Has(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Links(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::IndexedRelation(inner) => inner.cost_and_metadata(vertex_ordering, graph),

            Self::Sub(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Owns(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Relates(inner) => inner.cost_and_metadata(vertex_ordering, graph),
            Self::Plays(inner) => inner.cost_and_metadata(vertex_ordering, graph),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TypeListConstraint<'a> {
    Label(&'a Label<Variable>),
    RoleName(&'a RoleName<Variable>),
    Kind(&'a Kind<Variable>),
    Value(&'a Value<Variable>),
}

impl TypeListConstraint<'_> {
    pub(crate) fn var(self) -> Variable {
        match self {
            TypeListConstraint::Label(label) => label.type_(),
            TypeListConstraint::RoleName(role_name) => role_name.type_(),
            TypeListConstraint::Kind(kind) => kind.type_(),
            TypeListConstraint::Value(value) => value.attribute_type(),
        }
        .as_variable()
        .unwrap()
    }
}

#[derive(Clone)]
pub(crate) struct TypeListPlanner<'a> {
    constraint: TypeListConstraint<'a>,
    var: VariableVertexId,
    types: Arc<BTreeSet<Type>>,
}

impl fmt::Debug for TypeListPlanner<'_> {
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
        let types = type_annotations.vertex_annotations_of(label.type_label()).cloned().unwrap_or_default();
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
        let types = type_annotations.vertex_annotations_of(role_name.type_()).cloned().unwrap_or_default();
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
        let types = type_annotations.vertex_annotations_of(kind.type_()).cloned().unwrap_or_default();
        Self {
            constraint: TypeListConstraint::Kind(kind),
            var: variable_index[&kind.type_().as_variable().unwrap()],
            types,
        }
    }

    pub(crate) fn from_value_constraint(
        value: &'a Value<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
    ) -> Self {
        let types = type_annotations.vertex_annotations_of(value.attribute_type()).cloned().unwrap_or_default();
        Self {
            constraint: TypeListConstraint::Value(value),
            var: variable_index[&value.attribute_type().as_variable().unwrap()],
            types,
        }
    }

    pub(crate) fn constraint(&self) -> &TypeListConstraint<'a> {
        &self.constraint
    }

    pub(crate) fn lower(&self) -> ConstraintInstruction<Variable> {
        let var = self.constraint.var();
        ConstraintInstruction::TypeList(TypeListInstruction::new(var, self.types.clone()))
    }

    pub(crate) fn lower_check(&self) -> CheckInstruction<Variable> {
        CheckInstruction::TypeList { type_var: self.constraint.var(), types: self.types.clone() }
    }
}

impl Costed for TypeListPlanner<'_> {
    fn cost_and_metadata(&self, _vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::in_mem_complex_with_ratio(self.types.len() as f64), CostMetaData::Direction(Direction::Canonical))
    }
}

#[derive(Clone)]
pub(crate) struct IidPlanner<'a> {
    iid: &'a Iid<Variable>,
    var: VariableVertexId,
}

impl fmt::Debug for IidPlanner<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IidPlanner").field("iid", self.iid).finish()
    }
}

impl<'a> IidPlanner<'a> {
    pub(crate) fn from_constraint(
        iid: &'a Iid<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        _type_annotations: &TypeAnnotations,
        _statistics: &Statistics,
    ) -> Self {
        let var = variable_index[&iid.var().as_variable().unwrap()];
        Self { iid, var }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        iter::once(self.var)
    }

    pub(crate) fn iid(&self) -> &Iid<Variable> {
        self.iid
    }
}

impl Costed for IidPlanner<'_> {
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], _graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let cost = if vertex_ordering.contains(&VertexId::Variable(self.var)) {
            Cost::in_mem_simple_with_ratio(0.001) // TODO calculate properly, assuming the IID is originating from the DB
        } else {
            Cost { cost: OPEN_ITERATOR_RELATIVE_COST, io_ratio: 1.0 }
        };
        (cost, CostMetaData::None)
    }
}

#[derive(Clone)]
pub(crate) struct IsaPlanner<'a> {
    isa: &'a Isa<Variable>,
    thing: VariableVertexId,
    type_: Input,
    unrestricted_expected_size: f64,
}

impl fmt::Debug for IsaPlanner<'_> {
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
                thing_types.iter().map(|thing_type| instance_count(thing_type, statistics)).sum::<u64>() as f64
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

    fn expected_output_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        let thing = graph.elements()[&VertexId::Variable(self.thing)].as_variable().unwrap();
        let thing_selectivity = thing.restriction_based_selectivity(inputs);
        f64::max(self.unrestricted_expected_size * thing_selectivity, VariableVertex::OUTPUT_SIZE_MIN)
    }
}

impl Costed for IsaPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let thing_id = VertexId::Variable(self.thing);
        let thing = graph.elements()[&thing_id].as_variable().unwrap();

        let is_thing_bound = inputs.contains(&thing_id);
        let is_type_bound = match &self.type_ {
            Input::Fixed => true,
            Input::Variable(var) => inputs.contains(&VertexId::Variable(*var)),
        };

        let thing_size = thing.expected_output_size(inputs);
        let type_size = match &self.type_ {
            Input::Fixed => 1.0,
            Input::Variable(var) => {
                let type_id = VertexId::Variable(*var);
                let type_ = graph.elements()[&type_id].as_variable().unwrap();
                type_.expected_output_size(inputs)
            }
        };

        let mut scan_size = self.unrestricted_expected_size;
        scan_size *= thing.restriction_based_selectivity(inputs); // account for restrictions (like iid), which (we assume) can be used to reduce scan size
        scan_size = f64::max(scan_size, VariableVertex::OUTPUT_SIZE_MIN); // ???
        if is_type_bound {
            scan_size /= type_size;
        } // account for narrowed prefix
        if is_thing_bound {
            scan_size /= thing_size;
        } // account for narrowed prefix

        let cost = match is_thing_bound {
            true => 0.0,
            false => OPEN_ITERATOR_RELATIVE_COST + ADVANCE_ITERATOR_RELATIVE_COST * scan_size,
        };

        let io_ratio = scan_size;

        (Cost { cost, io_ratio }, CostMetaData::Direction(Direction::Canonical))
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

impl fmt::Debug for HasPlanner<'_> {
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

        let unbound_typed_expected_size = itertools::iproduct!(owner_types, attribute_types)
            .filter_map(|(owner, attribute)| {
                statistics.has_attribute_counts.get(&owner.as_object_type())?.get(&attribute.as_attribute_type())
            })
            .sum::<u64>() as f64;

        //  We should compute that we are doing multiple seeks() and merge-sorting.
        //  in general, we assume the cardinality is small, so we just open 1 iterator and post-filter
        let unbound_typed_expected_size_canonical = owner_types
            .iter()
            .filter_map(|owner| statistics.has_attribute_counts.get(&owner.as_object_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let unbound_typed_expected_size_reverse = attribute_types
            .iter()
            .filter_map(|attribute| statistics.attribute_owner_counts.get(&attribute.as_attribute_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        Self {
            has,
            owner: variable_index[&owner.as_variable().unwrap()],
            attribute: variable_index[&attribute.as_variable().unwrap()],
            unbound_typed_expected_size,
            unbound_typed_expected_size_canonical,
            unbound_typed_expected_size_reverse,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.owner, self.attribute].into_iter()
    }

    pub(crate) fn has(&self) -> &Has<Variable> {
        self.has
    }

    fn unbound_direction(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> Direction {
        if self.unbound_expected_scan_size_canonical(graph, inputs)
            < self.unbound_expected_scan_size_reverse(graph, inputs)
        {
            Direction::Canonical
        } else {
            Direction::Reverse
        }
    }

    fn unbound_expected_scan_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        f64::min(
            self.unbound_expected_scan_size_canonical(graph, inputs),
            self.unbound_expected_scan_size_reverse(graph, inputs),
        )
    }

    fn unbound_expected_scan_size_canonical(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        let owner = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        self.unbound_typed_expected_size_canonical * owner.restriction_based_selectivity(inputs)
    }

    fn unbound_expected_scan_size_reverse(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        let attribute = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        self.unbound_typed_expected_size_reverse * attribute.restriction_based_selectivity(inputs)
    }

    fn expected_output_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        // get selectivity of attribute and multiply it by the expected size based on types
        let attribute = &graph.elements()[&VertexId::Variable(self.attribute)].as_variable().unwrap();
        let owner = &graph.elements()[&VertexId::Variable(self.owner)].as_variable().unwrap();
        let expected_size = self.unbound_typed_expected_size
            * attribute.restriction_based_selectivity(inputs)
            * owner.restriction_based_selectivity(inputs);
        f64::max(expected_size, VariableVertex::OUTPUT_SIZE_MIN)
    }
}

impl Costed for HasPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let owner_id = VertexId::Variable(self.owner);
        let owner = &graph.elements()[&owner_id].as_variable().unwrap();

        let attribute_id = VertexId::Variable(self.attribute);
        let attribute = &graph.elements()[&attribute_id].as_variable().unwrap();

        let is_owner_bound = inputs.contains(&owner_id);
        let is_attribute_bound = inputs.contains(&attribute_id);

        let owner_size = owner.expected_output_size(inputs);
        let attribute_size = attribute.expected_output_size(inputs);

        let mut scan_size_canonical = self.unbound_typed_expected_size_canonical;
        if is_owner_bound {
            scan_size_canonical /= owner_size; // accounts for bound prefix
            if is_attribute_bound {
                scan_size_canonical /= attribute_size; // accounts for bound prefix
            }
        } else {
            scan_size_canonical *= owner.restriction_based_selectivity(inputs); // account for restrictions (like iid)
        }
        scan_size_canonical = scan_size_canonical.max(MIN_SCAN_SIZE);

        let mut scan_size_reverse = self.unbound_typed_expected_size_reverse;
        if is_attribute_bound {
            scan_size_reverse /= attribute_size; // accounts for bound prefix
            if is_owner_bound {
                scan_size_reverse /= owner_size; // accounts for bound prefix
            }
        } else {
            // account for restrictions (like ==)
            scan_size_reverse *= attribute.restriction_based_selectivity(inputs);
        }
        scan_size_reverse = scan_size_reverse.max(MIN_SCAN_SIZE);

        let mut io_ratio = self.unbound_typed_expected_size;
        if is_owner_bound {
            io_ratio /= owner_size;
        }
        if is_attribute_bound {
            io_ratio /= attribute_size;
        }
        io_ratio *= owner.restriction_based_selectivity(inputs) * attribute.restriction_based_selectivity(inputs);

        let cost: f64;
        let direction: Direction;

        if scan_size_canonical <= scan_size_reverse {
            cost = OPEN_ITERATOR_RELATIVE_COST + ADVANCE_ITERATOR_RELATIVE_COST * scan_size_canonical;
            direction = Direction::Canonical;
        } else {
            cost = OPEN_ITERATOR_RELATIVE_COST + ADVANCE_ITERATOR_RELATIVE_COST * scan_size_reverse;
            direction = Direction::Reverse;
        }
        (Cost { cost, io_ratio }, CostMetaData::Direction(direction))
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

impl fmt::Debug for LinksPlanner<'_> {
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

        let constraint_types = type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_links();

        let unbound_typed_expected_size = constraint_types
            .relation_to_role()
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

        let unbound_typed_expected_size_canonical = relation_types
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

        let unbound_typed_expected_size_reverse = player_types
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
            unbound_typed_expected_size,
            unbound_typed_expected_size_canonical,
            unbound_typed_expected_size_reverse,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.relation, self.player, self.role].into_iter()
    }

    pub(crate) fn links(&self) -> &Links<Variable> {
        self.links
    }

    pub(crate) fn unbound_direction(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> Direction {
        if self.unbound_expected_scan_size_canonical(graph, inputs)
            < self.unbound_expected_scan_size_reverse(graph, inputs)
        {
            Direction::Canonical
        } else {
            Direction::Reverse
        }
    }

    fn unbound_expected_scan_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        f64::max(
            f64::min(
                self.unbound_expected_scan_size_canonical(graph, inputs),
                self.unbound_expected_scan_size_reverse(graph, inputs),
            ),
            VariableVertex::OUTPUT_SIZE_MIN,
        )
    }

    fn unbound_expected_scan_size_canonical(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        let relation = &graph.elements()[&VertexId::Variable(self.relation)].as_variable().unwrap();
        self.unbound_typed_expected_size_canonical * relation.restriction_based_selectivity(inputs)
    }

    fn unbound_expected_scan_size_reverse(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        let player = &graph.elements()[&VertexId::Variable(self.player)].as_variable().unwrap();
        self.unbound_typed_expected_size_reverse * player.restriction_based_selectivity(inputs)
    }

    fn expected_output_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        // get selectivity of attribute and multiply it by the expected size based on types
        let player = &graph.elements()[&VertexId::Variable(self.player)].as_variable().unwrap();
        let relation = &graph.elements()[&VertexId::Variable(self.relation)].as_variable().unwrap();
        f64::max(
            self.unbound_typed_expected_size
                * player.restriction_based_selectivity(inputs)
                * relation.restriction_based_selectivity(inputs),
            VariableVertex::OUTPUT_SIZE_MIN,
        )
    }
}

impl Costed for LinksPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let relation_id = VertexId::Variable(self.relation);
        let relation = &graph.elements()[&relation_id].as_variable().unwrap();

        let player_id = VertexId::Variable(self.player);
        let player = &graph.elements()[&player_id].as_variable().unwrap();

        let is_relation_bound = inputs.contains(&relation_id);
        let is_player_bound = inputs.contains(&player_id);

        let relation_size = relation.expected_output_size(inputs);
        let player_size = player.expected_output_size(inputs);

        let mut scan_size_canonical = self.unbound_typed_expected_size_canonical;
        if is_relation_bound {
            scan_size_canonical /= relation_size;
            if is_player_bound {
                scan_size_canonical /= player_size;
            }
        } else {
            scan_size_canonical *= relation.restriction_based_selectivity(inputs);
            // account for restrictions (like iid)
        }
        scan_size_canonical = scan_size_canonical.max(MIN_SCAN_SIZE);

        let mut scan_size_reverse = self.unbound_typed_expected_size_reverse;
        if is_player_bound {
            scan_size_reverse /= player_size;
            if is_relation_bound {
                scan_size_reverse /= relation_size;
            }
        } else {
            scan_size_reverse *= player.restriction_based_selectivity(inputs); // account for restrictions (like iid)
        }
        scan_size_reverse = scan_size_reverse.max(MIN_SCAN_SIZE);

        let mut io_ratio = self.unbound_typed_expected_size;
        if is_relation_bound {
            io_ratio /= relation_size;
        }
        if is_player_bound {
            io_ratio /= player_size;
        }
        io_ratio *= relation.restriction_based_selectivity(inputs) * player.restriction_based_selectivity(inputs);

        let cost: f64;
        let direction: Direction;

        if scan_size_canonical <= scan_size_reverse {
            cost = OPEN_ITERATOR_RELATIVE_COST + ADVANCE_ITERATOR_RELATIVE_COST * scan_size_canonical;
            direction = Direction::Canonical;
        } else {
            cost = OPEN_ITERATOR_RELATIVE_COST + ADVANCE_ITERATOR_RELATIVE_COST * scan_size_reverse;
            direction = Direction::Reverse;
        }
        (Cost { cost, io_ratio }, CostMetaData::Direction(direction))
    }
}

#[derive(Clone)]
pub(crate) struct IndexedRelationPlanner<'a> {
    indexed_relation: &'a IndexedRelation<Variable>,
    pub player_1: VariableVertexId,
    pub player_2: VariableVertexId,
    pub relation: VariableVertexId,
    pub role_1: VariableVertexId,
    pub role_2: VariableVertexId,
    unbound_typed_expected_size: f64,
    unbound_typed_expected_size_canonical: f64,
    unbound_typed_expected_size_reverse: f64,
}

impl fmt::Debug for IndexedRelationPlanner<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedRelationPlanner").field("indexed_relation", &self.indexed_relation).finish()
    }
}

impl<'a> IndexedRelationPlanner<'a> {
    pub(crate) fn from_constraint(
        indexed_relation: &'a IndexedRelation<Variable>,
        variable_index: &HashMap<Variable, VariableVertexId>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let player_1 = indexed_relation.player_1();
        let player_2 = indexed_relation.player_2();
        let relation = indexed_relation.relation();
        let role_1 = indexed_relation.role_type_1();
        let role_2 = indexed_relation.role_type_2();

        let _player_1_types = &**type_annotations.vertex_annotations_of(player_1).unwrap();
        let _player_2_types = &**type_annotations.vertex_annotations_of(player_2).unwrap();
        let _relation_types = &**type_annotations.vertex_annotations_of(relation).unwrap();

        // let constraint_types =
        //     type_annotations.constraint_annotations_of(indexed_relation.clone().into()).unwrap().as_links();

        let unbound_typed_expected_size = 10.0;
        let unbound_typed_expected_size_canonical = 10.0;
        let unbound_typed_expected_size_reverse = 10.0;

        let player_1 = player_1.as_variable().unwrap();
        let player_2 = player_2.as_variable().unwrap();
        let relation = relation.as_variable().unwrap();
        let role_1 = role_1.as_variable().unwrap();
        let role_2 = role_2.as_variable().unwrap();

        Self {
            indexed_relation,
            player_1: variable_index[&player_1],
            player_2: variable_index[&player_2],
            relation: variable_index[&relation],
            role_1: variable_index[&role_1],
            role_2: variable_index[&role_2],
            unbound_typed_expected_size,
            unbound_typed_expected_size_canonical,
            unbound_typed_expected_size_reverse,
        }
    }

    fn variables(&self) -> impl Iterator<Item = VariableVertexId> {
        [self.player_1, self.player_2, self.relation, self.role_1, self.role_2].into_iter()
    }

    pub(crate) fn indexed_relation(&self) -> &IndexedRelation<Variable> {
        self.indexed_relation
    }

    /// We can choose either direction here since it's fully symmetric?
    pub(crate) fn unbound_direction(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> Direction {
        Direction::Canonical
    }

    fn unbound_expected_scan_size(&self, graph: &Graph<'_>) -> f64 {
        // TODO
        10.0
    }

    fn unbound_expected_scan_size_canonical(&self, graph: &Graph<'_>) -> f64 {
        // TODO
        10.0
    }

    fn unbound_expected_scan_size_reverse(&self, graph: &Graph<'_>) -> f64 {
        // fully symmetric?
        self.unbound_expected_scan_size(graph)
    }

    fn expected_output_size(&self, graph: &Graph<'_>, inputs: &[VertexId]) -> f64 {
        // TODO
        10.0
    }
}

impl Costed for IndexedRelationPlanner<'_> {
    fn cost_and_metadata(&self, vertex_ordering: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        // TODO: using a random cost
        (Cost { cost: 10.0, io_ratio: 2.0 }, CostMetaData::None)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubPlanner<'a> {
    sub: &'a Sub<Variable>,
    type_: Input,
    supertype: Input,
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
    fn cost_and_metadata(&self, _: &[VertexId], _: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::in_mem_complex_with_ratio(1.0), CostMetaData::Direction(Direction::Canonical))
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
    fn cost_and_metadata(&self, _: &[VertexId], _: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::in_mem_complex_with_ratio(1.0), CostMetaData::Direction(Direction::Canonical))
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
    fn cost_and_metadata(&self, _: &[VertexId], _: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::in_mem_complex_with_ratio(1.0), CostMetaData::Direction(Direction::Canonical))
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
    fn cost_and_metadata(&self, _: &[VertexId], _: &Graph<'_>) -> (Cost, CostMetaData) {
        (Cost::in_mem_complex_with_ratio(1.0), CostMetaData::Direction(Direction::Canonical))
    }
}

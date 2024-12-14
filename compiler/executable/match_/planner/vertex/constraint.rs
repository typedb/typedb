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
use std::fmt::Formatter;
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

impl<'a> fmt::Display for ConstraintVertex<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            ConstraintVertex::TypeList(_) => { write!(f, "|TypeId|") } //TODO
            ConstraintVertex::Iid(_) => { write!(f, "|ThingId|") } //TODO
            ConstraintVertex::Isa(p) => { write!(f, "|{:?} isa {:?}|", p.isa.thing, p.isa.type_ ) }
            ConstraintVertex::Has(p) => { write!(f, "|{:?} has {:?}|", p.has.owner(), p.has.attribute()) }
            ConstraintVertex::Links(p) => { write!(f, "|{:?} links {:?}|", p.links.relation(), p.links.player()) }
            ConstraintVertex::Sub(_) => { write!(f, "|Sub|") } //TODO
            ConstraintVertex::Owns(_) => { write!(f, "|Owns|") } //TODO
            ConstraintVertex::Relates(_) => { write!(f, "|Relates|") } //TODO
            ConstraintVertex::Plays(_) => { write!(f, "|Plays|") } //TODO
            ConstraintVertex::IndexedRelation(_) => {  write!(f, "|Relation Index|") }
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
    pub(crate) unrestricted_expected_size: f64,
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

    pub(crate) fn thing_estimates(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64, f64) {
        let thing_id = VertexId::Variable(self.thing);
        let thing = graph.elements()[&thing_id].as_variable().unwrap();
        let is_thing_bound = inputs.contains(&thing_id);
        let thing_size = thing.unrestricted_expected_output_size();
        let thing_selectivity = thing.restriction_based_selectivity(inputs);
        (is_thing_bound, thing_size, thing_selectivity)
    }

    pub(crate) fn type_estimate(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64) {
        let is_type_bound = match &self.type_ {
            Input::Fixed => true,
            Input::Variable(var) => inputs.contains(&VertexId::Variable(*var)),
        };
        let num_types = match &self.type_ {
            Input::Fixed => 1.0,
            Input::Variable(var) => {
                let type_id = VertexId::Variable(*var);
                let type_ = graph.elements()[&type_id].as_variable().unwrap();
                type_.expected_output_size(inputs)
            }
        };
        (is_type_bound, num_types)
    }

    pub(crate) fn output_size_estimate(&self, is_thing_bound: bool, thing_size: f64, thing_selectivity: f64, is_type_bound: bool, num_types: f64) -> f64 {
        let mut scan_size = self.unrestricted_expected_size;
        if is_type_bound {
            scan_size /= num_types; // account for narrowed prefix
        }
        if is_thing_bound {
            scan_size /= thing_size;
        } else {
            scan_size *= thing_selectivity; // account for restrictions (like iid), which (we assume) can be used to reduce scan size
        }
        scan_size = f64::max(scan_size, VariableVertex::OUTPUT_SIZE_MIN); // TODO: verify if this is useful (part of previous model)
        scan_size
    }
}

impl Costed for IsaPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let (is_thing_bound, thing_size, thing_selectivity) = self.thing_estimates(inputs, graph);
        let (is_type_bound, num_types) = self.type_estimate(inputs, graph);
        let scan_size = self.output_size_estimate(is_thing_bound, thing_size, thing_selectivity, is_type_bound, num_types);
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
    pub unbound_typed_expected_size: f64,
    pub unbound_typed_expected_size_canonical: f64,
    pub unbound_typed_expected_size_reverse: f64,
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

    pub(crate) fn owner_estimates(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64, f64) {
        let owner_id = VertexId::Variable(self.owner);
        let owner = &graph.elements()[&owner_id].as_variable().unwrap();
        let is_owner_bound = inputs.contains(&owner_id);
        let owner_size = owner.unrestricted_expected_output_size();
        let owner_selectivity = owner.restriction_based_selectivity(inputs);
        (is_owner_bound, owner_size, owner_selectivity)
    }

    pub(crate) fn attribute_estimates(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64, f64) {
        let attribute_id = VertexId::Variable(self.attribute);
        let attribute = &graph.elements()[&attribute_id].as_variable().unwrap();
        let is_attribute_bound = inputs.contains(&attribute_id);
        let attribute_size = attribute.unrestricted_expected_output_size();
        let attribute_selectivity = attribute.restriction_based_selectivity(inputs);
        (is_attribute_bound, attribute_size, attribute_selectivity)
    }

    pub(crate) fn canonical_scan_size_estimate(&self,
                                               is_owner_bound: bool,
                                               owner_size: f64,
                                               owner_selectivity: f64,
                                               is_attribute_bound: bool,
                                               attribute_size: f64,
    ) -> f64 {
        let mut scan_size_canonical = self.unbound_typed_expected_size_canonical;
        if is_owner_bound {
            scan_size_canonical /= owner_size;
            if is_attribute_bound {
                scan_size_canonical /= attribute_size;
            }
        } else {
            scan_size_canonical *= owner_selectivity;  // restrictions (like iid) apply if var still unbound
        }
        scan_size_canonical.max(MIN_SCAN_SIZE)
    }


    pub(crate) fn reverse_scan_size_estimate(&self,
                                             is_owner_bound: bool,
                                             owner_size: f64,
                                             is_attribute_bound: bool,
                                             attribute_size: f64,
                                             attribute_selectivity: f64
    ) -> f64 {
        let mut scan_size_reverse = self.unbound_typed_expected_size_reverse;
        if is_attribute_bound {
            scan_size_reverse /= attribute_size;
            if is_owner_bound {
                scan_size_reverse /= owner_size;
            }
        } else {
            scan_size_reverse *= attribute_selectivity; // restrictions (like iid) apply if var still unbound
        }
        scan_size_reverse.max(MIN_SCAN_SIZE)
    }

    pub(crate) fn output_size_estimate(&self,
                                       is_owner_bound: bool,
                                       owner_size: f64,
                                       owner_selectivity: f64,
                                       is_attribute_bound: bool,
                                       attribute_size: f64,
                                       attribute_selectivity: f64
    ) -> f64 {
        let mut scan_size = self.unbound_typed_expected_size;
        if is_owner_bound {
            scan_size /= owner_size;
        } else {
            scan_size *= owner_selectivity;
        }
        if is_attribute_bound {
            scan_size /= attribute_size;
        } else {
            scan_size *= attribute_selectivity;
        }
        scan_size
    }
}

impl Costed for HasPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let (is_owner_bound, owner_size, owner_selectivity) = self.owner_estimates(inputs, graph);
        let (is_attribute_bound, attribute_size, attribute_selectivity) = self.attribute_estimates(inputs, graph);

        let scan_size_canonical = self.canonical_scan_size_estimate(is_owner_bound, owner_size, owner_selectivity, is_attribute_bound, attribute_size,);
        let scan_size_reverse= self.reverse_scan_size_estimate(is_owner_bound, owner_size, is_attribute_bound, attribute_size, attribute_selectivity);
        let mut io_ratio = self.output_size_estimate(is_owner_bound, owner_size, owner_selectivity, is_attribute_bound, attribute_size, attribute_selectivity);
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
    pub(crate) unbound_typed_expected_size: f64,
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

    pub(crate) fn relation_estimates(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64, f64) {
        let relation_id = VertexId::Variable(self.relation);
        let relation = &graph.elements()[&relation_id].as_variable().unwrap();
        let is_relation_bound = inputs.contains(&relation_id);
        let relation_size = relation.unrestricted_expected_output_size();
        let relation_selectivity = relation.restriction_based_selectivity(inputs);
        (is_relation_bound, relation_size, relation_selectivity)
    }

    pub(crate) fn player_estimates(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (bool, f64, f64) {
        let player_id = VertexId::Variable(self.player);
        let player = &graph.elements()[&player_id].as_variable().unwrap();
        let is_player_bound = inputs.contains(&player_id);
        let player_size = player.unrestricted_expected_output_size();
        let player_selectivity = player.restriction_based_selectivity(inputs);
        (is_player_bound, player_size, player_selectivity)
    }

    pub(crate) fn canonical_scan_size_estimate(&self,
                                               is_relation_bound: bool,
                                               relation_size: f64,
                                               relation_selectivity: f64,
                                               is_player_bound: bool,
                                               player_size: f64,
    ) -> f64 {
        let mut scan_size_canonical = self.unbound_typed_expected_size_canonical;
        if is_relation_bound {
            scan_size_canonical /= relation_size;
            if is_player_bound {
                scan_size_canonical /= player_size;
            }
        } else {
            scan_size_canonical *= relation_selectivity;  // restrictions (like iid) apply if var still unbound
        }
        scan_size_canonical.max(MIN_SCAN_SIZE)
    }


    pub(crate) fn reverse_scan_size_estimate(&self,
                                               is_relation_bound: bool,
                                               relation_size: f64,
                                               is_player_bound: bool,
                                               player_size: f64,
                                               player_selectivity: f64
    ) -> f64 {
        let mut scan_size_reverse = self.unbound_typed_expected_size_reverse;
        if is_player_bound {
            scan_size_reverse /= player_size;
            if is_relation_bound {
                scan_size_reverse /= relation_size;
            }
        } else {
            scan_size_reverse *= player_selectivity; // restrictions (like iid) apply if var still unbound
        }
        scan_size_reverse.max(MIN_SCAN_SIZE)
    }

    pub(crate) fn output_size_estimate(&self,
                                       is_relation_bound: bool,
                                       relation_size: f64,
                                       relation_selectivity: f64,
                                       is_player_bound: bool,
                                       player_size: f64,
                                       player_selectivity: f64
    ) -> f64 {
        let mut scan_size = self.unbound_typed_expected_size;
        if is_relation_bound {
            scan_size /= relation_size;
        } else {
            scan_size *= relation_selectivity;
        }
        if is_player_bound {
            scan_size /= player_size;
        } else {
            scan_size *= player_selectivity;
        }
        scan_size
    }
}

impl Costed for LinksPlanner<'_> {
    fn cost_and_metadata(&self, inputs: &[VertexId], graph: &Graph<'_>) -> (Cost, CostMetaData) {
        let (is_relation_bound, relation_size, relation_selectivity) = self.relation_estimates(inputs, graph);
        let (is_player_bound, player_size, player_selectivity) = self.player_estimates(inputs, graph);

        let scan_size_canonical = self.canonical_scan_size_estimate(is_relation_bound, relation_size, relation_selectivity, is_player_bound, player_size,);
        let scan_size_reverse= self.reverse_scan_size_estimate(is_relation_bound, relation_size, is_player_bound, player_size, player_selectivity);
        let mut io_ratio = self.output_size_estimate(is_relation_bound, relation_size, relation_selectivity, is_player_bound, player_size, player_selectivity);
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
        (Cost::in_mem_complex_with_ratio(1.0), CostMetaData::Direction(Direction::Reverse))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OwnsPlanner<'a> {
    owns: &'a Owns<Variable>,
    owner: Input,
    attribute: Input,
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
        Self { owns, owner, attribute }
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
        Self { relates, relation, role_type }
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
        Self { plays, player, role_type }
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

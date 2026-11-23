/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{
        Capability,
        annotation::AnnotationCardinality,
        constraint::{CapabilityConstraint, Constraint, get_cardinality_constraints},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        type_manager::type_reader::TypeReader,
    },
};

#[derive(Debug)]
pub(crate) struct SchemaCardinalities {
    pub(crate) owns: HashMap<ObjectType, TypeCardinalities<Owns>>,
    pub(crate) plays: HashMap<ObjectType, TypeCardinalities<Plays>>,
    pub(crate) relates: HashMap<RelationType, TypeCardinalities<Relates>>,
}

#[derive(Debug)]
pub(crate) struct TypeCardinalities<CAP: Capability> {
    cardinalities: Vec<CardinalityDescription<CAP>>,
    counted_by: HashMap<CAP::InterfaceType, Vec<usize>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CardinalityDescription<CAP: Capability> {
    pub(crate) constraint: CapabilityConstraint<CAP>,
    // cache to support diffing between two different cardinality constraints, and to avoid recomputing
    covered_interface_types: HashSet<CAP::InterfaceType>,
}

impl SchemaCardinalities {
    pub(crate) fn new(snapshot: &impl ReadableSnapshot) -> Result<Self, Box<ConceptReadError>> {
        let mut owns = HashMap::new();
        let mut plays = HashMap::new();
        for object_type in TypeReader::get_object_types(snapshot)? {
            let owns_constraints = TypeReader::get_type_capabilities_constraints::<Owns>(snapshot, object_type)?;
            if let Some(cardinalities) = Self::type_cardinalities(snapshot, owns_constraints)? {
                owns.insert(object_type, cardinalities);
            }
            let plays_constraints = TypeReader::get_type_capabilities_constraints::<Plays>(snapshot, object_type)?;
            if let Some(cardinalities) = Self::type_cardinalities(snapshot, plays_constraints)? {
                plays.insert(object_type, cardinalities);
            }
        }

        let mut relates = HashMap::new();
        for relation_type in TypeReader::get_relation_types(snapshot)? {
            let relates_constraints =
                TypeReader::get_type_capabilities_constraints::<Relates>(snapshot, relation_type)?;
            if let Some(cardinalities) = Self::type_cardinalities(snapshot, relates_constraints)? {
                relates.insert(relation_type, cardinalities);
            }
        }

        Ok(Self { owns, plays, relates })
    }

    fn type_cardinalities<CAP: Capability>(
        snapshot: &impl ReadableSnapshot,
        constraints_by_interface_type: HashMap<CAP::InterfaceType, HashSet<CapabilityConstraint<CAP>>>,
    ) -> Result<Option<TypeCardinalities<CAP>>, Box<ConceptReadError>> {
        let mut cardinalities = Vec::new();
        for constraint in get_cardinality_constraints(constraints_by_interface_type.values().flatten()) {
            if !constraint.requires_validation() {
                continue;
            }
            let covered_interface_types = TypeReader::get_constraint_covered_interface_types(snapshot, &constraint)?;
            cardinalities.push(CardinalityDescription { constraint, covered_interface_types });
        }
        Ok((!cardinalities.is_empty()).then(|| TypeCardinalities::new(cardinalities)))
    }

    // Keep only the cardinalities that instances satisfying the previous state might now violate
    pub(crate) fn diff_from(&self, previous: &SchemaCardinalities) -> Self {
        Self {
            owns: Self::cardinalities_to_revalidate(&self.owns, &previous.owns),
            plays: Self::cardinalities_to_revalidate(&self.plays, &previous.plays),
            relates: Self::cardinalities_to_revalidate(&self.relates, &previous.relates),
        }
    }

    fn cardinalities_to_revalidate<CAP: Capability>(
        current: &HashMap<CAP::ObjectType, TypeCardinalities<CAP>>,
        previous: &HashMap<CAP::ObjectType, TypeCardinalities<CAP>>,
    ) -> HashMap<CAP::ObjectType, TypeCardinalities<CAP>> {
        let mut to_revalidate = HashMap::new();
        for (object_type, type_cardinalities) in current {
            let previous_by_source: HashMap<CAP, &CardinalityDescription<CAP>> =
                (previous.get(object_type).into_iter())
                    .flat_map(|previous| &previous.cardinalities)
                    .map(|cardinality| (cardinality.constraint.source(), cardinality))
                    .collect();
            let revalidated: Vec<_> = type_cardinalities
                .cardinalities
                .iter()
                .filter(|description| {
                    let Some(previous) = previous_by_source.get(&description.constraint.source()) else {
                        return true;
                    };
                    // if the previous state was not strictly contained within the current one,
                    // we have to recheck the card
                    if !description.cardinality().narrowed_correctly_by(&previous.cardinality()) {
                        return true;
                    }
                    // counting more instances can only break an upper bound, and counting fewer only a lower bound
                    let counts_more = description
                        .covered_interface_types
                        .difference(&previous.covered_interface_types)
                        .next()
                        .is_some();
                    let counts_fewer = previous
                        .covered_interface_types
                        .difference(&description.covered_interface_types)
                        .next()
                        .is_some();
                    (counts_more && description.cardinality().end().is_some())
                        || (counts_fewer && description.cardinality().start() > 0)
                })
                .cloned()
                .collect();
            if !revalidated.is_empty() {
                to_revalidate.insert(*object_type, TypeCardinalities::new(revalidated));
            }
        }
        to_revalidate
    }
}

impl<CAP: Capability> TypeCardinalities<CAP> {
    fn new(cardinalities: Vec<CardinalityDescription<CAP>>) -> Self {
        let mut counted_by: HashMap<CAP::InterfaceType, Vec<usize>> = HashMap::new();
        for (index, cardinality) in cardinalities.iter().enumerate() {
            for &interface_type in &cardinality.covered_interface_types {
                counted_by.entry(interface_type).or_default().push(index);
            }
        }
        Self { cardinalities, counted_by }
    }

    pub(crate) fn counters<'a>(&'a self, storage: &'a mut Vec<u64>) -> CardinalityCounters<'a, CAP> {
        storage.clear();
        storage.resize(self.cardinalities.len(), 0);
        CardinalityCounters { cardinalities: self, cardinality_counts: storage }
    }

    pub(crate) fn counts_any<'b>(&self, interface_types: impl IntoIterator<Item = &'b CAP::InterfaceType>) -> bool
    where
        CAP::InterfaceType: 'b,
    {
        interface_types.into_iter().any(|interface_type| self.counted_by.contains_key(interface_type))
    }
}

impl<CAP: Capability> CardinalityDescription<CAP> {
    pub(crate) fn counts_any<'b>(&self, interface_types: impl IntoIterator<Item = &'b CAP::InterfaceType>) -> bool
    where
        CAP::InterfaceType: 'b,
    {
        interface_types.into_iter().any(|interface_type| self.covered_interface_types.contains(interface_type))
    }

    fn cardinality(&self) -> AnnotationCardinality {
        self.constraint.description().unwrap_cardinality().expect("Only Cardinality constraints")
    }
}

// Instantiate in order to count cardinalities indexed to the constraints
pub(crate) struct CardinalityCounters<'a, CAP: Capability> {
    cardinalities: &'a TypeCardinalities<CAP>,
    cardinality_counts: &'a mut Vec<u64>,
}

impl<'a, CAP: Capability> CardinalityCounters<'a, CAP> {
    pub(crate) fn count(&mut self, interface_type: CAP::InterfaceType, count: u64) {
        if let Some(indices) = self.cardinalities.counted_by.get(&interface_type) {
            for &index in indices {
                self.cardinality_counts[index] += count;
            }
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&'a CardinalityDescription<CAP>, u64)> + '_ {
        self.cardinalities.cardinalities.iter().zip(self.cardinality_counts.iter().copied())
    }

    pub(crate) fn reset(&mut self) {
        self.cardinality_counts.fill(0);
    }
}

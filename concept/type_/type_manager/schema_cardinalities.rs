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
        Capability, TypeAPI,
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

// the cardinalities instances of each type must satisfy, and the interface types each of them counts
#[derive(Debug)]
pub(crate) struct SchemaCardinalities {
    pub(crate) owns: HashMap<ObjectType, Vec<CardinalityDescription<Owns>>>,
    pub(crate) plays: HashMap<ObjectType, Vec<CardinalityDescription<Plays>>>,
    pub(crate) relates: HashMap<RelationType, Vec<CardinalityDescription<Relates>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CardinalityDescription<CAP: Capability> {
    pub(crate) constraint: CapabilityConstraint<CAP>,
    pub(crate) cardinality: AnnotationCardinality,
    pub(crate) relevant_types: HashSet<CAP::InterfaceType>,
}

impl SchemaCardinalities {
    pub(crate) fn new(snapshot: &impl ReadableSnapshot) -> Result<Self, Box<ConceptReadError>> {
        let mut owns = HashMap::new();
        let mut plays = HashMap::new();
        for object_type in TypeReader::get_object_types(snapshot)? {
            let owns_constraints = TypeReader::get_type_capabilities_constraints::<Owns>(snapshot, object_type)?;
            owns.insert(object_type, Self::describe_cardinalities(snapshot, owns_constraints)?);
            let plays_constraints = TypeReader::get_type_capabilities_constraints::<Plays>(snapshot, object_type)?;
            plays.insert(object_type, Self::describe_cardinalities(snapshot, plays_constraints)?);
        }

        let mut relates = HashMap::new();
        for relation_type in TypeReader::get_relation_types(snapshot)? {
            let relates_constraints =
                TypeReader::get_type_capabilities_constraints::<Relates>(snapshot, relation_type)?;
            relates.insert(relation_type, Self::describe_cardinalities(snapshot, relates_constraints)?);
        }

        Ok(Self { owns, plays, relates })
    }

    fn describe_cardinalities<CAP: Capability>(
        snapshot: &impl ReadableSnapshot,
        constraints_by_interface_type: HashMap<CAP::InterfaceType, HashSet<CapabilityConstraint<CAP>>>,
    ) -> Result<Vec<CardinalityDescription<CAP>>, Box<ConceptReadError>> {
        let mut cardinalities = Vec::new();
        for constraint in get_cardinality_constraints(constraints_by_interface_type.into_values().flatten()) {
            if !constraint.requires_validation() {
                continue;
            }
            let cardinality = constraint.description().unwrap_cardinality().expect("Only Cardinality constraints");
            let source_interface_type = constraint.source().interface();
            let sub_interface_types = TypeReader::get_subtypes_transitive(snapshot, source_interface_type)?;
            let relevant_types = TypeAPI::chain_types(source_interface_type, sub_interface_types).collect();
            cardinalities.push(CardinalityDescription { constraint, cardinality, relevant_types });
        }
        Ok(cardinalities)
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
        current: &HashMap<CAP::ObjectType, Vec<CardinalityDescription<CAP>>>,
        previous: &HashMap<CAP::ObjectType, Vec<CardinalityDescription<CAP>>>,
    ) -> HashMap<CAP::ObjectType, Vec<CardinalityDescription<CAP>>> {
        let mut to_revalidate = HashMap::new();
        for (object_type, cardinality_descriptions) in current {
            let previous_by_source: HashMap<CAP, &CardinalityDescription<CAP>> =
                (previous.get(object_type).into_iter())
                    .flatten()
                    .map(|cardinality| (cardinality.constraint.source(), cardinality))
                    .collect();
            let revalidated: Vec<_> = cardinality_descriptions
                .iter()
                .filter(|description| {
                    let Some(previous) = previous_by_source.get(&description.constraint.source()) else {
                        return true;
                    };
                    // if the previous state was not strictly contained within the current one,
                    // we have to recheck the card
                    if !description.cardinality.narrowed_correctly_by(&previous.cardinality) {
                        return true;
                    }
                    // counting more instances can only break an upper bound, and counting fewer only a lower bound
                    let counts_more = description.relevant_types.difference(&previous.relevant_types).next().is_some();
                    let counts_fewer = previous.relevant_types.difference(&description.relevant_types).next().is_some();
                    (counts_more && description.cardinality.end().is_some())
                        || (counts_fewer && description.cardinality.start() > 0)
                })
                .cloned()
                .collect();
            if !revalidated.is_empty() {
                to_revalidate.insert(*object_type, revalidated);
            }
        }
        to_revalidate
    }
}

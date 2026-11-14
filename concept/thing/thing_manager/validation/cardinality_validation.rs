/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use concurrency::parallel_flat_map;
use encoding::Keyable;
use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    iterator::{InstanceEdgeWalk, InstanceIterator},
    thing::{
        ThingAPI,
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{
            ThingManager,
            validation::{DataValidationError, validation::DataValidation},
        },
    },
    type_::{
        Capability,
        constraint::{CapabilityConstraint, Constraint},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::RelationType,
        type_manager::schema_cardinalities::{CardinalityDescription, SchemaCardinalities},
    },
};

macro_rules! collect_errors {
    ($vec:ident, $expr:expr, $wrap:expr) => {
        if let Err(e) = $expr {
            $vec.push($wrap(e));
        }
    };

    ($vec:ident, $expr:expr) => {
        if let Err(e) = $expr {
            $vec.push(e);
        }
    };
}

macro_rules! capability_cardinality_validation {
    (
        $validate_func_name:ident,
        $validate_existing_func_name:ident,
        $capability_type:ident,
        $object_instance:ident,
        $get_interface_counts_func:ident,
        $check_func:path
    ) => {
        fn $validate_existing_func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            cardinalities: impl Iterator<Item = &'a CardinalityDescription<$capability_type>>,
            interface_types: &HashSet<<$capability_type as Capability>::InterfaceType>,
            out_errors: &mut Vec<DataValidationError>,
            storage_counters: StorageCounters,
        ) -> Result<(), Box<ConceptReadError>> {
            let key = object.vertex().into_storage_key();
            if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_some() {
                return Ok(());
            }
            let mut affected_cardinalities = cardinalities
                .filter(|cardinality| {
                    interface_types.iter().any(|interface_type| cardinality.relevant_types.contains(interface_type))
                })
                .peekable();
            if affected_cardinalities.peek().is_none()
                || !thing_manager.is_persisted_in_storage(snapshot, object.vertex(), storage_counters.clone())?
            {
                return Ok(());
            }
            let check =
                Self::$validate_func_name(snapshot, thing_manager, object, affected_cardinalities, storage_counters);
            collect_errors!(out_errors, check, |e: Box<_>| *e);
            Ok(())
        }

        fn $validate_func_name<'a>(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            cardinalities: impl Iterator<Item = &'a CardinalityDescription<$capability_type>>,
            storage_counters: StorageCounters,
        ) -> Result<(), Box<DataValidationError>> {
            let mut cardinalities = cardinalities.peekable();
            if cardinalities.peek().is_none() {
                return Ok(());
            }

            let counts = object
                .$get_interface_counts_func(snapshot, thing_manager, storage_counters)
                .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
            for cardinality in cardinalities {
                let count =
                    cardinality.relevant_types.iter().filter_map(|interface_type| counts.get(interface_type)).sum();
                $check_func(
                    snapshot,
                    thing_manager.type_manager(),
                    &cardinality.constraint,
                    object,
                    cardinality.constraint.source().interface(),
                    count,
                )?;
            }

            Ok(())
        }
    };
}

/*
The cardinalities validation flow is the following:
1. Every type's cardinalities are described once per schema (see SchemaCardinalities). A cardinality counts the
   instances of its source interface type and its subtypes.
2. New objects are validated against all the cardinalities of their type.
3. Existing objects with modified edges are validated against the cardinalities counting any modified interface type.
4. In schema transactions, every instance of a type is validated against the type's cardinalities that changed
   (see SchemaCardinalities::diff_from).

Let's consider the following example:
  entity person,
    owns name @card(1..),
    owns surname @card(0..), # sub name
    owns changed-surname @card(1..2); # sub surname

person's cardinalities are @card(1..), counting names, surnames, and changed-surnames, and @card(1..2), counting
changed-surnames. @card(0..) can never be violated, so it is not validated.
Deleting a surname of an existing person validates @card(1..) only, as it is the only cardinality counting surnames.
This still holds after undefining person owns surname in the same transaction, as name's @card(1..) still counts
surnames.
*/

pub(crate) struct CardinalityValidation {}

impl CardinalityValidation {
    pub(crate) fn validate_commit<Snapshot: WritableSnapshot + Sync>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinalities = thing_manager.type_manager().get_schema_cardinalities(snapshot)?;
        Self::validate_new_objects(snapshot, thing_manager, &cardinalities, out_errors, storage_counters.clone())?;
        Self::validate_existing_owners_of_modified_has(
            snapshot,
            thing_manager,
            &cardinalities,
            out_errors,
            storage_counters.clone(),
        )?;
        Self::validate_existing_players_of_modified_links(
            snapshot,
            thing_manager,
            &cardinalities,
            out_errors,
            storage_counters.clone(),
        )?;
        Self::validate_existing_relations_of_modified_links(
            snapshot,
            thing_manager,
            &cardinalities,
            out_errors,
            storage_counters.clone(),
        )?;

        if !Snapshot::IMMUTABLE_SCHEMA {
            Self::validate_instances_of_modified_cardinalities(
                snapshot,
                thing_manager,
                &cardinalities,
                out_errors,
                storage_counters,
            )?;
        }
        Ok(())
    }

    fn validate_new_objects<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_new_object(snapshot, |snapshot, object| {
            let owns_check = CardinalityValidation::validate_owns_cardinality_constraints(
                snapshot,
                thing_manager,
                object,
                cardinalities.owns.get(&object.type_()).into_iter().flatten(),
                storage_counters.clone(),
            );
            collect_errors!(out_errors, owns_check, |e: Box<_>| *e);

            let plays_check = CardinalityValidation::validate_plays_cardinality_constraints(
                snapshot,
                thing_manager,
                object,
                cardinalities.plays.get(&object.type_()).into_iter().flatten(),
                storage_counters.clone(),
            );
            collect_errors!(out_errors, plays_check, |e: Box<_>| *e);

            if let Object::Relation(relation) = object {
                let relates_check = CardinalityValidation::validate_relates_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    relation,
                    cardinalities.relates.get(&relation.type_()).into_iter().flatten(),
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, relates_check, |e: Box<_>| *e);
            }
            Ok(())
        })
    }

    fn validate_existing_owners_of_modified_has<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_owner_with_modified_has(snapshot, |snapshot, modified| {
            Self::validate_existing_object_has(
                snapshot,
                thing_manager,
                modified.owner,
                cardinalities.owns.get(&modified.owner.type_()).into_iter().flatten(),
                &modified.modified_attribute_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_existing_players_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_player_with_modified_links(snapshot, |snapshot, modified| {
            Self::validate_existing_object_links(
                snapshot,
                thing_manager,
                modified.player,
                cardinalities.plays.get(&modified.player.type_()).into_iter().flatten(),
                &modified.modified_role_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_existing_relations_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_relation_with_modified_links(snapshot, |snapshot, modified| {
            Self::validate_existing_relation_links(
                snapshot,
                thing_manager,
                modified.relation,
                cardinalities.relates.get(&modified.relation.type_()).into_iter().flatten(),
                &modified.modified_role_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_instances_of_modified_cardinalities(
        snapshot: &(impl WritableSnapshot + Sync),
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let preexisting_cardinalities = SchemaCardinalities::new(&snapshot.read_snapshot_before_writes())?;
        let modified_cardinalities = cardinalities.diff_from(&preexisting_cardinalities);

        let tasks: Vec<CardinalityTask<'_>> = (modified_cardinalities.owns.iter())
            .map(|(&object_type, descriptions)| CardinalityTask::Owns(object_type, descriptions))
            .chain(
                (modified_cardinalities.plays.iter())
                    .map(|(&object_type, descriptions)| CardinalityTask::Plays(object_type, descriptions)),
            )
            .chain(
                (modified_cardinalities.relates.iter())
                    .map(|(&relation_type, descriptions)| CardinalityTask::Relates(relation_type, descriptions)),
            )
            .collect();

        out_errors.extend(parallel_flat_map(&tasks, |task| -> Result<_, Box<ConceptReadError>> {
            let mut errors = Vec::new();
            match task {
                CardinalityTask::Owns(object_type, descriptions) => Self::validate_owns_cardinalities(
                    snapshot,
                    thing_manager,
                    &mut errors,
                    *object_type,
                    descriptions,
                    storage_counters.clone(),
                )?,
                CardinalityTask::Plays(object_type, descriptions) => Self::validate_plays_cardinalities(
                    snapshot,
                    thing_manager,
                    &mut errors,
                    *object_type,
                    descriptions,
                    storage_counters.clone(),
                )?,
                CardinalityTask::Relates(relation_type, descriptions) => Self::validate_relates_cardinalities(
                    snapshot,
                    thing_manager,
                    &mut errors,
                    *relation_type,
                    descriptions,
                    storage_counters.clone(),
                )?,
            }
            Ok(errors)
        })?);
        Ok(())
    }

    fn validate_owns_cardinalities(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        object_type: ObjectType,
        cardinality_descriptions: &[CardinalityDescription<Owns>],
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            object_type,
            cardinality_descriptions,
            |object_type| thing_manager.get_objects_in(snapshot, object_type, storage_counters.clone()),
            |object_type| thing_manager.get_has_in(snapshot, object_type, storage_counters.clone()),
            |has| (has.owner(), has.attribute().type_()),
            |constraint, object, attribute_type, count| {
                DataValidation::validate_owns_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    object,
                    attribute_type,
                    count,
                )
            },
        )
    }

    fn validate_plays_cardinalities(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        object_type: ObjectType,
        cardinality_descriptions: &[CardinalityDescription<Plays>],
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            object_type,
            cardinality_descriptions,
            |object_type| thing_manager.get_objects_in(snapshot, object_type, storage_counters.clone()),
            |object_type| thing_manager.get_links_reverse_in(snapshot, object_type, storage_counters.clone()),
            |links| (links.player(), links.role_type()),
            |constraint, object, role_type, count| {
                DataValidation::validate_plays_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    object,
                    role_type,
                    count,
                )
            },
        )
    }

    fn validate_relates_cardinalities(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        relation_type: RelationType,
        cardinality_descriptions: &[CardinalityDescription<Relates>],
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            relation_type,
            cardinality_descriptions,
            |relation_type| thing_manager.get_relations_in(snapshot, relation_type, storage_counters.clone()),
            |relation_type| thing_manager.get_links_in(snapshot, relation_type, storage_counters.clone()),
            |links| (links.relation(), links.role_type()),
            |constraint, relation, role_type, count| {
                DataValidation::validate_relates_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    relation,
                    role_type,
                    count,
                )
            },
        )
    }

    fn validate_instances_cardinalities<
        CAP: Capability,
        Instance: ThingAPI + Copy + Ord + 'static,
        Edge,
        EdgeIterator: Iterator<Item = Result<(Edge, u64), Box<ConceptReadError>>>,
    >(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        type_: CAP::ObjectType,
        cardinality_descriptions: &[CardinalityDescription<CAP>],
        get_instances_in: impl Fn(CAP::ObjectType) -> InstanceIterator<Instance>,
        get_edges_in: impl Fn(CAP::ObjectType) -> EdgeIterator,
        edge_instance_and_interface: impl Fn(&Edge) -> (Instance, CAP::InterfaceType),
        validate_cardinality: impl Fn(
            &CapabilityConstraint<CAP>,
            Instance,
            CAP::InterfaceType,
            u64,
        ) -> Result<(), Box<DataValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        // one counter per constraint, plus an index of which interface types add to which counters
        let mut counts = vec![0u64; cardinality_descriptions.len()];
        let mut counted_by: HashMap<CAP::InterfaceType, Vec<usize>> = HashMap::new();
        for (index, description) in cardinality_descriptions.iter().enumerate() {
            for &interface_type in &description.covered_interface_types {
                counted_by.entry(interface_type).or_default().push(index);
            }
        }

        let mut walk = InstanceEdgeWalk::new(get_instances_in(type_), get_edges_in(type_), |(edge, _)| {
            edge_instance_and_interface(edge).0
        });
        while let Some(instance) = walk.next_instance().transpose()? {
            while let Some((edge, count)) = walk.next_edge().transpose()? {
                let (_, interface_type) = edge_instance_and_interface(&edge);
                if let Some(indices) = counted_by.get(&interface_type) {
                    for &index in indices {
                        counts[index] += count;
                    }
                }
            }

            // instances written in this transaction are fully validated by the previous delta checks
            let key = instance.vertex().into_storage_key();
            if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_none() {
                for (description, &count) in cardinality_descriptions.iter().zip(&counts) {
                    let check = validate_cardinality(
                        &description.constraint,
                        instance,
                        description.constraint.source().interface(),
                        count,
                    );
                    collect_errors!(out_errors, check, |e: Box<_>| *e);
                }
            }
            counts.fill(0);
        }
        Ok(())
    }

    capability_cardinality_validation!(
        validate_owns_cardinality_constraints,
        validate_existing_object_has,
        Owns,
        Object,
        get_has_counts,
        DataValidation::validate_owns_instances_cardinality_constraint
    );
    capability_cardinality_validation!(
        validate_plays_cardinality_constraints,
        validate_existing_object_links,
        Plays,
        Object,
        get_played_roles_counts,
        DataValidation::validate_plays_instances_cardinality_constraint
    );
    capability_cardinality_validation!(
        validate_relates_cardinality_constraints,
        validate_existing_relation_links,
        Relates,
        Relation,
        get_player_counts,
        DataValidation::validate_relates_instances_cardinality_constraint
    );
}

enum CardinalityTask<'a> {
    Owns(ObjectType, &'a [CardinalityDescription<Owns>]),
    Plays(ObjectType, &'a [CardinalityDescription<Plays>]),
    Relates(RelationType, &'a [CardinalityDescription<Relates>]),
}

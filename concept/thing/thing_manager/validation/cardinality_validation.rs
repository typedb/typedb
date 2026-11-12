/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    panic,
    sync::atomic::{self, AtomicUsize},
    thread,
};

use encoding::Keyable;
use resource::{profile::StorageCounters, system_info::SystemInfo};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
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
        Capability, OwnerAPI, PlayerAPI, TypeAPI,
        constraint::{CapabilityConstraint, Constraint},
        owns::Owns,
        plays::Plays,
        relates::Relates,
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

use crate::{
    iterator::InstanceIterator,
    type_::{
        annotation::AnnotationCardinality, object_type::ObjectType, relation_type::RelationType,
        type_manager::TypeManager,
    },
};

macro_rules! capability_cardinality_validation {
    (
        $collect_func_name:ident,
        $validate_func_name:ident,
        $validate_existing_func_name:ident,
        $capability_type:ident,
        $object_instance:ident,
        $get_cardinality_constraints_func:ident,
        $get_interface_counts_func:ident,
        $check_func:path
    ) => {
        fn $validate_existing_func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            interface_types: &HashSet<<$capability_type as Capability>::InterfaceType>,
            out_errors: &mut Vec<DataValidationError>,
            storage_counters: StorageCounters,
        ) -> Result<(), Box<ConceptReadError>> {
            let key = object.vertex().into_storage_key();
            if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_some() {
                return Ok(());
            }
            let constraints = Self::$collect_func_name(snapshot, thing_manager, object.type_(), interface_types)?;
            if constraints.is_empty()
                || !thing_manager.is_persisted_in_storage(snapshot, object.vertex(), storage_counters.clone())?
            {
                return Ok(());
            }
            let check = Self::$validate_func_name(snapshot, thing_manager, object, &constraints, storage_counters);
            collect_errors!(out_errors, check, |e: Box<_>| *e);
            Ok(())
        }

        fn $collect_func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object_type: <$capability_type as Capability>::ObjectType,
            interface_types: &HashSet<<$capability_type as Capability>::InterfaceType>,
        ) -> Result<Vec<CheckedCardinality<$capability_type>>, Box<ConceptReadError>> {
            let mut cardinality_constraints: HashSet<CapabilityConstraint<$capability_type>> = HashSet::new();
            for interface_type in interface_types {
                for constraint in object_type
                    .$get_cardinality_constraints_func(snapshot, thing_manager.type_manager(), interface_type.clone())?
                    .into_iter()
                {
                    cardinality_constraints.insert(constraint);
                }
            }

            let mut checked = Vec::new();
            for constraint in cardinality_constraints {
                if !constraint.requires_validation() {
                    continue;
                }
                let source_interface_type = constraint.source().interface();
                let sub_interface_types =
                    source_interface_type.get_subtypes_transitive(snapshot, thing_manager.type_manager())?;
                let counted_interface_types =
                    TypeAPI::chain_types(source_interface_type.clone(), sub_interface_types.into_iter().cloned())
                        .collect();
                checked.push(CheckedCardinality { constraint, counted_interface_types });
            }
            Ok(checked)
        }

        fn $validate_func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            checked_cardinalities: &[CheckedCardinality<$capability_type>],
            storage_counters: StorageCounters,
        ) -> Result<(), Box<DataValidationError>> {
            if checked_cardinalities.is_empty() {
                return Ok(());
            }

            let counts = object
                .$get_interface_counts_func(snapshot, thing_manager, storage_counters)
                .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
            for cardinality in checked_cardinalities {
                let count = cardinality
                    .counted_interface_types
                    .iter()
                    .filter_map(|interface_type| counts.get(interface_type))
                    .sum();
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

struct CheckedCardinality<CAP: Capability> {
    constraint: CapabilityConstraint<CAP>,
    counted_interface_types: Vec<CAP::InterfaceType>,
}

/*
The cardinalities validation flow is the following:
1. Find instances affected by cardinalities changes (separately for 3 capabilities: owns, plays, relates): instance writes are visited straight from the write buffer, grouped by its key order; a capability cardinality change is recorded per type and every instance of the type is visited.
2. Validate only the affected instances to avoid rescanning the whole system (see capability_cardinality_validation). For each object,
  2a. Count every capability instance it has (every has, every played role, every roleplayer).
  2b. Collect cardinality constraints (declared and inherited) of all marked capabilities without duplications (if a subtype and its supertype are affected, the supertype's constraint is checked once).
  2c. Validate each constraint separately using the counts prepared in 2a. To validate a constraint, take its source type (where this constraint is declared), and count all instances of the source type and its subtypes.

Let's consider the following example:
  entity person,
    owns name @card(1..),
    owns surname, # sub name
    owns changed-surname @card(1..2); # sub surname

A query is being run:
  define person owns surname @card(1..10);

It will be processed like:
1. person is recorded with surname as the only modified attribute type, and every instance of person is visited.
2. For each instance of persons:
  2a. All names, surnames, and changed-surnames are counted (based on instances' explicit types).
  2b. surname's constraints will be taken: @card(1..) from name and @card(1..10) from surname.
  2c. For each constraint:
    @card(1..): combines counts of names, surnames, and changed-surnames. If it's at least 1, it's good.
    @card(1..10): combines counts of surnames and changed-surnames (without names). If it's from 1 to 10, it's good.

This way, the validation on step 2 always goes up the sub hierarchy to collect current constraints, and then goes down the hierarchy to consider all the suitable instances.

HOWEVER, it won't work if stage 1 is incomplete. For example:
  undefine owns surname from person;

If we mark only surnames as affected attribute types, we will get 0 constraints on the validation stage (as person does not have any constraints for it anymore, it does not own it).
Thus, we will not check the cardinality of names, although it might be violated as it does not now count surnames!
We could potentially use the old version of storage (ignoring the snapshot), but it would make the reasoning even more complicated.
Please keep these complexities in mind when modifying the collection stage in the following methods.
*/

pub(crate) struct CardinalityValidation {}

impl CardinalityValidation {
    pub(crate) fn validate_commit<Snapshot: WritableSnapshot + Sync>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_new_objects(snapshot, thing_manager, out_errors, storage_counters.clone())?;
        Self::validate_existing_owners_of_modified_has(snapshot, thing_manager, out_errors, storage_counters.clone())?;
        Self::validate_existing_players_of_modified_links(
            snapshot,
            thing_manager,
            out_errors,
            storage_counters.clone(),
        )?;
        Self::validate_existing_relations_of_modified_links(
            snapshot,
            thing_manager,
            out_errors,
            storage_counters.clone(),
        )?;

        // at this point, we've validated the delta, and only need to revalidate untouched/persisted data according to the type modifications
        // TODO validate this thought
        if !Snapshot::IMMUTABLE_SCHEMA {
            Self::validate_instances_of_modified_cardinalities(snapshot, thing_manager, out_errors, storage_counters)?;
        }
        Ok(())
    }

    fn validate_new_objects<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();
        let mut owns_constraints_by_type = HashMap::new();
        let mut plays_constraints_by_type = HashMap::new();
        let mut relates_constraints_by_type = HashMap::new();
        thing_manager.for_each_new_object(snapshot, |snapshot, object| {
            let object_type = object.type_();
            let owns_constraints = get_or_try_insert_with(&mut owns_constraints_by_type, object_type, || {
                let attribute_types =
                    object_type.get_owns(snapshot, type_manager)?.into_iter().map(|owns| owns.attribute()).collect();
                Self::collect_checked_owns_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    object_type,
                    &attribute_types,
                )
            })?;
            let owns_check = CardinalityValidation::validate_owns_cardinality_constraints(
                snapshot,
                thing_manager,
                object,
                owns_constraints,
                storage_counters.clone(),
            );
            collect_errors!(out_errors, owns_check, |e: Box<_>| *e);

            let plays_constraints = get_or_try_insert_with(&mut plays_constraints_by_type, object_type, || {
                let role_types =
                    object_type.get_plays(snapshot, type_manager)?.into_iter().map(|plays| plays.role()).collect();
                Self::collect_checked_plays_cardinality_constraints(snapshot, thing_manager, object_type, &role_types)
            })?;
            let plays_check = CardinalityValidation::validate_plays_cardinality_constraints(
                snapshot,
                thing_manager,
                object,
                plays_constraints,
                storage_counters.clone(),
            );
            collect_errors!(out_errors, plays_check, |e: Box<_>| *e);

            if let Object::Relation(relation) = object {
                let relation_type = relation.type_();
                let relates_constraints =
                    get_or_try_insert_with(&mut relates_constraints_by_type, relation_type, || {
                        let role_types = relation_type
                            .get_relates(snapshot, type_manager)?
                            .into_iter()
                            .map(|relates| relates.role())
                            .collect();
                        Self::collect_checked_relates_cardinality_constraints(
                            snapshot,
                            thing_manager,
                            relation_type,
                            &role_types,
                        )
                    })?;
                let relates_check = CardinalityValidation::validate_relates_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    relation,
                    relates_constraints,
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
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_owner_with_modified_has(snapshot, |snapshot, modified| {
            Self::validate_existing_object_has(
                snapshot,
                thing_manager,
                modified.owner,
                &modified.modified_attribute_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_existing_players_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_player_with_modified_links(snapshot, |snapshot, modified| {
            Self::validate_existing_object_links(
                snapshot,
                thing_manager,
                modified.player,
                &modified.modified_role_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_existing_relations_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_relation_with_modified_links(snapshot, |snapshot, modified| {
            Self::validate_existing_relation_links(
                snapshot,
                thing_manager,
                modified.relation,
                &modified.modified_role_types,
                out_errors,
                storage_counters.clone(),
            )
        })
    }

    fn validate_instances_of_modified_cardinalities(
        snapshot: &(impl WritableSnapshot + Sync),
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();

        let preexisting_cardinalities = SchemaCardinalities::new(
            &snapshot.read_snapshot_before_writes(),
            &TypeManager::new(type_manager.definition_key_generator(), type_manager.vertex_generator(), None),
        )?;
        let modified_cardinalities =
            SchemaCardinalities::new(snapshot, type_manager)?.diff_from(preexisting_cardinalities);

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

        // types are validated independently, so each worker keeps taking the next type until none are left
        let next_task = AtomicUsize::new(0);
        let validate_tasks = || -> Result<Vec<DataValidationError>, Box<ConceptReadError>> {
            let mut errors = Vec::new();
            while let Some(task) = tasks.get(next_task.fetch_add(1, atomic::Ordering::Relaxed)) {
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
            }
            Ok(errors)
        };

        let worker_count = SystemInfo::get_cpu_count().min(tasks.len());
        let results: Vec<_> = thread::scope(|scope| {
            let workers: Vec<_> = (0..worker_count).map(|_| scope.spawn(&validate_tasks)).collect();
            workers.into_iter().map(|worker| worker.join().unwrap_or_else(|err| panic::resume_unwind(err))).collect()
        });
        for result in results {
            out_errors.extend(result?);
        }

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
        Instance: ThingAPI + Copy + PartialEq + 'static,
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
            for &interface_type in &description.relevant_types {
                counted_by.entry(interface_type).or_default().push(index);
            }
        }

        // instances and their edges are both ordered by instance, so walk them together to also visit instances without any edge
        let mut edges = get_edges_in(type_).peekable();
        for instance in get_instances_in(type_) {
            let instance = instance?;
            while edges.peek().is_some_and(|result| {
                result.as_ref().map_or(true, |(edge, _)| edge_instance_and_interface(edge).0 == instance)
            }) {
                let (edge, count) = edges.next().unwrap()?;
                if let Some(indices) = counted_by.get(&edge_instance_and_interface(&edge).1) {
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
        collect_checked_owns_cardinality_constraints,
        validate_owns_cardinality_constraints,
        validate_existing_object_has,
        Owns,
        Object,
        get_owned_attribute_type_constraints_cardinality,
        get_has_counts,
        DataValidation::validate_owns_instances_cardinality_constraint
    );
    capability_cardinality_validation!(
        collect_checked_plays_cardinality_constraints,
        validate_plays_cardinality_constraints,
        validate_existing_object_links,
        Plays,
        Object,
        get_played_role_type_constraints_cardinality,
        get_played_roles_counts,
        DataValidation::validate_plays_instances_cardinality_constraint
    );
    capability_cardinality_validation!(
        collect_checked_relates_cardinality_constraints,
        validate_relates_cardinality_constraints,
        validate_existing_relation_links,
        Relates,
        Relation,
        get_related_role_type_constraints_cardinality,
        get_player_counts,
        DataValidation::validate_relates_instances_cardinality_constraint
    );
}

enum CardinalityTask<'a> {
    Owns(ObjectType, &'a [CardinalityDescription<Owns>]),
    Plays(ObjectType, &'a [CardinalityDescription<Plays>]),
    Relates(RelationType, &'a [CardinalityDescription<Relates>]),
}

pub(crate) struct SchemaCardinalities {
    owns: HashMap<ObjectType, Vec<CardinalityDescription<Owns>>>,
    plays: HashMap<ObjectType, Vec<CardinalityDescription<Plays>>>,
    pub relates: HashMap<RelationType, Vec<CardinalityDescription<Relates>>>,
}

pub(crate) struct CardinalityDescription<CAP: Capability> {
    constraint: CapabilityConstraint<CAP>,
    cardinality: AnnotationCardinality,
    relevant_types: HashSet<CAP::InterfaceType>,
}

impl SchemaCardinalities {
    pub(crate) fn new(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        let mut owns = HashMap::new();
        let mut plays = HashMap::new();
        for object_type in type_manager.get_object_types(snapshot)? {
            let mut owns_cardinalities = HashSet::new();
            for attribute_type in object_type.get_owned_attribute_types(snapshot, type_manager)? {
                owns_cardinalities.extend(object_type.get_owned_attribute_type_constraints_cardinality(
                    snapshot,
                    type_manager,
                    attribute_type,
                )?);
            }
            owns.insert(object_type, Self::describe_cardinalities(snapshot, type_manager, owns_cardinalities)?);

            let mut plays_cardinalities = HashSet::new();
            for role_type in object_type.get_played_role_types(snapshot, type_manager)? {
                plays_cardinalities.extend(object_type.get_played_role_type_constraints_cardinality(
                    snapshot,
                    type_manager,
                    role_type,
                )?);
            }
            plays.insert(object_type, Self::describe_cardinalities(snapshot, type_manager, plays_cardinalities)?);
        }

        let mut relates = HashMap::new();
        for relation_type in type_manager.get_relation_types(snapshot)? {
            let mut relates_cardinalities = HashSet::new();
            for role_type in relation_type.get_related_role_types(snapshot, type_manager)? {
                relates_cardinalities.extend(relation_type.get_related_role_type_constraints_cardinality(
                    snapshot,
                    type_manager,
                    role_type,
                )?);
            }
            relates.insert(relation_type, Self::describe_cardinalities(snapshot, type_manager, relates_cardinalities)?);
        }

        Ok(Self { owns, plays, relates })
    }

    fn describe_cardinalities<CAP: Capability>(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        constraints: HashSet<CapabilityConstraint<CAP>>,
    ) -> Result<Vec<CardinalityDescription<CAP>>, Box<ConceptReadError>> {
        let mut cardinalities = Vec::new();
        for constraint in constraints {
            if !constraint.requires_validation() {
                continue;
            }
            let cardinality = constraint.description().unwrap_cardinality().expect("Only Cardinality constraints");
            let source_interface_type = constraint.source().interface();
            let sub_interface_types = source_interface_type.get_subtypes_transitive(snapshot, type_manager)?;
            let relevant_types =
                TypeAPI::chain_types(source_interface_type, sub_interface_types.into_iter().cloned()).collect();
            cardinalities.push(CardinalityDescription { constraint, cardinality, relevant_types });
        }
        Ok(cardinalities)
    }

    // Keep only the cardinalities that instances satisfying the previous state might now violate
    pub(crate) fn diff_from(mut self, previous: SchemaCardinalities) -> Self {
        Self::retain_cardinalities_to_revalidate(&mut self.owns, previous.owns);
        Self::retain_cardinalities_to_revalidate(&mut self.plays, previous.plays);
        Self::retain_cardinalities_to_revalidate(&mut self.relates, previous.relates);
        self
    }

    fn retain_cardinalities_to_revalidate<CAP: Capability>(
        current: &mut HashMap<CAP::ObjectType, Vec<CardinalityDescription<CAP>>>,
        mut previous: HashMap<CAP::ObjectType, Vec<CardinalityDescription<CAP>>>,
    ) {
        current.retain(|object_type, cardinality_descriptions| {
            let previous_by_source: HashMap<CAP, CardinalityDescription<CAP>> = previous
                .remove(object_type)
                .unwrap_or_default()
                .into_iter()
                .map(|cardinality| (cardinality.constraint.source(), cardinality))
                .collect();
            cardinality_descriptions.retain(|description| {
                let Some(previous) = previous_by_source.get(&description.constraint.source()) else {
                    return true;
                };
                // if the previous state was not strictl contained within the current one, we have to recheck the card
                if !description.cardinality.narrowed_correctly_by(&previous.cardinality) {
                    return true;
                }
                // counting more instances can only break an upper bound, and counting fewer only a lower bound
                let counts_more = description.relevant_types.difference(&previous.relevant_types).next().is_some();
                let counts_fewer = previous.relevant_types.difference(&description.relevant_types).next().is_some();
                (counts_more && description.cardinality.end().is_some())
                    || (counts_fewer && description.cardinality.start() > 0)
            });
            !cardinality_descriptions.is_empty()
        });
    }
}

fn get_or_try_insert_with<K: Eq + Hash, V, E>(
    map: &mut HashMap<K, V>,
    key: K,
    value_producer: impl FnOnce() -> Result<V, E>,
) -> Result<&V, E> {
    match map.entry(key) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(value_producer()?)),
    }
}

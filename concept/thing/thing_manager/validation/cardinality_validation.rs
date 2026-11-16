/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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
        type_manager::schema_cardinalities::{CardinalityDescription, SchemaCardinalities, TypeCardinalities},
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
        Self::validate_new_owners(snapshot, thing_manager, &cardinalities, out_errors)?;
        Self::validate_new_players(snapshot, thing_manager, &cardinalities, out_errors)?;
        Self::validate_new_relations(snapshot, thing_manager, &cardinalities, out_errors)?;
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

    fn validate_new_owners(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_new_instances(
            out_errors,
            thing_manager.buffered_new_objects(snapshot),
            thing_manager.buffered_has(snapshot),
            |owner: Object| cardinalities.owns.get(&owner.type_()),
            |constraint, owner, attribute_type, count| {
                DataValidation::validate_owns_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    owner,
                    attribute_type,
                    count,
                )
            },
        )
    }

    fn validate_new_players(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_new_instances(
            out_errors,
            thing_manager.buffered_new_objects(snapshot),
            thing_manager.buffered_links_reverse(snapshot),
            |player: Object| cardinalities.plays.get(&player.type_()),
            |constraint, player, role_type, count| {
                DataValidation::validate_plays_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    player,
                    role_type,
                    count,
                )
            },
        )
    }

    fn validate_new_relations(
        snapshot: &impl WritableSnapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
    ) -> Result<(), Box<ConceptReadError>> {
        let new_relations = thing_manager.buffered_new_objects(snapshot).filter_map(|object| match object {
            Object::Relation(relation) => Some(relation),
            Object::Entity(_) => None,
        });
        Self::validate_new_instances(
            out_errors,
            new_relations,
            thing_manager.buffered_links(snapshot),
            |relation: Relation| cardinalities.relates.get(&relation.type_()),
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

    // every edge of a new instance is buffered, so it is counted from the buffer alone, against all its cardinalities
    fn validate_new_instances<'c, CAP: Capability + 'c, Instance: Copy + Ord>(
        out_errors: &mut Vec<DataValidationError>,
        new_instances: impl Iterator<Item = Instance>,
        buffered_edges: impl Iterator<Item = (Instance, CAP::InterfaceType, u64)>,
        cardinalities_of: impl Fn(Instance) -> Option<&'c TypeCardinalities<CAP>>,
        validate_cardinality: impl Fn(
            &CapabilityConstraint<CAP>,
            Instance,
            CAP::InterfaceType,
            u64,
        ) -> Result<(), Box<DataValidationError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut counter_storage = Vec::new();
        let mut walk =
            InstanceEdgeWalk::new(new_instances.map(Ok), buffered_edges.map(Ok), |&(instance, _, _)| instance);
        while let Some(instance) = walk.next_instance().transpose()? {
            let Some(cardinalities) = cardinalities_of(instance) else {
                continue;
            };
            let mut counters = cardinalities.counters(&mut counter_storage);
            while let Some((_, interface_type, count)) = walk.next_edge().transpose()? {
                counters.count(interface_type, count);
            }
            for (description, count) in counters.iter() {
                let constraint = &description.constraint;
                let check = validate_cardinality(constraint, instance, constraint.source().interface(), count);
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }
        }
        Ok(())
    }

    fn validate_existing_owners_of_modified_has<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut counter_storage = Vec::new();
        thing_manager.for_each_owner_with_modified_has(snapshot, |snapshot, modified| {
            let Some(owns_cardinalities) = cardinalities.owns.get(&modified.owner.type_()) else {
                return Ok(());
            };
            let modified_types = modified.modified_attribute_types;
            if Self::is_affected_persisted_instance(
                snapshot,
                thing_manager,
                modified.owner,
                owns_cardinalities,
                modified_types,
                storage_counters.clone(),
            )? {
                let check = Self::validate_owns_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.owner,
                    owns_cardinalities,
                    |description| description.counts_any(modified_types),
                    &mut counter_storage,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }
            Ok(())
        })
    }

    fn validate_existing_players_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut counter_storage = Vec::new();
        thing_manager.for_each_player_with_modified_links(snapshot, |snapshot, modified| {
            let Some(plays_cardinalities) = cardinalities.plays.get(&modified.player.type_()) else {
                return Ok(());
            };
            let modified_types = modified.modified_role_types;
            if Self::is_affected_persisted_instance(
                snapshot,
                thing_manager,
                modified.player,
                plays_cardinalities,
                modified_types,
                storage_counters.clone(),
            )? {
                let check = Self::validate_plays_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.player,
                    plays_cardinalities,
                    |description| description.counts_any(modified_types),
                    &mut counter_storage,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }
            Ok(())
        })
    }

    fn validate_existing_relations_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        cardinalities: &SchemaCardinalities,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut counter_storage = Vec::new();
        thing_manager.for_each_relation_with_modified_links(snapshot, |snapshot, modified| {
            let Some(relates_cardinalities) = cardinalities.relates.get(&modified.relation.type_()) else {
                return Ok(());
            };
            let modified_types = &modified.modified_role_types;
            if Self::is_affected_persisted_instance(
                snapshot,
                thing_manager,
                modified.relation,
                relates_cardinalities,
                modified_types,
                storage_counters.clone(),
            )? {
                let check = Self::validate_relates_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.relation,
                    relates_cardinalities,
                    |description| description.counts_any(modified_types),
                    &mut counter_storage,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }
            Ok(())
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
        cardinalities: &TypeCardinalities<Owns>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            object_type,
            cardinalities,
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
        cardinalities: &TypeCardinalities<Plays>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            object_type,
            cardinalities,
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
        cardinalities: &TypeCardinalities<Relates>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        Self::validate_instances_cardinalities(
            snapshot,
            thing_manager,
            out_errors,
            relation_type,
            cardinalities,
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
        cardinalities: &TypeCardinalities<CAP>,
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
        let mut counter_storage = Vec::new();
        let mut counters = cardinalities.counters(&mut counter_storage);

        let mut walk = InstanceEdgeWalk::new(get_instances_in(type_), get_edges_in(type_), |(edge, _)| {
            edge_instance_and_interface(edge).0
        });
        while let Some(instance) = walk.next_instance().transpose()? {
            while let Some((edge, count)) = walk.next_edge().transpose()? {
                let (_, interface_type) = edge_instance_and_interface(&edge);
                counters.count(interface_type, count);
            }

            // instances written in this transaction are fully validated by the previous delta checks
            let key = instance.vertex().into_storage_key();
            if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_none() {
                for (description, count) in counters.iter() {
                    let check = validate_cardinality(
                        &description.constraint,
                        instance,
                        description.constraint.source().interface(),
                        count,
                    );
                    collect_errors!(out_errors, check, |e: Box<_>| *e);
                }
            }
            counters.reset();
        }
        Ok(())
    }

    fn validate_owns_cardinality_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: Object,
        cardinalities: &TypeCardinalities<Owns>,
        is_validated: impl Fn(&CardinalityDescription<Owns>) -> bool,
        counter_storage: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            is_validated,
            counter_storage,
            || {
                let has = owner.get_has_unordered(snapshot, thing_manager, storage_counters)?;
                Ok(has.map(|has| has.map(|(has, count)| (has.attribute().type_(), count))))
            },
            |constraint, attribute_type, count| {
                DataValidation::validate_owns_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    owner,
                    attribute_type,
                    count,
                )
            },
        )
    }

    fn validate_plays_cardinality_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        player: Object,
        cardinalities: &TypeCardinalities<Plays>,
        is_validated: impl Fn(&CardinalityDescription<Plays>) -> bool,
        counter_storage: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            is_validated,
            counter_storage,
            || {
                let relations_roles = player.get_relations_roles(snapshot, thing_manager, storage_counters);
                Ok(relations_roles.map(|relation_role| relation_role.map(|(_, role_type, count)| (role_type, count))))
            },
            |constraint, role_type, count| {
                DataValidation::validate_plays_instances_cardinality_constraint(
                    snapshot,
                    thing_manager.type_manager(),
                    constraint,
                    player,
                    role_type,
                    count,
                )
            },
        )
    }

    fn validate_relates_cardinality_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        cardinalities: &TypeCardinalities<Relates>,
        is_validated: impl Fn(&CardinalityDescription<Relates>) -> bool,
        counter_storage: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            is_validated,
            counter_storage,
            || {
                let players = relation.get_players(snapshot, thing_manager, storage_counters);
                Ok(players.map(|player| player.map(|(role_player, count)| (role_player.role_type(), count))))
            },
            |constraint, role_type, count| {
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

    // counts the instance's edges into one counter per cardinality, reusing the counter storage across instances
    fn validate_cardinalities<
        CAP: Capability,
        EdgeIterator: Iterator<Item = Result<(CAP::InterfaceType, u64), Box<ConceptReadError>>>,
    >(
        cardinalities: &TypeCardinalities<CAP>,
        is_validated: impl Fn(&CardinalityDescription<CAP>) -> bool,
        counter_storage: &mut Vec<u64>,
        get_edges: impl FnOnce() -> Result<EdgeIterator, Box<ConceptReadError>>,
        validate_cardinality: impl Fn(
            &CapabilityConstraint<CAP>,
            CAP::InterfaceType,
            u64,
        ) -> Result<(), Box<DataValidationError>>,
    ) -> Result<(), Box<DataValidationError>> {
        let to_validation_error =
            |source: Box<ConceptReadError>| Box::new(DataValidationError::ConceptRead { typedb_source: source });
        let mut counters = cardinalities.counters(counter_storage);
        for edge in get_edges().map_err(to_validation_error)? {
            let (interface_type, count) = edge.map_err(to_validation_error)?;
            counters.count(interface_type, count);
        }
        for (description, count) in counters.iter().filter(|(description, _)| is_validated(description)) {
            let constraint = &description.constraint;
            validate_cardinality(constraint, constraint.source().interface(), count)?;
        }
        Ok(())
    }

    // new instances are validated against all their cardinalities, and persisted ones only against those counting a
    // modified interface type
    fn is_affected_persisted_instance<'a, CAP: Capability>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        instance: impl ThingAPI,
        cardinalities: &TypeCardinalities<CAP>,
        modified_types: impl IntoIterator<Item = &'a CAP::InterfaceType>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>>
    where
        CAP::InterfaceType: 'a,
    {
        let key = instance.vertex().into_storage_key();
        if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_some()
            || !cardinalities.counts_any(modified_types)
        {
            return Ok(false);
        }
        thing_manager.is_persisted_in_storage(snapshot, instance.vertex(), storage_counters)
    }
}

enum CardinalityTask<'a> {
    Owns(ObjectType, &'a TypeCardinalities<Owns>),
    Plays(ObjectType, &'a TypeCardinalities<Plays>),
    Relates(RelationType, &'a TypeCardinalities<Relates>),
}

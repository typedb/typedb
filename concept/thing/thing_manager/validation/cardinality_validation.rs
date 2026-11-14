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
        type_manager::schema_cardinalities::{SchemaCardinalities, TypeCardinalities},
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
        let mut counts = Vec::new();
        thing_manager.for_each_new_object(snapshot, |snapshot, object| {
            if let Some(owns_cardinalities) = cardinalities.owns.get(&object.type_()) {
                let check = Self::validate_owns_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    object,
                    owns_cardinalities,
                    0..owns_cardinalities.cardinalities.len(),
                    &mut counts,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }

            if let Some(plays_cardinalities) = cardinalities.plays.get(&object.type_()) {
                let check = Self::validate_plays_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    object,
                    plays_cardinalities,
                    0..plays_cardinalities.cardinalities.len(),
                    &mut counts,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
            }

            if let Object::Relation(relation) = object
                && let Some(relates_cardinalities) = cardinalities.relates.get(&relation.type_())
            {
                let check = Self::validate_relates_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    relation,
                    relates_cardinalities,
                    0..relates_cardinalities.cardinalities.len(),
                    &mut counts,
                    storage_counters.clone(),
                );
                collect_errors!(out_errors, check, |e: Box<_>| *e);
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
        let mut counts = Vec::new();
        thing_manager.for_each_owner_with_modified_has(snapshot, |snapshot, modified| {
            let Some(owns_cardinalities) = cardinalities.owns.get(&modified.owner.type_()) else {
                return Ok(());
            };
            let affected_cardinalities = Self::get_affected_cardinalities(
                snapshot,
                thing_manager,
                modified.owner,
                owns_cardinalities,
                modified.modified_attribute_types,
                storage_counters.clone(),
            )?;
            if let Some(affected_cardinalities) = affected_cardinalities {
                let check = Self::validate_owns_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.owner,
                    owns_cardinalities,
                    affected_cardinalities,
                    &mut counts,
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
        let mut counts = Vec::new();
        thing_manager.for_each_player_with_modified_links(snapshot, |snapshot, modified| {
            let Some(plays_cardinalities) = cardinalities.plays.get(&modified.player.type_()) else {
                return Ok(());
            };
            let affected_cardinalities = Self::get_affected_cardinalities(
                snapshot,
                thing_manager,
                modified.player,
                plays_cardinalities,
                modified.modified_role_types,
                storage_counters.clone(),
            )?;
            if let Some(affected_cardinalities) = affected_cardinalities {
                let check = Self::validate_plays_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.player,
                    plays_cardinalities,
                    affected_cardinalities,
                    &mut counts,
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
        let mut counts = Vec::new();
        thing_manager.for_each_relation_with_modified_links(snapshot, |snapshot, modified| {
            let Some(relates_cardinalities) = cardinalities.relates.get(&modified.relation.type_()) else {
                return Ok(());
            };
            let affected_cardinalities = Self::get_affected_cardinalities(
                snapshot,
                thing_manager,
                modified.relation,
                relates_cardinalities,
                &modified.modified_role_types,
                storage_counters.clone(),
            )?;
            if let Some(affected_cardinalities) = affected_cardinalities {
                let check = Self::validate_relates_cardinality_constraints(
                    snapshot,
                    thing_manager,
                    modified.relation,
                    relates_cardinalities,
                    affected_cardinalities,
                    &mut counts,
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
        let mut counts = vec![0u64; cardinalities.cardinalities.len()];

        let mut walk = InstanceEdgeWalk::new(get_instances_in(type_), get_edges_in(type_), |(edge, _)| {
            edge_instance_and_interface(edge).0
        });
        while let Some(instance) = walk.next_instance().transpose()? {
            while let Some((edge, count)) = walk.next_edge().transpose()? {
                let (_, interface_type) = edge_instance_and_interface(&edge);
                cardinalities.count(&mut counts, interface_type, count);
            }

            // instances written in this transaction are fully validated by the previous delta checks
            let key = instance.vertex().into_storage_key();
            if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_none() {
                for (description, &count) in cardinalities.cardinalities.iter().zip(&counts) {
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

    fn validate_owns_cardinality_constraints(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        owner: Object,
        cardinalities: &TypeCardinalities<Owns>,
        validated: impl Iterator<Item = usize>,
        counts: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            validated,
            counts,
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
        validated: impl Iterator<Item = usize>,
        counts: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            validated,
            counts,
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
        validated: impl Iterator<Item = usize>,
        counts: &mut Vec<u64>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<DataValidationError>> {
        Self::validate_cardinalities(
            cardinalities,
            validated,
            counts,
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

    // counts the instance's edges into one counter per cardinality, reusing the counters across instances
    fn validate_cardinalities<
        CAP: Capability,
        EdgeIterator: Iterator<Item = Result<(CAP::InterfaceType, u64), Box<ConceptReadError>>>,
    >(
        cardinalities: &TypeCardinalities<CAP>,
        validated: impl Iterator<Item = usize>,
        counts: &mut Vec<u64>,
        get_edges: impl FnOnce() -> Result<EdgeIterator, Box<ConceptReadError>>,
        validate_cardinality: impl Fn(
            &CapabilityConstraint<CAP>,
            CAP::InterfaceType,
            u64,
        ) -> Result<(), Box<DataValidationError>>,
    ) -> Result<(), Box<DataValidationError>> {
        let to_validation_error =
            |source: Box<ConceptReadError>| Box::new(DataValidationError::ConceptRead { typedb_source: source });
        counts.clear();
        counts.resize(cardinalities.cardinalities.len(), 0);
        for edge in get_edges().map_err(to_validation_error)? {
            let (interface_type, count) = edge.map_err(to_validation_error)?;
            cardinalities.count(counts, interface_type, count);
        }
        for index in validated {
            let constraint = &cardinalities.cardinalities[index].constraint;
            validate_cardinality(constraint, constraint.source().interface(), counts[index])?;
        }
        Ok(())
    }

    // new instances are validated against all their cardinalities, and persisted ones only against those counting a
    // modified interface type
    fn get_affected_cardinalities<'a, CAP: Capability>(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        instance: impl ThingAPI,
        cardinalities: &'a TypeCardinalities<CAP>,
        interface_types: impl IntoIterator<Item = &'a CAP::InterfaceType> + Copy + 'a,
        storage_counters: StorageCounters,
    ) -> Result<Option<impl Iterator<Item = usize> + 'a>, Box<ConceptReadError>> {
        let key = instance.vertex().into_storage_key();
        if thing_manager.get_buffered_status(snapshot, key.as_reference()).is_some() {
            return Ok(None);
        }
        let mut affected_cardinalities = (0..cardinalities.cardinalities.len())
            .filter(move |&index| {
                let relevant_types = &cardinalities.cardinalities[index].relevant_types;
                interface_types.into_iter().any(|interface_type| relevant_types.contains(interface_type))
            })
            .peekable();
        if affected_cardinalities.peek().is_none()
            || !thing_manager.is_persisted_in_storage(snapshot, instance.vertex(), storage_counters)?
        {
            return Ok(None);
        }
        Ok(Some(affected_cardinalities))
    }
}

enum CardinalityTask<'a> {
    Owns(ObjectType, &'a TypeCardinalities<Owns>),
    Plays(ObjectType, &'a TypeCardinalities<Plays>),
    Relates(RelationType, &'a TypeCardinalities<Relates>),
}

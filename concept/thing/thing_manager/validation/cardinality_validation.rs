/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    thing::{
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{
            ThingManager,
            validation::{DataValidationError, validation::DataValidation},
        },
    },
    type_::{
        Capability, OwnerAPI, PlayerAPI, TypeAPI,
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint},
        owns::Owns,
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
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

use encoding::{
    Prefixed,
    graph::type_::{edge::TypeEdge, property::TypeEdgeProperty},
    layout::{infix::Infix, prefix::Prefix},
};
use storage::{key_range::KeyRange, snapshot::write::Write};

use crate::{
    ConceptStatus,
    type_::{object_type::ObjectType, relation_type::RelationType, type_manager::TypeManager},
};

macro_rules! validate_capability_cardinality_constraint {
    ($func_name:ident, $capability_type:ident, $object_instance:ident, $get_cardinality_constraints_func:ident, $get_interface_counts_func:ident, $check_func:path) => {
        pub(crate) fn $func_name(
            snapshot: &impl ReadableSnapshot,
            thing_manager: &ThingManager,
            object: $object_instance,
            interface_types_to_check: &HashSet<<$capability_type as Capability>::InterfaceType>,
            storage_counters: StorageCounters,
        ) -> Result<(), Box<DataValidationError>> {
            let mut cardinality_constraints: HashSet<CapabilityConstraint<$capability_type>> = HashSet::new();

            let counts = std::cell::LazyCell::new(|| {
                object
                    .$get_interface_counts_func(snapshot, thing_manager, storage_counters)
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))
            });

            for interface_type in interface_types_to_check {
                for constraint in object
                    .type_()
                    .$get_cardinality_constraints_func(snapshot, thing_manager.type_manager(), interface_type.clone())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                    .into_iter()
                {
                    cardinality_constraints.insert(constraint);
                }
            }

            for constraint in cardinality_constraints {
                if !constraint
                    .description()
                    .unwrap_cardinality()
                    .map_err(|source| Box::new(ConceptReadError::Constraint { typedb_source: source }))
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?
                    .requires_validation()
                {
                    continue;
                }

                let Ok(counts) = &*counts else { return Err(counts.clone().unwrap_err()) };
                let source_interface_type = constraint.source().interface();
                let sub_interface_types = source_interface_type
                    .get_subtypes_transitive(snapshot, thing_manager.type_manager())
                    .map_err(|source| Box::new(DataValidationError::ConceptRead { typedb_source: source }))?;
                let count =
                    TypeAPI::chain_types(source_interface_type.clone(), sub_interface_types.into_iter().cloned())
                        .filter_map(|interface_type| counts.get(&interface_type))
                        .sum();
                $check_func(snapshot, thing_manager.type_manager(), &constraint, object, source_interface_type, count)?;
            }

            Ok(())
        }
    };
}

/*
The cardinalities validation flow is the following:
1. Find instances affected by cardinalities changes (separately for 3 capabilities: owns, plays, relates): instance writes are visited straight from the write buffer, grouped by its key order; a capability cardinality change is recorded per type and every instance of the type is visited.
2. Validate only the affected instances to avoid rescanning the whole system (see validate_capability_cardinality_constraint). For each object,
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
    pub(crate) fn validate_commit<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        modified_types: &ModifiedCapabilityTypes,
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
        Self::validate_instances_of_modified_types(
            snapshot,
            thing_manager,
            modified_types,
            out_errors,
            storage_counters,
        )
    }

    fn validate_new_objects<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();
        thing_manager.for_each_new_object(snapshot, |snapshot, object| {
            let attribute_types =
                object.type_().get_owns(snapshot, type_manager)?.into_iter().map(|owns| owns.attribute()).collect();
            CardinalityValidation::validate_object_has(
                snapshot,
                thing_manager,
                object,
                &attribute_types,
                out_errors,
                storage_counters.clone(),
            )?;
            let role_types =
                object.type_().get_plays(snapshot, type_manager)?.into_iter().map(|plays| plays.role()).collect();
            CardinalityValidation::validate_object_links(
                snapshot,
                thing_manager,
                object,
                &role_types,
                out_errors,
                storage_counters.clone(),
            )?;
            if let Object::Relation(relation) = object {
                let role_types = relation
                    .type_()
                    .get_relates(snapshot, type_manager)?
                    .into_iter()
                    .map(|relates| relates.role())
                    .collect();
                CardinalityValidation::validate_relation_links(
                    snapshot,
                    thing_manager,
                    relation,
                    &role_types,
                    out_errors,
                    storage_counters.clone(),
                )?;
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
        thing_manager.for_each_owner_with_modified_has(
            snapshot,
            storage_counters.clone(),
            |error| error,
            |snapshot, modified| {
                if modified.status == ConceptStatus::Persisted {
                    CardinalityValidation::validate_object_has(
                        snapshot,
                        thing_manager,
                        modified.owner,
                        &modified.modified_attribute_types,
                        out_errors,
                        storage_counters.clone(),
                    )?;
                }
                Ok(())
            },
        )
    }

    fn validate_existing_players_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_player_with_modified_links(
            snapshot,
            storage_counters.clone(),
            |error| error,
            |snapshot, modified| {
                if modified.status == ConceptStatus::Persisted {
                    CardinalityValidation::validate_object_links(
                        snapshot,
                        thing_manager,
                        modified.player,
                        &modified.modified_role_types,
                        out_errors,
                        storage_counters.clone(),
                    )?;
                }
                Ok(())
            },
        )
    }

    fn validate_existing_relations_of_modified_links<Snapshot: ReadableSnapshot>(
        snapshot: &mut Snapshot,
        thing_manager: &ThingManager,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        thing_manager.for_each_relation_with_modified_links(
            snapshot,
            storage_counters.clone(),
            |error| error,
            |snapshot, modified| {
                if modified.status == ConceptStatus::Persisted {
                    CardinalityValidation::validate_relation_links(
                        snapshot,
                        thing_manager,
                        modified.relation,
                        &modified.modified_role_types,
                        out_errors,
                        storage_counters.clone(),
                    )?;
                }
                Ok(())
            },
        )
    }

    fn validate_instances_of_modified_types(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        modified_types: &ModifiedCapabilityTypes,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let type_manager = thing_manager.type_manager();
        for (object_type, attribute_types) in &modified_types.owns {
            let mut objects = thing_manager.get_objects_in_range(
                snapshot,
                &object_type.range_with_subtypes_transitive(snapshot, type_manager)?,
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut objects).transpose()? {
                CardinalityValidation::validate_object_has(
                    snapshot,
                    thing_manager,
                    object,
                    attribute_types,
                    out_errors,
                    storage_counters.clone(),
                )?;
            }
        }
        for (object_type, role_types) in &modified_types.plays {
            let mut objects = thing_manager.get_objects_in_range(
                snapshot,
                &object_type.range_with_subtypes_transitive(snapshot, type_manager)?,
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut objects).transpose()? {
                CardinalityValidation::validate_object_links(
                    snapshot,
                    thing_manager,
                    object,
                    role_types,
                    out_errors,
                    storage_counters.clone(),
                )?;
            }
        }
        for (relation_type, role_types) in &modified_types.relates {
            let mut relations = thing_manager.get_relations_in_range(
                snapshot,
                &relation_type.range_with_subtypes_transitive(snapshot, type_manager)?,
                storage_counters.clone(),
            );
            while let Some(relation) = Iterator::next(&mut relations).transpose()? {
                CardinalityValidation::validate_relation_links(
                    snapshot,
                    thing_manager,
                    relation,
                    role_types,
                    out_errors,
                    storage_counters.clone(),
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_object_has(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        modified_attribute_types: &HashSet<AttributeType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = Self::validate_owns_cardinality_constraint(
            snapshot,
            thing_manager,
            object,
            modified_attribute_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    pub(crate) fn validate_object_links(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        modified_role_types: &HashSet<RoleType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = Self::validate_plays_cardinality_constraint(
            snapshot,
            thing_manager,
            object,
            modified_role_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    pub(crate) fn validate_relation_links(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        relation: Relation,
        modified_role_types: &HashSet<RoleType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = Self::validate_relates_cardinality_constraint(
            snapshot,
            thing_manager,
            relation,
            modified_role_types,
            storage_counters,
        );
        collect_errors!(out_errors, cardinality_check, |e: Box<_>| *e);
        Ok(())
    }

    validate_capability_cardinality_constraint!(
        validate_owns_cardinality_constraint,
        Owns,
        Object,
        get_owned_attribute_type_constraints_cardinality,
        get_has_counts,
        DataValidation::validate_owns_instances_cardinality_constraint
    );
    validate_capability_cardinality_constraint!(
        validate_plays_cardinality_constraint,
        Plays,
        Object,
        get_played_role_type_constraints_cardinality,
        get_played_roles_counts,
        DataValidation::validate_plays_instances_cardinality_constraint
    );
    validate_capability_cardinality_constraint!(
        validate_relates_cardinality_constraint,
        Relates,
        Relation,
        get_related_role_type_constraints_cardinality,
        get_player_counts,
        DataValidation::validate_relates_instances_cardinality_constraint
    );
}

pub(crate) struct ModifiedCapabilityTypes {
    owns: HashMap<ObjectType, HashSet<AttributeType>>,
    plays: HashMap<ObjectType, HashSet<RoleType>>,
    relates: HashMap<RelationType, HashSet<RoleType>>,
}

impl ModifiedCapabilityTypes {
    pub(crate) fn collect(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<Self, Box<ConceptReadError>> {
        let mut collection = Self { owns: HashMap::new(), plays: HashMap::new(), relates: HashMap::new() };
        collection.collect_modified_schema_capability_cardinalities(snapshot, type_manager)?;
        Ok(collection)
    }

    pub(crate) fn relates(&self) -> &HashMap<RelationType, HashSet<RoleType>> {
        &self.relates
    }

    fn collect_modified_schema_capability_cardinalities(
        &mut self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
    ) -> Result<(), Box<ConceptReadError>> {
        // New / deleted capabilities

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeOwns),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let attribute_type = AttributeType::new(edge.to());
            let updated_attribute_types = self.owns.entry(ObjectType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_attribute_types.insert(attribute_type);
                }
                Write::Delete => {
                    updated_attribute_types.extend(TypeAPI::chain_types(
                        attribute_type,
                        attribute_type.get_supertypes_transitive(snapshot, type_manager)?.into_iter().cloned(),
                    ));
                }
            }
        }

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgePlays),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let role_type = RoleType::new(edge.to());
            let updated_role_types = self.plays.entry(ObjectType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_role_types.insert(role_type);
                }
                Write::Delete => {
                    updated_role_types.extend(TypeAPI::chain_types(
                        role_type,
                        role_type.get_supertypes_transitive(snapshot, type_manager)?.into_iter().cloned(),
                    ));
                }
            }
        }

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeRelates),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let role_type = RoleType::new(edge.to());
            let updated_role_types = self.relates.entry(RelationType::new(edge.from())).or_default();
            match write {
                Write::Insert { .. } | Write::Put { .. } => {
                    updated_role_types.insert(role_type);
                }
                Write::Delete => {
                    updated_role_types.extend(TypeAPI::chain_types(
                        role_type,
                        role_type.get_supertypes_transitive(snapshot, type_manager)?.into_iter().cloned(),
                    ));
                }
            }
        }

        // New / deleted subs between objects and interfaces

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeSub),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let subtype = edge.from();
            let supertype = edge.to();
            let prefix = supertype.prefix();
            match prefix {
                // Interfaces: owns
                Prefix::VertexAttributeType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let attribute_subtype = AttributeType::new(subtype);
                        for &object_type in attribute_subtype.get_owner_types(snapshot, type_manager)?.keys() {
                            let updated_attribute_types = self.owns.entry(object_type).or_default();
                            updated_attribute_types.insert(attribute_subtype);
                        }
                    }
                    Write::Delete => {
                        let attribute_supertype = AttributeType::new(supertype);
                        for attribute_type in TypeAPI::chain_types(
                            attribute_supertype,
                            attribute_supertype.get_supertypes_transitive(snapshot, type_manager)?.into_iter().cloned(),
                        ) {
                            for &object_type in attribute_type.get_owner_types(snapshot, type_manager)?.keys() {
                                let updated_attribute_types = self.owns.entry(object_type).or_default();
                                updated_attribute_types.insert(attribute_type);
                            }
                        }
                    }
                },
                // Interfaces: plays and relates
                Prefix::VertexRoleType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let role_subtype = RoleType::new(subtype);
                        for &object_type in role_subtype.get_player_types(snapshot, type_manager)?.keys() {
                            let updated_role_types = self.plays.entry(object_type).or_default();
                            updated_role_types.insert(role_subtype);
                        }
                        for &relation_type in role_subtype.get_relation_types(snapshot, type_manager)?.keys() {
                            let updated_role_types = self.relates.entry(relation_type).or_default();
                            updated_role_types.insert(role_subtype);
                        }
                    }
                    Write::Delete => {
                        let role_supertype = RoleType::new(supertype);
                        for role_type in TypeAPI::chain_types(
                            role_supertype,
                            role_supertype.get_supertypes_transitive(snapshot, type_manager)?.into_iter().cloned(),
                        ) {
                            for &object_type in role_type.get_player_types(snapshot, type_manager)?.keys() {
                                let updated_role_types = self.plays.entry(object_type).or_default();
                                updated_role_types.insert(role_type);
                            }
                            for &relation_type in role_type.get_relation_types(snapshot, type_manager)?.keys() {
                                let updated_role_types = self.relates.entry(relation_type).or_default();
                                updated_role_types.insert(role_type);
                            }
                        }
                    }
                },
                // Objects and Relations: owns, plays, and relates
                Prefix::VertexEntityType | Prefix::VertexRelationType => match write {
                    Write::Insert { .. } | Write::Put { .. } => {
                        let object_subtype = ObjectType::new(subtype);
                        let object_supertype = ObjectType::new(supertype);

                        let supertype_owned_attribute_types =
                            object_supertype.get_owned_attribute_types(snapshot, type_manager)?;
                        for attribute_type in object_subtype.get_owned_attribute_types(snapshot, type_manager)? {
                            for &supertype_attribute_type in &supertype_owned_attribute_types {
                                if supertype_attribute_type.is_supertype_transitive_of_or_same(
                                    snapshot,
                                    type_manager,
                                    attribute_type,
                                )? {
                                    let updated_attribute_types = self.owns.entry(object_subtype).or_default();
                                    updated_attribute_types.insert(attribute_type);
                                    break;
                                }
                            }
                        }

                        let supertype_played_role_types =
                            object_supertype.get_played_role_types(snapshot, type_manager)?;
                        for role_type in object_subtype.get_played_role_types(snapshot, type_manager)? {
                            for &supertype_role_type in &supertype_played_role_types {
                                if supertype_role_type.is_supertype_transitive_of_or_same(
                                    snapshot,
                                    type_manager,
                                    role_type,
                                )? {
                                    let updated_role_types = self.plays.entry(object_subtype).or_default();
                                    updated_role_types.insert(role_type);
                                    break;
                                }
                            }
                        }

                        if prefix == Prefix::VertexRelationType {
                            let relation_subtype = RelationType::new(subtype);
                            let relation_supertype = RelationType::new(supertype);

                            let supertype_related_role_types =
                                relation_supertype.get_related_role_types(snapshot, type_manager)?;
                            for role_type in relation_subtype.get_related_role_types(snapshot, type_manager)? {
                                for &supertype_role_type in &supertype_related_role_types {
                                    if supertype_role_type.is_supertype_transitive_of_or_same(
                                        snapshot,
                                        type_manager,
                                        role_type,
                                    )? {
                                        let updated_role_types = self.relates.entry(relation_subtype).or_default();
                                        updated_role_types.insert(role_type);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        // New / deleted annotations

        for (key, _) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::PropertyTypeEdge),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let property = TypeEdgeProperty::decode(Bytes::reference(key.bytes()));
            match property.infix() {
                Infix::PropertyAnnotationKey | Infix::PropertyAnnotationCardinality => {
                    let edge = property.type_edge();
                    match edge.prefix() {
                        Prefix::EdgeOwns => {
                            let updated_attribute_types = self.owns.entry(ObjectType::new(edge.from())).or_default();
                            updated_attribute_types.insert(AttributeType::new(edge.to()));
                        }
                        Prefix::EdgeOwnsReverse => debug_assert!(false, "Unexpected property on reverse owns"),
                        Prefix::EdgePlays => {
                            let updated_role_types = self.plays.entry(ObjectType::new(edge.from())).or_default();
                            updated_role_types.insert(RoleType::new(edge.to()));
                        }
                        Prefix::EdgePlaysReverse => debug_assert!(false, "Unexpected property on reverse plays"),
                        Prefix::EdgeRelates => {
                            let updated_role_types = self.relates.entry(RelationType::new(edge.from())).or_default();
                            updated_role_types.insert(RoleType::new(edge.to()));
                        }
                        Prefix::EdgeRelatesReverse => debug_assert!(false, "Unexpected property on reverse relates"),
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

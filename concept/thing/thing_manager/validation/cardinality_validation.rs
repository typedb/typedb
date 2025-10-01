/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{Bound, HashMap, HashSet};

use bytes::Bytes;
use resource::profile::StorageCounters;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    thing::{
        object::{Object, ObjectAPI},
        relation::Relation,
        thing_manager::{
            validation::{validation::DataValidation, DataValidationError},
            ThingManager,
        },
    },
    type_::{
        attribute_type::AttributeType,
        constraint::{CapabilityConstraint, Constraint},
        owns::Owns,
        plays::Plays,
        relates::Relates,
        role_type::RoleType,
        Capability, OwnerAPI, PlayerAPI, TypeAPI,
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

pub(crate) use collect_errors;
use encoding::{
    graph::{
        thing::{
            edge::{ThingEdgeHas, ThingEdgeLinks},
            vertex_object::ObjectVertex,
            ThingVertex,
        },
        type_::{edge::TypeEdge, property::TypeEdgeProperty, vertex::PrefixedTypeVertexEncoding},
    },
    layout::{infix::Infix, prefix::Prefix},
    Prefixed,
};
use iterator::minmax_or;
use storage::{
    key_range::{KeyRange, RangeEnd, RangeStart},
    key_value::StorageKey,
    snapshot::write::Write,
};

use crate::{
    thing::{attribute::Attribute, ThingAPI},
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
1. Collect instances affected by cardinalities changes (separately for 3 capabilities: owns, plays, relates)
2. Validate only the affected instances to avoid rescanning the whole system (see validate_capability_cardinality_constraint). For each object,
  2a. Count every capability instance it has (every has, every played role, every roleplayer)
  2b. Collect cardinality constraints (declared and inherited) of all marked capabilities without duplications (if a subtype and its supertype are affected, the supertype's constraint is checked once)
  2c. Validate each constraint separately using the counts prepared in 2a. To validate a constraint, take its source type (where this constraint is declared), and count all instances of the source type and its subtypes.

Let's consider the following example:
  entity person,
    owns name @card(1..),
    owns surname, # sub name
    owns changed-surname @card(1..2); # sub surname

A query is being run:
  define person owns surname @card(1..10);

It will be processed like:
1. All instances of persons will be collected, the only surname attribute type saved as modified.
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

pub(crate) struct CardinalityChangeTracker {
    modified_objects_attribute_types: HashMap<Object, HashSet<AttributeType>>,
    has_modified_owns: bool,
    modified_objects_role_types: HashMap<Object, HashSet<RoleType>>,
    has_modified_plays: bool,
    modified_relations_role_types: HashMap<Relation, HashSet<RoleType>>,
    has_modified_relates: bool,
}

impl CardinalityChangeTracker {
    pub(crate) fn build(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        storage_counters: StorageCounters,
    ) -> Result<Self, Box<ConceptReadError>> {
        let mut modified_objects_attribute_types: HashMap<Object, HashSet<AttributeType>> = HashMap::new();
        let mut has_modified_owns = false;
        let mut modified_objects_role_types: HashMap<Object, HashSet<RoleType>> = HashMap::new();
        let mut has_modified_plays = false;
        let mut modified_relations_role_types: HashMap<Relation, HashSet<RoleType>> = HashMap::new();
        let mut has_modified_relates = false;

        Self::collect_new_objects(
            snapshot,
            type_manager,
            &mut modified_objects_attribute_types,
            &mut modified_objects_role_types,
            &mut modified_relations_role_types,
        )?;

        Self::collect_modified_has_objects(
            snapshot,
            thing_manager,
            &mut modified_objects_attribute_types,
            storage_counters.clone(),
        )?;

        Self::collect_modified_links_objects(
            snapshot,
            thing_manager,
            &mut modified_relations_role_types,
            &mut modified_objects_role_types,
            storage_counters.clone(),
        )?;

        Self::collect_modified_schema_capability_cardinalities_objects(
            snapshot,
            type_manager,
            thing_manager,
            &mut modified_objects_attribute_types,
            &mut has_modified_owns,
            &mut modified_objects_role_types,
            &mut has_modified_plays,
            &mut modified_relations_role_types,
            &mut has_modified_relates,
            storage_counters.clone(),
        )?;

        Ok(Self {
            modified_objects_attribute_types,
            has_modified_owns,
            modified_objects_role_types,
            has_modified_plays,
            modified_relations_role_types,
            has_modified_relates,
        })
    }

    pub(crate) fn modified_objects_attribute_types(&self) -> &HashMap<Object, HashSet<AttributeType>> {
        &self.modified_objects_attribute_types
    }

    pub(crate) fn has_modified_owns(&self) -> bool {
        self.has_modified_owns
    }

    pub(crate) fn modified_objects_role_types(&self) -> &HashMap<Object, HashSet<RoleType>> {
        &self.modified_objects_role_types
    }

    pub(crate) fn has_modified_plays(&self) -> bool {
        self.has_modified_plays
    }

    pub(crate) fn modified_relations_role_types(&self) -> &HashMap<Relation, HashSet<RoleType>> {
        &self.modified_relations_role_types
    }

    pub(crate) fn has_modified_relates(&self) -> bool {
        self.has_modified_relates
    }

    fn collect_new_objects(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
    ) -> Result<(), Box<ConceptReadError>> {
        for key in snapshot
            .iterate_writes_range(&KeyRange::new_variable_width(
                RangeStart::Inclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(
                        ObjectVertex::build_prefix_prefix(Prefix::VertexEntity, ObjectVertex::KEYSPACE).bytes(),
                    ),
                )),
                RangeEnd::EndPrefixInclusive(StorageKey::new(
                    ObjectVertex::KEYSPACE,
                    Bytes::<0>::reference(
                        ObjectVertex::build_prefix_prefix(Prefix::VertexRelation, ObjectVertex::KEYSPACE).bytes(),
                    ),
                )),
            ))
            .filter_map(|(key, write)| match write {
                Write::Insert { .. } => Some(key),
                Write::Delete => None,
                Write::Put { .. } => unreachable!("Encountered a Put for an entity"),
            })
        {
            let object = Object::new(ObjectVertex::decode(key.bytes()));
            match &object {
                Object::Entity(_) => {}
                Object::Relation(relation) => {
                    let updated_role_types = out_relation_role_types.entry(*relation).or_default();
                    for relates in relation.type_().get_relates(snapshot, type_manager)?.into_iter() {
                        updated_role_types.insert(relates.role());
                    }
                }
            }

            let updated_attribute_types = out_object_attribute_types.entry(object).or_default();
            for owns in object.type_().get_owns(snapshot, type_manager)?.into_iter() {
                updated_attribute_types.insert(owns.attribute());
            }

            let updated_role_types = out_object_role_types.entry(object).or_default();
            for plays in object.type_().get_plays(snapshot, type_manager)?.into_iter() {
                updated_role_types.insert(plays.role());
            }
        }
        Ok(())
    }

    fn collect_modified_has_objects(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeHas::prefix(), ThingEdgeHas::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeHas::decode(Bytes::Reference(key.byte_array()));
            let owner = Object::new(edge.from());
            let attribute = Attribute::new(edge.to());
            if thing_manager.instance_exists(snapshot, &owner, storage_counters.clone())? {
                let updated_attribute_types = out_object_attribute_types.entry(owner).or_default();
                updated_attribute_types.insert(attribute.type_());
            }
        }

        Ok(())
    }

    fn collect_modified_links_objects(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        for (key, _) in snapshot
            .iterate_writes_range(&KeyRange::new_within(ThingEdgeLinks::prefix(), ThingEdgeLinks::FIXED_WIDTH_ENCODING))
        {
            let edge = ThingEdgeLinks::decode(Bytes::reference(key.bytes()));
            let relation = Relation::new(edge.relation());
            let player = Object::new(edge.player());
            let role_type = RoleType::build_from_type_id(edge.role_id());

            if thing_manager.instance_exists(snapshot, &relation, storage_counters.clone())? {
                let updated_role_types = out_relation_role_types.entry(relation).or_default();
                updated_role_types.insert(role_type);
            }

            if thing_manager.instance_exists(snapshot, &player, storage_counters.clone())? {
                let updated_role_types = out_object_role_types.entry(player).or_default();
                updated_role_types.insert(role_type);
            }
        }

        Ok(())
    }

    fn collect_modified_schema_capability_cardinalities_objects(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        thing_manager: &ThingManager,
        out_object_attribute_types: &mut HashMap<Object, HashSet<AttributeType>>,
        out_has_modified_owns: &mut bool,
        out_object_role_types: &mut HashMap<Object, HashSet<RoleType>>,
        out_has_modified_plays: &mut bool,
        out_relation_role_types: &mut HashMap<Relation, HashSet<RoleType>>,
        out_has_modified_relates: &mut bool,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut modified_owns = HashMap::new();
        let mut modified_plays = HashMap::new();
        let mut modified_relates = HashMap::new();
        Self::collect_modified_schema_capability_cardinalities(
            snapshot,
            type_manager,
            &mut modified_owns,
            &mut modified_plays,
            &mut modified_relates,
            storage_counters.clone(),
        )?;

        if !modified_owns.is_empty() {
            *out_has_modified_owns = true;
        }
        if !modified_plays.is_empty() {
            *out_has_modified_plays = true;
        }
        if !modified_relates.is_empty() {
            *out_has_modified_relates = true;
        }

        for (relation_type, role_types) in modified_relates {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    relation_type,
                    relation_type.get_subtypes_transitive(snapshot, type_manager)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = thing_manager.get_relations_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(relation) = Iterator::next(&mut it).transpose()? {
                let updated_role_types = out_relation_role_types.entry(relation).or_default();
                updated_role_types.extend(role_types.iter());
            }
        }

        for (object_type, role_types) in modified_plays {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    object_type,
                    object_type.get_subtypes_transitive(snapshot, type_manager,)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = thing_manager.get_objects_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut it).transpose()? {
                let updated_role_types = out_object_role_types.entry(object).or_default();
                updated_role_types.extend(role_types.iter());
            }
        }

        for (object_type, attribute_types) in modified_owns {
            let (min, max) = minmax_or!(
                TypeAPI::chain_types(
                    object_type,
                    object_type.get_subtypes_transitive(snapshot, type_manager,)?.into_iter().cloned()
                ),
                unreachable!("Expected at least one object type")
            );
            let mut it = thing_manager.get_objects_in_range(
                snapshot,
                &(Bound::Included(min), Bound::Included(max)),
                storage_counters.clone(),
            );
            while let Some(object) = Iterator::next(&mut it).transpose()? {
                let updated_attribute_types = out_object_attribute_types.entry(object).or_default();
                updated_attribute_types.extend(attribute_types.iter());
            }
        }

        Ok(())
    }

    fn collect_modified_schema_capability_cardinalities(
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        modified_owns: &mut HashMap<ObjectType, HashSet<AttributeType>>,
        modified_plays: &mut HashMap<ObjectType, HashSet<RoleType>>,
        modified_relates: &mut HashMap<RelationType, HashSet<RoleType>>,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        // New / deleted capabilities

        for (key, write) in snapshot.iterate_writes_range(&KeyRange::new_within(
            TypeEdge::build_prefix(Prefix::EdgeOwns),
            TypeEdge::FIXED_WIDTH_ENCODING,
        )) {
            let edge = TypeEdge::decode(Bytes::reference(key.bytes()));
            let attribute_type = AttributeType::new(edge.to());
            let updated_attribute_types = modified_owns.entry(ObjectType::new(edge.from())).or_default();
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
            let updated_role_types = modified_plays.entry(ObjectType::new(edge.from())).or_default();
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
            let updated_role_types = modified_relates.entry(RelationType::new(edge.from())).or_default();
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
                            let updated_attribute_types = modified_owns.entry(object_type).or_default();
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
                                let updated_attribute_types = modified_owns.entry(object_type).or_default();
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
                            let updated_role_types = modified_plays.entry(object_type).or_default();
                            updated_role_types.insert(role_subtype);
                        }
                        for &relation_type in role_subtype.get_relation_types(snapshot, type_manager)?.keys() {
                            let updated_role_types = modified_relates.entry(relation_type).or_default();
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
                                let updated_role_types = modified_plays.entry(object_type).or_default();
                                updated_role_types.insert(role_type);
                            }
                            for &relation_type in role_type.get_relation_types(snapshot, type_manager)?.keys() {
                                let updated_role_types = modified_relates.entry(relation_type).or_default();
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
                                    let updated_attribute_types = modified_owns.entry(object_subtype).or_default();
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
                                    let updated_role_types = modified_plays.entry(object_subtype).or_default();
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
                                        let updated_role_types = modified_relates.entry(relation_subtype).or_default();
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
                            let updated_attribute_types =
                                modified_owns.entry(ObjectType::new(edge.from())).or_default();
                            updated_attribute_types.insert(AttributeType::new(edge.to()));
                        }
                        Prefix::EdgeOwnsReverse => debug_assert!(false, "Unexpected property on reverse owns"),
                        Prefix::EdgePlays => {
                            let updated_role_types = modified_plays.entry(ObjectType::new(edge.from())).or_default();
                            updated_role_types.insert(RoleType::new(edge.to()));
                        }
                        Prefix::EdgePlaysReverse => debug_assert!(false, "Unexpected property on reverse plays"),
                        Prefix::EdgeRelates => {
                            let updated_role_types =
                                modified_relates.entry(RelationType::new(edge.from())).or_default();
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

pub(crate) struct CardinalityValidation {}

impl CardinalityValidation {
    pub(crate) fn validate_object_has(
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        object: Object,
        modified_attribute_types: &HashSet<AttributeType>,
        out_errors: &mut Vec<DataValidationError>,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<ConceptReadError>> {
        let cardinality_check = CardinalityValidation::validate_owns_cardinality_constraint(
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

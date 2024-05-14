/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::label::Label;
use storage::snapshot::ReadableSnapshot;
use crate::type_::attribute_type::AttributeType;
use crate::type_::entity_type::EntityType;
use crate::type_::{OwnerAPI, PlayerAPI};
use crate::type_::object_type::ObjectType;
use crate::type_::relation_type::RelationType;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::{KindAPI, ReadableType};
use crate::type_::type_reader::TypeReader;
use crate::type_::validation::SchemaValidationError;


macro_rules! object_type_match {
    ($obj_var:ident, $block:block) => {
        match $obj_var {
            ObjectType::Entity($obj_var) => {
                $block
            },
            ObjectType::Relation($obj_var) => {
                $block
            }
        }
    }
}


pub struct TypeValidator { }

impl TypeValidator {
    pub(crate) fn validate_no_subtypes<Snapshot, T>(snapshot: &Snapshot, type_: T) -> Result<(), SchemaValidationError>
    where Snapshot: ReadableSnapshot,
          T: KindAPI<'static> + ReadableType
    {
        TypeReader::get_subtypes(snapshot, type_)
            .map_err(SchemaValidationError::ConceptRead)?;
        Ok(())
    }

    pub(crate) fn validate_no_instances<Snapshot, T>(snapshot: &Snapshot, type_: T) -> Result<(), SchemaValidationError>
    where Snapshot: ReadableSnapshot, T: KindAPI<'static>  + ReadableType
    {
        // todo!();
        Ok(())
    }

    pub(crate) fn validate_label_uniqueness<'a, Snapshot: ReadableSnapshot>(
        snapshot: &Snapshot,
        new_label: &Label<'static>,
    ) -> Result<(), SchemaValidationError>
    {
        let attribute_clash = TypeReader::get_labelled_type::<AttributeType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?.is_some();
        let relation_clash = TypeReader::get_labelled_type::<RelationType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?.is_some();
        let entity_clash = TypeReader::get_labelled_type::<EntityType<'static>>(snapshot, &new_label)
            .map_err(SchemaValidationError::ConceptRead)?.is_some();

        if attribute_clash || relation_clash || entity_clash {
            Err(SchemaValidationError::LabelUniqueness(new_label.clone()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_sub_does_not_create_cycle<T, Snapshot>(
        snapshot: &Snapshot,
        type_: T,
        supertype: T,
    ) -> Result<(), SchemaValidationError>
        where
            T: KindAPI<'static> + ReadableType<ReadOutput<'static>=T>,
            Snapshot: ReadableSnapshot,
    {
        let existing_supertypes = TypeReader::get_supertypes_transitive(snapshot, supertype.clone())
            .map_err(SchemaValidationError::ConceptRead)?;
        if supertype == type_ || existing_supertypes.contains(&type_) {
            Err(SchemaValidationError::CyclicTypeHierarchy(type_.wrap_for_error(), supertype.wrap_for_error()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate_role_is_inherited<Snapshot>(
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError>
        where Snapshot: ReadableSnapshot,
    {
        let super_relation = TypeReader::get_supertype(snapshot, relation_type)
            .map_err(SchemaValidationError::ConceptRead)?;
        if super_relation.is_none() {
            return Err(SchemaValidationError::RootModification);
        }
        let is_inherited = TypeReader::get_relates_transitive(snapshot, super_relation.unwrap())
            .map_err(SchemaValidationError::ConceptRead)?
            .contains_key(&role_type);
        if is_inherited { Ok(()) } else { Err(SchemaValidationError::RelatesNotInherited(role_type)) }
    }

    pub(crate) fn validate_owns_is_inherited<Snapshot>(
        snapshot: &Snapshot,
        owner: ObjectType<'static>,
        attribute: AttributeType<'static>
    ) -> Result<(), SchemaValidationError>
        where
            Snapshot: ReadableSnapshot,
    {
        let res = object_type_match!(owner, {
            let super_owner = TypeReader::get_supertype(snapshot, owner)
                .map_err(SchemaValidationError::ConceptRead)?;
            if super_owner.is_none() {
                return Err(SchemaValidationError::RootModification);
            }
            let is_inherited = TypeReader::get_owns_transitive(snapshot, super_owner.unwrap())
                .map_err(SchemaValidationError::ConceptRead)?
                .contains_key(&attribute);
            if is_inherited { Ok(()) } else { Err(SchemaValidationError::OwnsNotInherited(attribute)) }
        });
        res
    }

    pub(crate) fn validate_overridden_is_supertype<Snapshot, T>(
        snapshot: &Snapshot,
        type_: T,
        overridden: T
    ) -> Result<(), SchemaValidationError>
        where
            Snapshot: ReadableSnapshot,
            T: KindAPI<'static> + ReadableType<ReadOutput<'static>=T>
    {
        let supertypes = TypeReader::get_supertypes_transitive(snapshot, type_.clone())
            .map_err(SchemaValidationError::ConceptRead)?;

        if supertypes.contains(&overridden.clone()) {
            Ok(())
        } else {
            Err(SchemaValidationError::OverriddenTypeNotSupertype(type_.wrap_for_error(), overridden.wrap_for_error()))
        }
    }

    pub(crate) fn validate_plays_is_inherited<Snapshot>(
        snapshot: &Snapshot,
        player: ObjectType<'static>,
        role_type: RoleType<'static>,
    ) -> Result<(), SchemaValidationError>
        where Snapshot: ReadableSnapshot,
    {
        let res = object_type_match!(player, {
            let super_player = TypeReader::get_supertype(snapshot, player)
                .map_err(SchemaValidationError::ConceptRead)?;
            if super_player.is_none() {
                return Err(SchemaValidationError::RootModification);
            }
            let is_inherited = TypeReader::get_plays_transitive(snapshot, super_player.unwrap())
                .map_err(SchemaValidationError::ConceptRead)?
                .contains_key(&role_type);
            if is_inherited { Ok(()) } else { Err(SchemaValidationError::PlaysNotInherited(role_type)) }
        });
        res
    }
}

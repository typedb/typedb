/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::{collections::HashSet, rc::Rc, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use durability::DurabilityService;
use encoding::{
    graph::type_::{
        edge::{
            build_edge_owns, build_edge_owns_prefix_from, build_edge_owns_reverse, build_edge_plays,
            build_edge_plays_prefix_from, build_edge_plays_reverse, build_edge_relates, build_edge_relates_prefix_from,
            build_edge_relates_reverse, build_edge_sub, build_edge_sub_prefix_from, build_edge_sub_reverse,
            new_edge_owns, new_edge_plays, new_edge_relates, new_edge_sub,
        },
        index::LabelToTypeVertexIndex,
        property::{
            build_property_type_annotation_abstract, build_property_type_annotation_duplicate,
            build_property_type_label, build_property_type_value_type,
        },
        vertex::{
            new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, new_vertex_role_type,
            TypeVertex,
        },
        vertex_generator::TypeVertexGenerator,
        Kind,
    },
    value::{
        label::Label,
        string::StringBytes,
        value_type::{ValueType, ValueTypeID},
    },
    AsBytes, Keyable,
};
use primitive::{maybe_owns::MaybeOwns, prefix_range::PrefixRange};
use resource::constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_KEY_INLINE};
use storage::{snapshot::Snapshot, MVCCStorage};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{AnnotationAbstract, AnnotationDuplicate},
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        object_type::ObjectType,
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_cache::TypeCache,
        TypeAPI,
    },
};

pub struct TypeManager<'txn, 'storage: 'txn, D> {
    snapshot: Rc<Snapshot<'storage, D>>,
    vertex_generator: &'txn TypeVertexGenerator,
    type_cache: Option<Arc<TypeCache>>,
}

// TODO:
//   if we drop/close without committing, then we need to release all the IDs taken back to the IDGenerator
//   this is only applicable for type manager where we can only have 1 concurrent txn and IDs are precious

macro_rules! get_type_methods {
    ($(
        fn $method_name:ident() -> $output_type:ident = $cache_method:ident | $new_vertex_method:ident;
    )*) => {
        $(
            pub fn $method_name(&self, label: &Label<'_>) -> Result<Option<$output_type<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(label))
                } else {
                    self.get_labelled_type(label, |bytes| $output_type::new($new_vertex_method(bytes)))
                }
            }
        )*

        fn get_labelled_type<M, U>(&self, label: &Label<'_>, mapper: M) -> Result<Option<U>, ConceptReadError>
            where
                M: FnOnce(Bytes<'static, BUFFER_KEY_INLINE>) -> U,
        {
            let key = LabelToTypeVertexIndex::build(label).into_storage_key();
            match self.snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference()) {
                Ok(None) => Ok(None),
                Ok(Some(value)) => Ok(Some(mapper(Bytes::Array(value)))),
                Err(error) => Err(ConceptReadError::SnapshotGet { source: error })
            }
        }
    }
}

macro_rules! get_supertype_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<Option<$type_<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    Ok(self
                        .snapshot
                        .iterate_range(PrefixRange::new_within(build_edge_sub_prefix_from(type_.into_vertex().clone())))
                        .first_cloned()
                        .map_err(|error| ConceptReadError::SnapshotIterate { source: error })?
                        .map(|(key, _)| $type_::new(new_edge_sub(key.into_byte_array_or_ref()).to().into_owned())))
                }
            }
        )*
    }
}

macro_rules! get_supertypes_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let mut supertypes = Vec::new();
                    let mut super_vertex = self.get_storage_supertype(type_.vertex().clone().into_owned());
                    while super_vertex.is_some() {
                        let super_type = $type_::new(super_vertex.as_ref().unwrap().clone());
                        super_vertex = self.get_storage_supertype(super_type.vertex().clone());
                        supertypes.push(super_type);
                    }
                    Ok(MaybeOwns::owned(supertypes))
                }
            }
        )*
    }
}

macro_rules! get_type_is_root_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $base_variant:expr;
    )*) => {
        $(
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<bool, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    Ok(*type_.get_label(self)? == $base_variant.root_label())
                }
            }
        )*
    }
}

macro_rules! get_type_label_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(&self, type_: $type_<'static>) -> Result<MaybeOwns<'_, Label<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    Ok(MaybeOwns::owned(self.get_storage_label(type_.into_vertex())?.unwrap()))
                }
            }
        )*
    }
}

macro_rules! get_type_annotations {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let mut annotations = HashSet::new();
                    if let Some(annotation) = self.get_storage_vertex_annotation_abstract(type_.into_vertex())? {
                        annotations.insert($annotation_type::Abstract(annotation));
                    }
                    Ok(MaybeOwns::owned(annotations))
                }
            }
        )*
    }
}

impl<'txn, 'storage: 'txn, D> TypeManager<'txn, 'storage, D> {
    pub fn new(
        snapshot: Rc<Snapshot<'storage, D>>,
        vertex_generator: &'txn TypeVertexGenerator,
        schema_cache: Option<Arc<TypeCache>>,
    ) -> Self {
        TypeManager { snapshot, vertex_generator, type_cache: schema_cache }
    }

    pub fn initialise_types(
        storage: &mut MVCCStorage<D>,
        vertex_generator: &TypeVertexGenerator,
    ) -> Result<(), ConceptWriteError>
    where
        D: DurabilityService,
    {
        let snapshot = Rc::new(Snapshot::Write(storage.open_snapshot_write()));
        {
            let type_manager = TypeManager::new(snapshot.clone(), vertex_generator, None);
            let root_entity = type_manager.create_entity_type(&Kind::Entity.root_label(), true)?;
            root_entity.set_annotation(&type_manager, EntityTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
            let root_relation = type_manager.create_relation_type(&Kind::Relation.root_label(), true)?;
            root_relation.set_annotation(&type_manager, RelationTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
            let root_role = type_manager.create_role_type(&Kind::Role.root_label(), root_relation.clone(), true)?;
            root_role.set_annotation(&type_manager, RoleTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
            let root_attribute = type_manager.create_attribute_type(&Kind::Attribute.root_label(), true)?;
            root_attribute
                .set_annotation(&type_manager, AttributeTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
        }
        // TODO: handle error properly
        let Snapshot::Write(write_snapshot) = Rc::try_unwrap(snapshot).ok().unwrap() else { panic!() };
        write_snapshot.commit().unwrap();
        Ok(())
    }

    get_type_methods! {
        fn get_entity_type() -> EntityType = get_entity_type | new_vertex_entity_type;
        fn get_relation_type() -> RelationType = get_relation_type | new_vertex_relation_type;
        fn get_role_type() -> RoleType = get_role_type | new_vertex_role_type;
        fn get_attribute_type() -> AttributeType = get_attribute_type | new_vertex_attribute_type;
    }

    get_supertype_methods! {
        fn get_entity_type_supertype() -> EntityType = get_entity_type_supertype;
        fn get_relation_type_supertype() -> RelationType = get_relation_type_supertype;
        fn get_role_type_supertype() -> RoleType = get_role_type_supertype;
        fn get_attribute_type_supertype() -> AttributeType = get_attribute_type_supertype;
    }

    get_supertypes_methods! {
        fn get_entity_type_supertypes() -> EntityType = get_entity_type_supertypes;
        fn get_relation_type_supertypes() -> RelationType = get_relation_type_supertypes;
        fn get_role_type_supertypes() -> RoleType = get_role_type_supertypes;
        fn get_attribute_type_supertypes() -> AttributeType = get_attribute_type_supertypes;
    }

    pub fn create_entity_type(
        &self,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<EntityType<'static>, ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            // TODO: this should not crash the server, and be handled as an Error instead
            panic!("Illegal state: creating types requires write snapshot")
        };

        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_entity_type(write_snapshot);
        self.set_storage_label(type_vertex.clone(), label)?;
        if !is_root {
            self.set_storage_supertype(
                type_vertex.clone(),
                self.get_entity_type(&Kind::Entity.root_label()).unwrap().unwrap().into_vertex(),
            )?;
        }
        Ok(EntityType::new(type_vertex))
    }

    pub fn create_relation_type(
        &self,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<RelationType<'static>, ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating types requires write snapshot")
        };

        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_relation_type(write_snapshot);
        self.set_storage_label(type_vertex.clone(), label)?;
        if !is_root {
            self.set_storage_supertype(
                type_vertex.clone(),
                self.get_relation_type(&Kind::Relation.root_label())?.unwrap().into_vertex(),
            )?;
        }
        Ok(RelationType::new(type_vertex))
    }

    pub(crate) fn create_role_type(
        &self,
        label: &Label<'_>,
        relation_type: RelationType<'static>,
        is_root: bool,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating types requires write snapshot")
        };

        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_role_type(write_snapshot);
        self.set_storage_label(type_vertex.clone(), label)?;
        self.set_storage_relates(relation_type.into_vertex(), type_vertex.clone())?;
        if !is_root {
            self.set_storage_supertype(
                type_vertex.clone(),
                self.get_role_type(&Kind::Role.root_label()).unwrap().unwrap().into_vertex(),
            )?;
        }
        Ok(RoleType::new(type_vertex))
    }

    pub fn create_attribute_type(
        &self,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<AttributeType<'static>, ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating types requires write snapshot")
        };

        // TODO: validate type doesn't exist already
        let type_vertex = self.vertex_generator.create_attribute_type(write_snapshot);
        self.set_storage_label(type_vertex.clone(), label)?;
        if !is_root {
            self.set_storage_supertype(
                type_vertex.clone(),
                self.get_attribute_type(&Kind::Attribute.root_label())?.unwrap().into_vertex(),
            )?;
        }
        Ok(AttributeType::new(type_vertex))
    }

    get_type_is_root_methods! {
        fn get_entity_type_is_root() -> EntityType = get_entity_type_is_root | Kind::Entity;
        fn get_relation_type_is_root() -> RelationType = get_relation_type_is_root | Kind::Relation;
        fn get_role_type_is_root() -> RoleType = get_role_type_is_root | Kind::Role;
        fn get_attribute_type_is_root() -> AttributeType = get_attribute_type_is_root | Kind::Attribute;
    }

    get_type_label_methods! {
        fn get_entity_type_label() -> EntityType = get_entity_type_label;
        fn get_relation_type_label() -> RelationType = get_relation_type_label;
        fn get_role_type_label() -> RoleType = get_role_type_label;
        fn get_attribute_type_label() -> AttributeType = get_attribute_type_label;
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_owns(entity_type)))
        } else {
            let owns = self.get_storage_owns(entity_type.clone().into_vertex(), |attr_vertex| {
                Owns::new(ObjectType::Entity(entity_type.clone()), AttributeType::new(attr_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_owns(relation_type)))
        } else {
            let owns = self.get_storage_owns(relation_type.clone().into_vertex(), |attr_vertex| {
                Owns::new(
                    ObjectType::Relation(relation_type.clone()),
                    AttributeType::new(attr_vertex.clone().into_owned()),
                )
            })?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = self.get_storage_relates(relation_type.clone().into_vertex(), |role_vertex| {
                Relates::new(relation_type.clone(), RoleType::new(role_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(relates))
        }
    }

    pub(crate) fn get_entity_type_plays<'this>(
        &'this self,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_plays(entity_type)))
        } else {
            let plays = self.get_storage_plays(entity_type.clone().into_vertex(), |role_vertex| {
                Plays::new(ObjectType::Entity(entity_type.clone()), RoleType::new(role_vertex.clone().into_owned()))
            })?;
            Ok(MaybeOwns::owned(plays))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type))
        } else {
            self.get_storage_value_type(attribute_type.into_vertex())
        }
    }

    get_type_annotations! {
        fn get_entity_type_annotations() -> EntityType = get_entity_type_annotations | EntityTypeAnnotation;
        fn get_relation_type_annotations() -> RelationType = get_relation_type_annotations | RelationTypeAnnotation;
        fn get_role_type_annotations() -> RoleType = get_role_type_annotations | RoleTypeAnnotation;
        fn get_attribute_type_annotations() -> AttributeType = get_attribute_type_annotations | AttributeTypeAnnotation;
    }

    // --- storage operations ---
    // TODO: these should take Concepts instead of Vertices

    fn get_storage_label(&self, owner: TypeVertex<'_>) -> Result<Option<Label<'static>>, ConceptReadError> {
        let key = build_property_type_label(owner.clone().into_owned());
        self.snapshot
            .get_mapped(key.into_storage_key().as_reference(), |reference| {
                let value = StringBytes::new(Bytes::<LABEL_SCOPED_NAME_STRING_INLINE>::Reference(reference));
                Label::parse_from(value)
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn set_storage_label(
        &self,
        owner: TypeVertex<'static>,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating types requires write snapshot")
        };

        self.may_delete_storage_label(owner.clone())?;

        let vertex_to_label_key = build_property_type_label(owner.clone());
        let label_value = ByteArray::from(label.scoped_name().bytes());
        write_snapshot.put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

        let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
        let vertex_value = ByteArray::from(owner.bytes());
        write_snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);

        Ok(())
    }

    fn may_delete_storage_label(&self, owner: TypeVertex<'_>) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating types requires write snapshot")
        };

        let existing_label = self.get_storage_label(owner.clone())?;
        if let Some(label) = existing_label {
            let vertex_to_label_key = build_property_type_label(owner.clone());
            write_snapshot.delete(vertex_to_label_key.into_storage_key().into_owned_array());
            let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
            write_snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
        }
        Ok(())
    }

    fn get_storage_supertype(&self, subtype: TypeVertex<'static>) -> Option<TypeVertex<'static>> {
        // TODO: handle possible errors
        self.snapshot
            .iterate_range(PrefixRange::new_within(build_edge_sub_prefix_from(subtype.clone())))
            .first_cloned()
            .unwrap()
            .map(|(key, _)| new_edge_sub(key.into_byte_array_or_ref()).to().into_owned())
    }

    pub(crate) fn set_storage_supertype(
        &self,
        subtype: TypeVertex<'static>,
        supertype: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        self.may_delete_storage_supertype(subtype.clone())?;
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating supertype edge requires write snapshot")
        };
        let sub = build_edge_sub(subtype.clone(), supertype.clone());
        write_snapshot.put(sub.into_storage_key().into_owned_array());
        let sub_reverse = build_edge_sub_reverse(supertype, subtype);
        write_snapshot.put(sub_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    fn may_delete_storage_supertype(&self, subtype: TypeVertex<'static>) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting supertype edge requires write snapshot")
        };

        let supertype = self.get_storage_supertype(subtype.clone());
        if let Some(supertype) = supertype {
            let sub = build_edge_sub(subtype.clone(), supertype.clone());
            write_snapshot.delete(sub.into_storage_key().into_owned_array());
            let sub_reverse = build_edge_sub_reverse(supertype, subtype);
            write_snapshot.delete(sub_reverse.into_storage_key().into_owned_array());
        }
        Ok(())
    }

    fn get_storage_owns<F>(
        &self,
        owner: TypeVertex<'static>,
        mapper: F,
    ) -> Result<HashSet<Owns<'static>>, ConceptReadError>
    where
        F: for<'b> Fn(TypeVertex<'b>) -> Owns<'static>,
    {
        let owns_prefix = build_edge_owns_prefix_from(owner);
        // TODO: handle possible errors
        self.snapshot
            .iterate_range(PrefixRange::new_within(owns_prefix))
            .collect_cloned_key_hashset(|key| {
                let owns_edge = new_edge_owns(Bytes::Reference(key.byte_ref()));
                mapper(owns_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn set_storage_owns(
        &self,
        owner: TypeVertex<'static>,
        attribute: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating owns edge requires write snapshot")
        };
        let owns = build_edge_owns(owner.clone(), attribute.clone());
        write_snapshot.put(owns.into_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute, owner);
        write_snapshot.put(owns_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn delete_storage_owns(
        &self,
        owner: TypeVertex<'static>,
        attribute: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting owns edge requires write snapshot")
        };

        let owns = build_edge_owns(owner.clone(), attribute.clone());
        write_snapshot.delete(owns.into_storage_key().into_owned_array());
        let owns_reverse = build_edge_owns_reverse(attribute, owner);
        write_snapshot.delete(owns_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    fn get_storage_plays<F>(
        &self,
        player: TypeVertex<'static>,
        mapper: F,
    ) -> Result<HashSet<Plays<'static>>, ConceptReadError>
    where
        F: for<'b> Fn(TypeVertex<'b>) -> Plays<'static>,
    {
        let plays_prefix = build_edge_plays_prefix_from(player);
        self.snapshot
            .iterate_range(PrefixRange::new_within(plays_prefix))
            .collect_cloned_key_hashset(|key| {
                let plays_edge = new_edge_plays(Bytes::Reference(key.byte_ref()));
                mapper(plays_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn set_storage_plays(
        &self,
        player: TypeVertex<'static>,
        role: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating plays edge requires write snapshot")
        };
        let plays = build_edge_plays(player.clone(), role.clone());
        write_snapshot.put(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role, player);
        write_snapshot.put(plays_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn delete_storage_plays(
        &self,
        player: TypeVertex<'static>,
        role: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting plays edge requires write snapshot")
        };
        let plays = build_edge_plays(player.clone(), role.clone());
        write_snapshot.delete(plays.into_storage_key().into_owned_array());
        let plays_reverse = build_edge_plays_reverse(role, player);
        write_snapshot.delete(plays_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    fn get_storage_relates<F>(
        &self,
        relation: TypeVertex<'static>,
        mapper: F,
    ) -> Result<HashSet<Relates<'static>>, ConceptReadError>
    where
        F: for<'b> Fn(TypeVertex<'b>) -> Relates<'static>,
    {
        let relates_prefix = build_edge_relates_prefix_from(relation);
        self.snapshot
            .iterate_range(PrefixRange::new_within(relates_prefix))
            .collect_cloned_key_hashset(|key| {
                let relates_edge = new_edge_relates(Bytes::Reference(key.byte_ref()));
                mapper(relates_edge.to())
            })
            .map_err(|error| ConceptReadError::SnapshotIterate { source: error })
    }

    pub(crate) fn set_storage_relates(
        &self,
        relation: TypeVertex<'static>,
        role: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: creating relates edge requires write snapshot")
        };

        let relates = build_edge_relates(relation.clone(), role.clone());
        write_snapshot.put(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role, relation);
        write_snapshot.put(relates_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn delete_storage_relates(
        &self,
        relation: TypeVertex<'static>,
        role: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting relates edge requires write snapshot")
        };

        let relates = build_edge_relates(relation.clone(), role.clone());
        write_snapshot.delete(relates.into_storage_key().into_owned_array());
        let relates_reverse = build_edge_relates_reverse(role, relation);
        write_snapshot.delete(relates_reverse.into_storage_key().into_owned_array());
        Ok(())
    }

    fn get_storage_value_type(&self, vertex: TypeVertex<'static>) -> Result<Option<ValueType>, ConceptReadError> {
        self.snapshot
            .get_mapped(build_property_type_value_type(vertex).into_storage_key().as_reference(), |bytes| {
                ValueType::from_value_type_id(ValueTypeID::new(bytes.bytes().try_into().unwrap()))
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn set_storage_value_type(
        &self,
        vertex: TypeVertex<'static>,
        value_type: ValueType,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: setting value type requires write snapshot.")
        };

        let property_key = build_property_type_value_type(vertex).into_storage_key().into_owned_array();
        let property_value = ByteArray::copy(&value_type.value_type_id().bytes());
        write_snapshot.put_val(property_key, property_value);
        Ok(())
    }

    fn get_storage_vertex_annotation_abstract(
        &self,
        vertex: TypeVertex<'static>,
    ) -> Result<Option<AnnotationAbstract>, ConceptReadError> {
        self.snapshot
            .get_mapped(build_property_type_annotation_abstract(vertex).into_storage_key().as_reference(), |_bytes| {
                AnnotationAbstract::new()
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn set_storage_annotation_abstract(&self, vertex: TypeVertex<'static>) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: setting annotation requires write snapshot.")
        };
        let annotation_property = build_property_type_annotation_abstract(vertex);
        write_snapshot.put(annotation_property.into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn delete_storage_annotation_abstract(
        &self,
        vertex: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting annotation requires write snapshot.")
        };
        let annotation_property = build_property_type_annotation_abstract(vertex);
        write_snapshot.delete(annotation_property.into_storage_key().into_owned_array());
        Ok(())
    }

    fn get_storage_vertex_annotation_duplicate(
        &self,
        vertex: TypeVertex<'static>,
    ) -> Result<Option<AnnotationDuplicate>, ConceptReadError> {
        self.snapshot
            .get_mapped(build_property_type_annotation_duplicate(vertex).into_storage_key().as_reference(), |_bytes| {
                AnnotationDuplicate::new()
            })
            .map_err(|error| ConceptReadError::SnapshotGet { source: error })
    }

    pub(crate) fn set_storage_annotation_duplicate(
        &self,
        vertex: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: setting annotation requires write snapshot.")
        };
        let annotation_property = build_property_type_annotation_duplicate(vertex);
        write_snapshot.put(annotation_property.into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn delete_storage_annotation_duplicate(
        &self,
        vertex: TypeVertex<'static>,
    ) -> Result<(), ConceptWriteError> {
        let Snapshot::Write(write_snapshot) = self.snapshot.as_ref() else {
            panic!("Illegal state: deleting annotation requires write snapshot.")
        };
        let annotation_property = build_property_type_annotation_duplicate(vertex);
        write_snapshot.delete(annotation_property.into_storage_key().into_owned_array());
        Ok(())
    }
}

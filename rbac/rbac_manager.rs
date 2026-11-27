/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use encoding::{
    AsBytes, Keyable,
    graph::{
        rbac::{RBACAccessKind, RBACRoleProperty, RBACRoleTypeAccess, RBACRoleVertex, RBACVertexGenerator},
        type_::{index::NameToRBACRoleVertexIndex, vertex::TypeVertexEncoding},
    },
    value::{label::Label, string_bytes::StringBytes},
};
use primitive::maybe_owns::MaybeOwns;
use resource::{
    constants::{encoding::LABEL_SCOPED_NAME_STRING_INLINE, snapshot::BUFFER_KEY_INLINE},
    profile::StorageCounters,
};
use storage::{
    key_range::KeyRange,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{RBACAccess, RBACError, RBACRole, rbac_cache::RBACCache};

#[derive(Debug)]
pub struct RBACManager {
    rbac_role_allocator: Arc<RBACVertexGenerator>,
    rbac_cache: Option<Arc<RBACCache>>,
}

impl RBACManager {
    pub fn new(role_id_allocator: Arc<RBACVertexGenerator>, rbac_cache: Option<Arc<RBACCache>>) -> Self {
        RBACManager { rbac_role_allocator: role_id_allocator, rbac_cache }
    }

    pub fn get_rbac_role(
        &self,
        snapshot: &impl ReadableSnapshot,
        role_name: &str,
        _storage_counters: StorageCounters,
    ) -> Result<Option<RBACRole>, Box<RBACError>> {
        if let Some(cache) = &self.rbac_cache {
            Ok(cache.get_rbac_role(role_name))
        } else {
            RBACReader::get_rbac_role(snapshot, role_name)
        }
    }

    pub fn list_rbac_declared_access(
        &self,
        snapshot: &impl ReadableSnapshot,
        role: &RBACRole,
        _storage_counters: StorageCounters,
    ) -> Result<MaybeOwns<'_, HashSet<RBACAccess>>, Box<RBACError>> {
        if let Some(cache) = &self.rbac_cache {
            Ok(MaybeOwns::Borrowed(cache.list_rbac_role_accesses(role)))
        } else {
            Ok(MaybeOwns::Owned(RBACReader::list_rbac_declared_access(snapshot, role)?))
        }
    }

    pub fn create_rbac_role(
        &self,
        snapshot: &mut impl WritableSnapshot,
        name: &str,
        storage_counters: StorageCounters,
    ) -> Result<RBACRole, Box<RBACError>> {
        let existing = self.get_rbac_role(snapshot, name, storage_counters)?;
        if existing.is_some() {
            return Err(Box::new(RBACError::RBACRoleAlreadyExists { name: name.to_owned() }));
        }
        let role_vertex = self
            .rbac_role_allocator
            .allocate(snapshot)
            .map_err(|source| Box::new(RBACError::CreateRBACRoleEncoding { source }))?;
        let role = RBACRole::new(role_vertex);
        RBACWriter::storage_set_rbac_role_name(snapshot, role, name);
        Ok(role)
    }

    pub fn delete_rbac_role(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role: RBACRole,
        storage_counters: StorageCounters,
    ) -> Result<(), Box<RBACError>> {
        for access in &self.list_rbac_declared_access(snapshot, &role, storage_counters.clone())? {
            self.unset_rbac_access(snapshot, &role, *access, storage_counters.clone())?;
        }
        RBACWriter::storage_delete_rbac_role_and_name(snapshot, role)?;
        Ok(())
    }

    pub fn set_rbac_access(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role: RBACRole,
        access: RBACAccess,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<RBACError>> {
        RBACWriter::storage_put_rbac_role_access(snapshot, role, access);
        Ok(())
    }

    pub fn unset_rbac_access(
        &self,
        snapshot: &mut impl WritableSnapshot,
        role: &RBACRole,
        access: RBACAccess,
        _storage_counters: StorageCounters,
    ) -> Result<(), Box<RBACError>> {
        RBACWriter::storage_delete_rbac_role_access(snapshot, role, access);
        Ok(())
    }

    pub fn check_has_access(
        &self,
        snapshot: &impl ReadableSnapshot,
        role: &RBACRole,
        access: &RBACAccess,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<RBACError>> {
        let all_accesses = self.list_rbac_declared_access(snapshot, role, storage_counters)?;
        Ok(all_accesses.contains(access))
    }
}

pub struct RBACReader;

impl RBACReader {
    pub fn get_all_rbac_roles(snapshot: &impl ReadableSnapshot) -> Result<Vec<RBACRole>, Box<RBACError>> {
        snapshot
            .iterate_range(
                &KeyRange::new_within(RBACRoleVertex::build_prefix(), RBACRoleVertex::FIXED_WIDTH_ENCODING),
                StorageCounters::DISABLED,
            )
            .collect_cloned_vec(|key, _| RBACRole::new(RBACRoleVertex::decode(Bytes::copy(key.bytes()))))
            .map_err(|error| Box::new(RBACError::SnapshotIterate { source: error }))
    }

    pub fn get_rbac_role(snapshot: &impl ReadableSnapshot, name: &str) -> Result<Option<RBACRole>, Box<RBACError>> {
        let key = NameToRBACRoleVertexIndex::build(name).into_storage_key();
        match snapshot.get::<BUFFER_KEY_INLINE>(key.as_reference(), StorageCounters::DISABLED) {
            Err(error) => Err(Box::new(RBACError::SnapshotGet { source: error })),
            Ok(None) => Ok(None),
            Ok(Some(value)) => Ok(Some(RBACRole::new(RBACRoleVertex::decode(Bytes::Array(value))))),
        }
    }

    pub fn get_rbac_role_name(
        snapshot: &impl ReadableSnapshot,
        role: &RBACRole,
    ) -> Result<Option<String>, Box<RBACError>> {
        let property = snapshot
            .get_mapped(
                RBACRoleProperty::new_key_for_label(role.vertex()).into_storage_key().as_reference(),
                |value| {
                    let string_bytes = StringBytes::new(Bytes::<{ LABEL_SCOPED_NAME_STRING_INLINE }>::Reference(value));
                    string_bytes.as_str().to_owned()
                },
                StorageCounters::DISABLED,
            )
            .map_err(|err| Box::new(RBACError::SnapshotGet { source: err }))?;
        Ok(property)
    }

    pub fn list_rbac_declared_access(
        snapshot: &impl ReadableSnapshot,
        role: &RBACRole,
    ) -> Result<HashSet<RBACAccess>, Box<RBACError>> {
        snapshot
            .iterate_range(
                &KeyRange::new_within(
                    RBACRoleTypeAccess::build_prefix_role(role.vertex()),
                    RBACRoleTypeAccess::FIXED_WIDTH_ENCODING,
                ),
                StorageCounters::DISABLED,
            )
            .collect_cloned_hashset(|key, _| {
                let access = RBACRoleTypeAccess::decode(Bytes::Reference(key.bytes()).into_owned());
                RBACAccess::new_type_access(access.access_kind(), access.type_())
            })
            .map_err(|error| Box::new(RBACError::SnapshotIterate { source: error }))
    }
}

pub(crate) struct RBACWriter;

impl RBACWriter {
    pub(crate) fn storage_set_rbac_role_name(snapshot: &mut impl WritableSnapshot, role: RBACRole, name: &str) {
        let index_key = NameToRBACRoleVertexIndex::build(name);
        snapshot.put_val(
            index_key.into_storage_key().into_owned_array(),
            bytes::byte_array::ByteArray::copy(&role.vertex().to_bytes()),
        );

        let property_label_key = RBACRoleProperty::new_key_for_label(role.vertex());
        let property_label_value = StringBytes::build_ref(name).to_bytes();
        snapshot.put_val(property_label_key.into_storage_key().into_owned_array(), property_label_value.into_array());
    }

    pub(crate) fn storage_put_rbac_role_access(
        snapshot: &mut impl WritableSnapshot,
        role: RBACRole,
        access: RBACAccess,
    ) {
        let to_write = access.to_type_access_for_role(&role);
        snapshot.put(to_write.into_storage_key().into_owned_array())
    }

    pub(crate) fn storage_delete_rbac_role_and_name(
        snapshot: &mut impl WritableSnapshot,
        role: RBACRole,
    ) -> Result<(), Box<RBACError>> {
        if let Some(name) = RBACReader::get_rbac_role_name(snapshot, &role)? {
            let index_key = NameToRBACRoleVertexIndex::build(name.as_str());
            snapshot.delete(index_key.into_storage_key().into_owned_array());

            let property_label_key = RBACRoleProperty::new_key_for_label(role.vertex());
            snapshot.delete(property_label_key.into_storage_key().into_owned_array());
        } else {
            tracing::warn!("No label found for role being deleted. Skipping deleting index & name property!");
        }
        snapshot.delete(role.vertex().into_storage_key().into_owned_array());
        Ok(())
    }

    pub(crate) fn storage_delete_rbac_role_access(
        snapshot: &mut impl WritableSnapshot,
        role: &RBACRole,
        access: RBACAccess,
    ) {
        let to_write = access.to_type_access_for_role(role);
        snapshot.delete(to_write.into_storage_key().into_owned_array())
    }
}

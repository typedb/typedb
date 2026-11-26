/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use storage::{MVCCStorage, sequence_number::SequenceNumber, snapshot::ReadableSnapshot};

use crate::{RBACAccess, RBACRole, rbac_manager::RBACReader};

#[derive(Debug)]
pub struct RBACCache {
    roles_index_name: HashMap<String, RBACRole>,
    role_access: HashMap<RBACRole, HashSet<RBACAccess>>,
}

impl RBACCache {
    pub fn new<D>(storage: Arc<MVCCStorage<D>>, open_sequence_number: SequenceNumber) -> Self {
        let snapshot = storage.open_snapshot_read_at(open_sequence_number);
        let cache = Self::build(&snapshot);
        cache
    }

    pub fn build(snapshot: &impl ReadableSnapshot) -> Self {
        let all_roles = RBACReader::get_all_rbac_roles(snapshot).unwrap();
        let mut roles_index_name = HashMap::new();
        let mut role_access = HashMap::new();
        for role in all_roles {
            if let Some(name) = RBACReader::get_rbac_role_name(snapshot, &role).unwrap() {
                roles_index_name.insert(name, role);
            }
            let access = RBACReader::list_rbac_declared_access(snapshot, &role).unwrap();
            role_access.insert(role, access);
        }
        RBACCache { roles_index_name, role_access }
    }

    pub fn get_rbac_role(&self, name: &str) -> Option<RBACRole> {
        self.roles_index_name.get(name).copied()
    }

    pub fn list_rbac_role_accesses(&self, role: &RBACRole) -> &HashSet<RBACAccess> {
        self.role_access.get(role).unwrap()
    }
}

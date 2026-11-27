/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock, RwLock},
};

use database::{Database, database::DatabaseUid};
use storage::durability_client::WALClient;
use system::{
    concepts::{DatabasePermission, Grantee, SchemaDefinedRole, SystemPermission, User},
    repositories::rbac_repository,
    util::transaction_util::TransactionUtil,
};

use crate::errors::{PermissionCheckError, PermissionManagementError};

#[derive(Debug)]
pub(crate) struct UserPermissionCache {
    system_permissions: OnceLock<HashSet<SystemPermission>>,
    database_permissions: RwLock<HashMap<DatabaseUid, HashSet<DatabasePermission>>>,
    database_roles: RwLock<HashMap<DatabaseUid, Arc<HashSet<SchemaDefinedRole>>>>,
}

impl UserPermissionCache {
    fn new() -> Self {
        Self {
            system_permissions: OnceLock::new(),
            database_permissions: RwLock::new(HashMap::new()),
            database_roles: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct UserSession {
    username: String,
    permission_cache: UserPermissionCache,
}

impl UserSession {
    pub fn new(username: String) -> UserSession {
        Self { username, permission_cache: UserPermissionCache::new() }
    }

    pub fn username(&self) -> &str {
        self.username.as_str()
    }

    fn grantee(&self) -> Grantee {
        Grantee::User(User::new(self.username.clone()))
    }

    pub(crate) fn has_system_permission(
        &self,
        permission_manager: &PermissionManager,
        permission: &SystemPermission,
    ) -> Result<bool, PermissionCheckError> {
        if let Some(cached_permissions) = self.permission_cache.system_permissions.get() {
            return Ok(cached_permissions.contains(permission));
        }
        let system_permissions = permission_manager
            .list_system_permissions(&self.grantee())
            .map_err(|typedb_source| PermissionCheckError::PermissionManagement { typedb_source })?;
        let answer = system_permissions.contains(permission);
        // Not a big deal. Another thread won the initialisation race.
        let _ = self.permission_cache.system_permissions.set(system_permissions);
        Ok(answer)
    }

    pub(crate) fn has_database_permission(
        &self,
        permission_manager: &PermissionManager,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<bool, PermissionCheckError> {
        let read_guard =
            self.permission_cache.database_permissions.read().expect("DatabasePermissions lock was poisoned");
        if let Some(cached_permissions) = read_guard.get(database) {
            return Ok(cached_permissions.contains(permission));
        }
        drop(read_guard);

        let database_permissions = permission_manager
            .list_database_permissions(&self.grantee(), database)
            .map_err(|typedb_source| PermissionCheckError::PermissionManagement { typedb_source })?;
        let answer = database_permissions.contains(permission);
        self.permission_cache
            .database_permissions
            .write()
            .expect("DatabasePermissions lock was poisoned")
            .insert(database.clone(), database_permissions);
        Ok(answer)
    }

    pub(crate) fn get_roles(
        &self,
        permission_manager: &PermissionManager,
        database: &DatabaseUid,
    ) -> Result<Arc<HashSet<SchemaDefinedRole>>, PermissionCheckError> {
        let read_guard = self.permission_cache.database_roles.read().expect("DatabasePermissions lock was poisoned");
        if let Some(cached_roles) = read_guard.get(database) {
            return Ok(cached_roles.clone());
        }
        drop(read_guard);

        let database_roles = Arc::new(
            permission_manager
                .list_database_roles(&self.grantee(), database)
                .map_err(|typedb_source| PermissionCheckError::PermissionManagement { typedb_source })?,
        );
        let answer = database_roles.clone();
        self.permission_cache
            .database_roles
            .write()
            .expect("DatabasePermissions lock was poisoned")
            .insert(database.clone(), database_roles);
        Ok(answer)
    }
}

#[derive(Debug)]
pub struct PermissionManager {
    transaction_util: TransactionUtil,
}

impl PermissionManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        Self { transaction_util: TransactionUtil::new(system_db.clone()) }
    }

    pub fn has_system_permission(
        &self,
        session: &UserSession,
        permission: &SystemPermission,
    ) -> Result<bool, PermissionCheckError> {
        session.has_system_permission(&self, permission)
    }

    pub fn has_database_permission(
        &self,
        session: &UserSession,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<bool, PermissionCheckError> {
        session.has_database_permission(&self, database, permission)
    }

    pub fn grant_system_permission(
        &self,
        grantee: &Grantee,
        granted_permission: &SystemPermission,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_system_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    granted_permission,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn grant_database_permission(
        &self,
        grantee: &Grantee,
        database: &DatabaseUid,
        granted_permission: &DatabasePermission,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_database_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    database,
                    granted_permission,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_system_permissions(
        &self,
        grantee: &Grantee,
    ) -> Result<HashSet<SystemPermission>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_system_permissions(tx, grantee))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn list_database_permissions(
        &self,
        grantee: &Grantee,
        database_id: &DatabaseUid,
    ) -> Result<HashSet<DatabasePermission>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_database_permissions(tx, grantee, database_id))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn grant_role(
        &self,
        grantee: &Grantee,
        granted_role: &SchemaDefinedRole,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_role(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    granted_role,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_database_roles(
        &self,
        grantee: &Grantee,
        database: &DatabaseUid,
    ) -> Result<HashSet<SchemaDefinedRole>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_database_roles(tx, grantee, database))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn revoke_system_permission(
        &self,
        grantee: &Grantee,
        permission: &SystemPermission,
    ) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_system_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    permission,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedSystemPermission {
                        grantee: grantee.clone(),
                        permission: permission.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn revoke_database_permission(
        &self,
        grantee: &Grantee,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_database_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    database,
                    permission,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedDatabasePermission {
                        grantee: grantee.clone(),
                        permission: permission.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn revoke_role(&self, grantee: &Grantee, role: &SchemaDefinedRole) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_role(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    role,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedRole {
                        grantee: grantee.clone(),
                        role: role.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }
}

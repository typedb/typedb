/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    collections::HashSet,
    fmt::Debug,
    sync::{Arc, RwLock as StdRwLock},
};

use async_trait::async_trait;
use database::{
    database::{DatabaseUid, SchemaLockedError},
    database_manager::DatabaseManager,
};
use system::concepts::{DatabasePermission, Grantee, SchemaDefinedRole, SystemPermission};
use user::{errors::PermissionManagementError, permission_manager::PermissionManager};

use crate::{
    authentication::Accessor,
    error::{ArcServerStateError, LocalServerStateError, PermissionRequirement, arc_server_state_err},
    system_init::SYSTEM_DB,
};

#[async_trait]
pub trait PermissionOperator: Debug + Send + Sync {
    #[must_use]
    async fn validate_self_or_has(
        &self,
        accessor: &Accessor,
        target: &str,
        permission: &SystemPermission,
    ) -> Result<(), ArcServerStateError>;

    #[must_use]
    async fn validate_system_permission(
        &self,
        accessor: &Accessor,
        permission: &SystemPermission,
    ) -> Result<(), ArcServerStateError>;

    #[must_use]
    async fn validate_database_permission(
        &self,
        accessor: &Accessor,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<(), ArcServerStateError>;

    async fn grant_system_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        permission: SystemPermission,
    ) -> Result<(), ArcServerStateError>;

    async fn list_system_permissions(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
    ) -> Result<HashSet<SystemPermission>, ArcServerStateError>;

    async fn revoke_system_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        permission: SystemPermission,
    ) -> Result<(), ArcServerStateError>;

    async fn grant_database_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        permission: DatabasePermission,
    ) -> Result<(), ArcServerStateError>;

    async fn list_database_permissions(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
    ) -> Result<HashSet<DatabasePermission>, ArcServerStateError>;

    async fn revoke_database_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        permission: DatabasePermission,
    ) -> Result<(), ArcServerStateError>;

    async fn grant_role(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        role: &str,
    ) -> Result<(), ArcServerStateError>;

    async fn list_database_roles(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
    ) -> Result<HashSet<SchemaDefinedRole>, ArcServerStateError>;

    async fn revoke_role(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        role: &str,
    ) -> Result<(), ArcServerStateError>;
}

#[derive(Debug)]
pub struct LocalPermissionOperator {
    database_manager: Arc<DatabaseManager>,
    permission_manager: StdRwLock<Option<Arc<PermissionManager>>>,
}

impl LocalPermissionOperator {
    pub fn new(database_manager: Arc<DatabaseManager>) -> Self {
        Self { database_manager, permission_manager: StdRwLock::new(None) }
    }

    fn try_load_system_managers(&self) {
        if let Some(system_db) = self.database_manager.database_unrestricted(SYSTEM_DB) {
            let permission_manager = Arc::new(PermissionManager::new(system_db));
            *self.permission_manager.write().unwrap() = Some(permission_manager);
        }
    }

    fn get_permission_manager(&self) -> Result<Arc<PermissionManager>, LocalServerStateError> {
        if let Some(pm) = self.permission_manager.read().unwrap().clone() {
            return Ok(pm);
        }
        self.try_load_system_managers();
        self.permission_manager.read().unwrap().clone().ok_or(LocalServerStateError::NotInitialised {})
    }

    fn resolve_database_for_permissions(&self, database: &str) -> Result<DatabaseUid, PermissionManagementError> {
        let db = self
            .database_manager
            .database(database)
            .ok_or_else(|| PermissionManagementError::DatabaseNotFound { database: database.to_owned() })?;
        Ok(db.database_id())
    }

    fn resolve_role(&self, database: &str, role_name: &str) -> Result<SchemaDefinedRole, PermissionManagementError> {
        let db = self
            .database_manager
            .database(database)
            .ok_or_else(|| PermissionManagementError::DatabaseNotFound { database: database.to_owned() })?;
        let rbac_cache = db.rbac_cache().map_err(|_: SchemaLockedError| {
            PermissionManagementError::DatabaseSchemaBusy { database: database.to_owned() }
        })?;
        let role = rbac_cache.get_rbac_role(role_name).ok_or_else(|| PermissionManagementError::RoleNotFound {
            database: database.to_owned(),
            role: role_name.to_owned(),
        })?;
        Ok(SchemaDefinedRole { database: db.database_id(), role_id: role.vertex().role_id() })
    }
}

#[async_trait]
impl PermissionOperator for LocalPermissionOperator {
    async fn validate_self_or_has(
        &self,
        accessor: &Accessor,
        target: &str,
        permission: &SystemPermission,
    ) -> Result<(), ArcServerStateError> {
        match accessor {
            Accessor::Session(session) if session.username() == target => Ok(()),
            _ => self.validate_system_permission(accessor, permission).await,
        }
    }

    async fn validate_system_permission(
        &self,
        accessor: &Accessor,
        permission: &SystemPermission,
    ) -> Result<(), ArcServerStateError> {
        let session = match accessor {
            Accessor::Default => return Ok(()),
            Accessor::Session(session) => session,
        };
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        let has_permission =
            permission_manager.has_system_permission(session, permission).map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::PermissionCheckError { typedb_source })
            })?;
        if has_permission {
            Ok(())
        } else {
            Err(arc_server_state_err(LocalServerStateError::PermissionDeniedGeneric {
                accessor: accessor.username().to_owned(),
                permission: PermissionRequirement::System(permission.clone()),
            }))
        }
    }

    async fn validate_database_permission(
        &self,
        accessor: &Accessor,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<(), ArcServerStateError> {
        let session = match accessor {
            Accessor::Default => return Ok(()),
            Accessor::Session(session) => session,
        };
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        let has_permission =
            permission_manager.has_database_permission(session, database, permission).map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::PermissionCheckError { typedb_source })
            })?;
        if has_permission {
            Ok(())
        } else {
            Err(arc_server_state_err(LocalServerStateError::PermissionDeniedGeneric {
                accessor: accessor.username().to_owned(),
                permission: PermissionRequirement::Database(permission.clone()),
            }))
        }
    }

    async fn grant_system_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        permission: SystemPermission,
    ) -> Result<(), ArcServerStateError> {
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.grant_system_permission(&grantee, &permission).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::SystemPermissionCannotBeGranted {
                typedb_source,
                grantee,
                permission,
            })
        })
    }

    async fn list_system_permissions(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
    ) -> Result<HashSet<SystemPermission>, ArcServerStateError> {
        match &grantee {
            Grantee::UserGroup(_) => {
                self.validate_system_permission(accessor, &SystemPermission::GrantPermissions).await?
            }
            Grantee::User(user) => {
                self.validate_self_or_has(accessor, user.name.as_str(), &SystemPermission::GrantPermissions).await?
            }
        }
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager
            .list_system_permissions(&grantee)
            .map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::SystemPermissionsCannotBeListed { typedb_source, grantee })
            })
            .map(|perms| perms.into_iter().map(SystemPermission::from).collect())
    }

    async fn revoke_system_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        permission: SystemPermission,
    ) -> Result<(), ArcServerStateError> {
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.revoke_system_permission(&grantee, &permission).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::SystemPermissionCannotBeRevoked {
                typedb_source,
                grantee,
                permission,
            })
        })
    }

    async fn grant_database_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        permission: DatabasePermission,
    ) -> Result<(), ArcServerStateError> {
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let database_id = self.resolve_database_for_permissions(database).map_err(|typedb_source| {
            LocalServerStateError::DatabasePermissionCannotBeGranted {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
                permission: permission.clone(),
            }
        })?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.grant_database_permission(&grantee, &database_id, &permission).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabasePermissionCannotBeGranted {
                typedb_source,
                grantee,
                database: database.to_owned(),
                permission,
            })
        })
    }

    async fn list_database_permissions(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
    ) -> Result<HashSet<DatabasePermission>, ArcServerStateError> {
        match &grantee {
            Grantee::UserGroup(_) => {
                self.validate_system_permission(accessor, &SystemPermission::GrantPermissions).await?
            }
            Grantee::User(user) => {
                self.validate_self_or_has(accessor, user.name.as_str(), &SystemPermission::GrantPermissions).await?
            }
        }
        let database_id = self.resolve_database_for_permissions(database).map_err(|typedb_source| {
            LocalServerStateError::DatabasePermissionsCannotBeListed {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
            }
        })?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager
            .list_database_permissions(&grantee, &database_id)
            .map_err(|typedb_source| {
                arc_server_state_err(LocalServerStateError::DatabasePermissionsCannotBeListed {
                    typedb_source,
                    grantee,
                    database: database.to_owned(),
                })
            })
            .map(|perms| perms.into_iter().map(DatabasePermission::from).collect())
    }

    async fn revoke_database_permission(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        permission: DatabasePermission,
    ) -> Result<(), ArcServerStateError> {
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let database_id = self.resolve_database_for_permissions(database).map_err(|typedb_source| {
            LocalServerStateError::DatabasePermissionCannotBeRevoked {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
                permission: permission.clone(),
            }
        })?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.revoke_database_permission(&grantee, &database_id, &permission).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::DatabasePermissionCannotBeRevoked {
                typedb_source,
                grantee,
                database: database.to_owned(),
                permission,
            })
        })
    }

    async fn grant_role(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        role: &str,
    ) -> Result<(), ArcServerStateError> {
        // TODO: must introduce per-database grant-role permission
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let map_err = |typedb_source| {
            arc_server_state_err(LocalServerStateError::RoleCannotBeGranted {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
                role: role.to_owned(),
            })
        };
        let resolved_role = self.resolve_role(database, role).map_err(map_err)?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.grant_role(&grantee, &resolved_role).map_err(map_err)?;
        Ok(())
    }

    async fn revoke_role(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
        role: &str,
    ) -> Result<(), ArcServerStateError> {
        // TODO: must introduce per-database grant-role permission
        self.validate_system_permission(&accessor, &SystemPermission::GrantPermissions).await?;
        let map_err = |typedb_source| {
            arc_server_state_err(LocalServerStateError::RoleCannotBeRevoked {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
                role: role.to_owned(),
            })
        };
        let resolved_role = self.resolve_role(database, role).map_err(map_err)?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.revoke_role(&grantee, &resolved_role).map_err(map_err)?;
        Ok(())
    }

    async fn list_database_roles(
        &self,
        accessor: &Accessor,
        grantee: Grantee,
        database: &str,
    ) -> Result<HashSet<SchemaDefinedRole>, ArcServerStateError> {
        match &grantee {
            Grantee::UserGroup(_) => {
                self.validate_system_permission(accessor, &SystemPermission::GrantPermissions).await?
            }
            Grantee::User(user) => {
                self.validate_self_or_has(accessor, user.name.as_str(), &SystemPermission::GrantPermissions).await?
            }
        }
        let database_id = self.resolve_database_for_permissions(database).map_err(|typedb_source| {
            LocalServerStateError::RolesCannotBeListed {
                typedb_source,
                grantee: grantee.clone(),
                database: database.to_owned(),
            }
        })?;
        let permission_manager = self.get_permission_manager().map_err(arc_server_state_err)?;
        permission_manager.list_database_roles(&grantee, &database_id).map_err(|typedb_source| {
            arc_server_state_err(LocalServerStateError::RolesCannotBeListed {
                typedb_source,
                grantee,
                database: database.to_owned(),
            })
        })
    }
}

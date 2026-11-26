/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use error::typedb_error;
use query::error::QueryError;

pub const SCHEMA: &str = include_str!("schema.tql");

fn is_valid_typeql_value(value: &str) -> bool {
    typeql::common::identifier::is_valid_label(value)
}

pub struct WrittenRowCount(usize);

impl std::ops::Deref for WrittenRowCount {
    type Target = usize;
    fn deref(&self) -> &usize {
        &self.0
    }
}

typedb_error! {
    pub SystemDBError(component = "System database", prefix = "SDB") {
        EmptyUpdate(1, "There is nothing to update"),
        IllegalQueryInput(2, "The specified input contains one or more illegal character(s)"),
        QueryFailed(3, "System database query could not be executed. The system database may not be fully initialised yet.", typedb_source: Box<QueryError>),
        UnexpectedParseFailed(4, "Parsing a system database query failed unexpectedly", typedb_source: typeql::Error),
    }
}

pub mod user_repository {
    use std::{collections::HashMap, sync::Arc};

    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager, type_::type_manager::TypeManager};
    use database::transaction::TransactionRead;
    use function::function_manager::FunctionManager;
    use query::query_manager::QueryManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use thing_manager::ThingManager;
    use typeql::parse_query;
    use uuid::Uuid;

    use crate::{
        concepts::{Credential, PasswordHash, User},
        repositories::{SystemDBError, is_valid_typeql_value},
        util::{
            answer_util::get_string,
            query_util::{execute_read_pipeline, execute_write_pipeline},
        },
    };

    pub fn list(tx: TransactionRead<WALClient>) -> Vec<User> {
        let unexpected_error_msg = "An unexpected error occurred when acquiring the list of users";
        let query_str = "match (user: $u, credentials: $c) isa user-credentials; $u has name $n;";
        let query = parse_query(query_str).expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), query_str);
        let rows = result.expect(unexpected_error_msg);
        let users = rows.iter().map(|row| User::new(get_string(&tx, row, "n"))).collect();
        users
    }

    pub fn get(tx: TransactionRead<WALClient>, username: &str) -> Result<Option<(User, Credential)>, SystemDBError> {
        if !is_valid_typeql_value(username) {
            return Err(SystemDBError::IllegalQueryInput {});
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to retrieve a user";
        let query_str = format!(
            "match
                (user: $u, credentials: $p) isa user-credentials;
                $u has name '{username}';
                $p has hash $h;"
        );
        let query = parse_query(&query_str).expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_str);
        let mut rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(typedb_source) => return Err(SystemDBError::QueryFailed { typedb_source }),
        };
        if !rows.is_empty() {
            let row = rows.pop().expect(unexpected_error_msg);
            let hash = get_string(&tx, &row, "h");
            Ok(Some((
                User::new(username.to_string()),
                Credential::PasswordType { password_hash: PasswordHash::new(hash) },
            )))
        } else {
            Ok(None)
        }
    }

    pub fn create(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        user: &User,
        credentials: &Credential,
        user_uuid: Uuid,
        credential_uuid: Uuid,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(&user.name) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to create a new user";
        let (query, query_string) = match credentials {
            Credential::PasswordType { password_hash: PasswordHash { value: hash } } => {
                let query_string = format!(
                    "insert $u isa user, has uuid '{user_uuid}', has name '{name}';
                        $p isa password, has uuid '{cred_uuid}', has hash '{hash}';
                        (user: $u, credentials: $p) isa user-credentials;",
                    user_uuid = user_uuid,
                    cred_uuid = credential_uuid,
                    name = user.name,
                    hash = hash
                );
                (parse_query(query_string.as_str()).expect(unexpected_error_msg), query_string)
            }
        };
        let (_, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            &query_string,
        );
        (Ok(()), snapshot)
    }

    // TODO: support updating user's username
    pub fn update(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        username: &str,
        _user: &Option<User>,
        credential: &Option<Credential>,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let unexpected_error_msg = "An unexpected error occurred when attempting to update a user";
        match credential {
            Some(Credential::PasswordType { password_hash: PasswordHash { value: hash } }) => {
                let query_string = format!(
                    "match (user: $u, credentials: $p) isa user-credentials; $u has name '{username}'; $p has hash $h; delete has $h of $p; insert $p has hash '{password_hash}';",
                    username = username,
                    password_hash = hash
                );
                let query = parse_query(&query_string).expect(unexpected_error_msg);
                let (_, snapshot) = execute_write_pipeline(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    query.into_structure().into_pipeline(),
                    &query_string,
                );
                (Ok(()), snapshot)
            }
            None => (Err(SystemDBError::EmptyUpdate {}), Arc::new(snapshot)),
        }
    }

    pub fn delete(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        username: &str,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(username) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to delete a user";
        let query_string = format!(
            "match $uc isa user-credentials, links (user: $u, credentials: $c);
                $u isa user, has name '{username}';
                delete $u; $c; $uc;",
            username = username
        );
        let query = parse_query(&query_string).expect(unexpected_error_msg);
        let (result, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            &query_string,
        );
        match result {
            Ok(_) => (Ok(()), snapshot),
            Err(typedb_source) => (Err(SystemDBError::QueryFailed { typedb_source }), snapshot),
        }
    }
}

pub mod rbac_repository {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
    use database::{database::DatabaseUid, transaction::TransactionRead};
    use encoding::graph::rbac::RBACRoleId;
    use function::function_manager::FunctionManager;
    use query::query_manager::QueryManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use typeql::parse_query;

    use crate::{
        concepts::{DatabasePermission, Grantee, SchemaDefinedRole, SystemPermission},
        repositories::{SystemDBError, WrittenRowCount, is_valid_typeql_value},
        util::query_util::{execute_read_pipeline, execute_write_pipeline},
    };

    const GRANTEE_NAME_VALUE_VARIABLE: &'static str = "grantee-name-value";
    const DATABASE_ID_VALUE_VARIABLE: &'static str = "database-id-value";
    const PERMISSION_LABEL_VALUE_VARIABLE: &'static str = "permission-type-label";

    const GRANTEE_TYPE_VARIABLE: &'static str = "grantee-type";
    const PERMISSION_TYPE_VARIABLE: &'static str = "permission-type";

    const GRANTEE_VARIABLE: &'static str = "grantee";
    const GRANTED_PERMISSION_VARIABLE: &'static str = "granted-permission";

    const GRANTED_ROLE_VARIABLE: &'static str = "granted-role";
    const ROLE_ID_VALUE_VARIABLE: &'static str = "role-id-value";

    pub fn grant_system_permission(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        granted_permission: &SystemPermission,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_grant_system_permission_query(grantee, granted_permission) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        execute_write_permission_query(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query_string,
        )
    }

    pub fn grant_database_permission(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        database: &DatabaseUid,
        granted_permission: &DatabasePermission,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_grant_database_permission_query(grantee, database, granted_permission) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        execute_write_permission_query(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query_string,
        )
    }

    pub fn list_system_permissions(
        tx: TransactionRead<WALClient>,
        grantee: &Grantee,
    ) -> Result<HashSet<SystemPermission>, SystemDBError> {
        let query_string = build_list_system_permissions_query(grantee)?;
        let query =
            parse_query(query_string.as_str()).expect("An unexpected error occurred while listing user permissions");
        let (_tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_string);
        let rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(typedb_source) => return Err(SystemDBError::QueryFailed { typedb_source }),
        };
        let permissions = rows
            .into_iter()
            .filter_map(|row| {
                let entry = row.get(PERMISSION_LABEL_VALUE_VARIABLE).expect("Expected permission label value");
                SystemPermission::try_from_typeql_label(entry.as_value().unwrap_string_ref())
            })
            .collect();
        Ok(permissions)
    }

    pub fn list_database_permissions(
        tx: TransactionRead<WALClient>,
        grantee: &Grantee,
        database_id: &DatabaseUid,
    ) -> Result<HashSet<DatabasePermission>, SystemDBError> {
        let query_string = build_list_database_permissions_query(grantee, database_id)?;
        let query =
            parse_query(query_string.as_str()).expect("An unexpected error occurred while listing user permissions");
        let (_tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_string);
        let rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(typedb_source) => return Err(SystemDBError::QueryFailed { typedb_source }),
        };
        let permissions = rows
            .into_iter()
            .filter_map(|row| {
                let entry = row.get(PERMISSION_LABEL_VALUE_VARIABLE).expect("Expected permission label value");
                DatabasePermission::try_from_typeql_label(entry.as_value().unwrap_string_ref())
            })
            .collect();
        Ok(permissions)
    }

    pub fn grant_role(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        role: &SchemaDefinedRole,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_grant_role_query(grantee, role) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        let query = match parse_query(query_string.as_str()) {
            Ok(query) => query,
            Err(typedb_source) => {
                return (Err(SystemDBError::UnexpectedParseFailed { typedb_source }), Arc::new(snapshot));
            }
        };
        let (result, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            query_string.as_str(),
        );
        match result {
            Ok(rows) => (Ok(WrittenRowCount(rows.len())), snapshot),
            Err(typedb_source) => (Err(SystemDBError::QueryFailed { typedb_source }), snapshot),
        }
    }

    pub fn list_database_roles(
        tx: TransactionRead<WALClient>,
        grantee: &Grantee,
        database: &DatabaseUid,
    ) -> Result<HashSet<SchemaDefinedRole>, SystemDBError> {
        let unexpected_error_msg = "An unexpected error occurred while listing user roles";
        let query_string = build_list_roles_query(grantee, database)?;
        let query = parse_query(query_string.as_str()).expect(unexpected_error_msg);
        let (_tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_string);
        let rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(typedb_source) => return Err(SystemDBError::QueryFailed { typedb_source }),
        };
        let roles = rows.into_iter().filter_map(|row| try_valid_granted_role_from_row(row)).collect();
        Ok(roles)
    }

    pub fn revoke_system_permission(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        permission: &SystemPermission,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_revoke_system_permission_query(grantee, permission) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        execute_write_permission_query(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query_string,
        )
    }

    pub fn revoke_database_permission(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_revoke_database_permission_query(grantee, database, permission) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        execute_write_permission_query(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query_string,
        )
    }

    pub fn revoke_role(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        role: &SchemaDefinedRole,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_revoke_role_query(grantee, role) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
        let query = match parse_query(query_string.as_str()) {
            Ok(query) => query,
            Err(typedb_source) => {
                return (Err(SystemDBError::UnexpectedParseFailed { typedb_source }), Arc::new(snapshot));
            }
        };
        let (result, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            query_string.as_str(),
        );
        match result {
            Ok(rows) => (Ok(WrittenRowCount(rows.len())), snapshot),
            Err(typedb_source) => (Err(SystemDBError::QueryFailed { typedb_source }), snapshot),
        }
    }

    // Shared write helper
    fn execute_write_permission_query(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        query_string: String,
    ) -> (Result<WrittenRowCount, SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query = match parse_query(query_string.as_str()) {
            Ok(query) => query,
            Err(typedb_source) => {
                return (Err(SystemDBError::UnexpectedParseFailed { typedb_source }), Arc::new(snapshot));
            }
        };
        let (result, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            query_string.as_str(),
        );
        match result {
            Ok(rows) => (Ok(WrittenRowCount(rows.len())), snapshot),
            Err(typedb_source) => (Err(SystemDBError::QueryFailed { typedb_source }), snapshot),
        }
    }

    // Query builders
    fn build_list_system_permissions_query(grantee: &Grantee) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let query_string = format!(
            r#"
            match
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
                ${PERMISSION_TYPE_VARIABLE} sub system-permission;
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE},
                    links (grantee: ${GRANTEE_VARIABLE});
            match
                let ${PERMISSION_LABEL_VALUE_VARIABLE} = label(${PERMISSION_TYPE_VARIABLE});
                select ${PERMISSION_LABEL_VALUE_VARIABLE};
            "#,
        );
        Ok(query_string)
    }

    fn build_list_database_permissions_query(
        grantee: &Grantee,
        database: &DatabaseUid,
    ) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id_value = database.0;
        let query_string = format!(
            r#"
            match
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id_value};
            match
                # grantee
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};

                # permission type
                ${PERMISSION_TYPE_VARIABLE} sub database-permission;
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE},
                    links (grantee: ${GRANTEE_VARIABLE}),
                    has database-id == ${DATABASE_ID_VALUE_VARIABLE};
            match
                let ${PERMISSION_LABEL_VALUE_VARIABLE} = label(${PERMISSION_TYPE_VARIABLE});
            select ${PERMISSION_LABEL_VALUE_VARIABLE}, ${DATABASE_ID_VALUE_VARIABLE};
            "#,
        );
        Ok(query_string)
    }

    fn build_grant_system_permission_query(
        grantee: &Grantee,
        granted_permission: &SystemPermission,
    ) -> Result<String, SystemDBError> {
        let permission_type = granted_permission.to_typeql_label();
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        Ok(format!(
            r#"
            match
                ${PERMISSION_TYPE_VARIABLE} label {permission_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
            put
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
        "#
        ))
    }

    fn build_grant_database_permission_query(
        grantee: &Grantee,
        database: &DatabaseUid,
        granted_permission: &DatabasePermission,
    ) -> Result<String, SystemDBError> {
        let permission_type = granted_permission.type_to_typeql_string();
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id_value = database.0;
        Ok(format!(
            r#"
            match
                ${PERMISSION_TYPE_VARIABLE} label {permission_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id_value};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
            put
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
                ${GRANTED_PERMISSION_VARIABLE} has database-id == ${DATABASE_ID_VALUE_VARIABLE};
        "#
        ))
    }

    fn build_list_roles_query(grantee: &Grantee, database: &DatabaseUid) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id_value = database.0;
        let query_string = format!(
            r#"
            match
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id_value};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
                ${GRANTED_ROLE_VARIABLE} isa! role-assignment,
                    links (grantee: ${GRANTEE_VARIABLE}),
                    has database-id == ${DATABASE_ID_VALUE_VARIABLE},
                    has role-id $role-id;
            match
                let ${ROLE_ID_VALUE_VARIABLE} = $role-id;
            select ${DATABASE_ID_VALUE_VARIABLE}, ${ROLE_ID_VALUE_VARIABLE};
            "#,
        );
        Ok(query_string)
    }

    fn build_grant_role_query(grantee: &Grantee, granted_role: &SchemaDefinedRole) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id = &granted_role.database.0;
        let role_id = &granted_role.role_id.to_system_db_id();
        let query_string = format!(
            r#"
            match
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id};
                let ${ROLE_ID_VALUE_VARIABLE} = {role_id};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
            put
               ${GRANTED_ROLE_VARIABLE} isa role-assignment,
                    links (grantee: ${GRANTEE_VARIABLE}),
                    has database-id == ${DATABASE_ID_VALUE_VARIABLE},
                    has role-id == ${ROLE_ID_VALUE_VARIABLE};
        "#
        );
        Ok(query_string)
    }

    fn build_revoke_system_permission_query(
        grantee: &Grantee,
        permission: &SystemPermission,
    ) -> Result<String, SystemDBError> {
        let permission_type = permission.to_typeql_label();
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        Ok(format!(
            r#"
            match
                ${PERMISSION_TYPE_VARIABLE} label {permission_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
            delete
                ${GRANTED_PERMISSION_VARIABLE};
        "#
        ))
    }

    fn build_revoke_database_permission_query(
        grantee: &Grantee,
        database: &DatabaseUid,
        permission: &DatabasePermission,
    ) -> Result<String, SystemDBError> {
        let permission_type = permission.type_to_typeql_string();
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id_value = database.0;
        Ok(format!(
            r#"
            match
                ${PERMISSION_TYPE_VARIABLE} label {permission_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id_value};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
                ${GRANTED_PERMISSION_VARIABLE} has database-id == ${DATABASE_ID_VALUE_VARIABLE};
            delete
                ${GRANTED_PERMISSION_VARIABLE};
        "#
        ))
    }

    fn build_revoke_role_query(grantee: &Grantee, role: &SchemaDefinedRole) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let database_id = &role.database.0;
        let role_id = &role.role_id.to_system_db_id();
        let query_string = format!(
            r#"
            match
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                let ${DATABASE_ID_VALUE_VARIABLE} = {database_id};
                let ${ROLE_ID_VALUE_VARIABLE} = {role_id};
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
                ${GRANTED_ROLE_VARIABLE} isa role-assignment,
                    links (grantee: ${GRANTEE_VARIABLE}),
                    has database-id == ${DATABASE_ID_VALUE_VARIABLE},
                    has role-id == ${ROLE_ID_VALUE_VARIABLE};
            delete
                ${GRANTED_ROLE_VARIABLE};
        "#
        );
        Ok(query_string)
    }

    // Value processing helpers
    fn check_valid_value<T: AsRef<str>>(value: T) -> Result<T, SystemDBError> {
        match is_valid_typeql_value(value.as_ref()) {
            true => Ok(value),
            false => Err(SystemDBError::IllegalQueryInput {}),
        }
    }

    fn extract_database_value(database_id_value: &VariableValue<'_>) -> DatabaseUid {
        DatabaseUid(database_id_value.as_value().to_owned().unwrap_integer() as u64)
    }

    fn extract_role_value(role_id_value: &VariableValue<'_>) -> RBACRoleId {
        RBACRoleId::from_system_db_id(role_id_value.as_value().to_owned().unwrap_integer())
    }

    fn try_valid_granted_role_from_row(row: HashMap<String, VariableValue>) -> Option<SchemaDefinedRole> {
        let database_id_value = row.get(DATABASE_ID_VALUE_VARIABLE).expect("Expected database id value");
        let role_id_value = row.get(ROLE_ID_VALUE_VARIABLE).expect("Expected database id value");

        let database = extract_database_value(database_id_value);
        let role = extract_role_value(role_id_value);
        Some(SchemaDefinedRole { database, role_id: role })
    }
}

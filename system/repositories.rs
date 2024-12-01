/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub const SCHEMA: &str = include_str!("schema.tql");

pub mod user_repository {
    use std::{collections::HashMap, sync::Arc};
    use typeql::common::identifier::is_valid_identifier;
    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager, type_::type_manager::TypeManager};
    use database::transaction::{TransactionRead, TransactionWrite};
    use function::function_manager::FunctionManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use thing_manager::ThingManager;
    use typeql::parse_query;
    use uuid::Uuid;
    use query::query_manager::QueryManager;
    use crate::{
        concepts::{Credential, PasswordHash, User},
        util::{
            answer_util::get_string,
            query_util::{execute_read_pipeline, execute_write_pipeline},
        },
    };
    use error::typedb_error;

    pub fn list(tx: TransactionRead<WALClient>) -> Vec<User> {
        let unexpected_error_msg = "An unexpected error occurred when acquiring the list of users";
        let query = parse_query("match (user: $u, credentials: $p) isa user-credentials; $u has name $n;")
            .expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, &query.into_pipeline());
        let rows = result.expect(unexpected_error_msg);
        let users = rows.iter().map(|row| User::new(get_string(&tx, &row, "n"))).collect();
        users
    }

    pub fn get(tx: TransactionRead<WALClient>, username: &str) -> Result<Option<(User, Credential)>, SystemDBError> {
        if !is_valid_typeql_value(username) {
            return Err(SystemDBError::IllegalQueryInput {});
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to retrieve a user";
        let query = parse_query(
            format!(
                "match
                (user: $u, credentials: $p) isa user-credentials;
                $u has name '{username}';
                $p has hash $h;"
            )
            .as_str(),
        )
        .expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, &query.into_pipeline());
        let mut rows: Vec<HashMap<String, VariableValue>> = result.expect(unexpected_error_msg);
        if !rows.is_empty() {
            let row = rows.pop().expect(unexpected_error_msg);
            let hash = get_string(&tx, &row, "h");
            Ok(Some((User::new(username.to_string()), Credential::PasswordType { password_hash: PasswordHash::new(hash) })))
        } else {
            Ok(None)
        }
    }

    pub fn create(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query_manager: &QueryManager,
        user: &User,
        credentials: &Credential,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(&user.name) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to create a new user";
        let query = match credentials {
            Credential::PasswordType { password_hash: PasswordHash { value: hash } } => {
                let uuid = Uuid::new_v4().to_string();
                parse_query(
                    format!(
                        "insert $u isa user, has uuid '{uuid}', has name '{name}';
                        $p isa password, has hash '{hash}';
                        (user: $u, credentials: $p) isa user-credentials;",
                        uuid = uuid,
                        name = user.name,
                        hash = hash
                    )
                    .as_str(),
                )
                .expect(unexpected_error_msg)
            }
        };
        let (_, snapshot) =
            execute_write_pipeline(snapshot, type_manager, thing_manager, function_manager, query_manager, &query.into_pipeline());
        (Ok(()), snapshot)
    }

    pub fn update(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        username: &str,
        user: &Option<User>,
        credentials: &Option<Credential>,
    ) -> Arc<WriteSnapshot<WALClient>> {
        if user.is_some() {
            todo!("update user detail")
        }

        if credentials.is_some() {
            todo!("update credentials detail")
        }

        todo!()
    }

    pub fn delete(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        query_manager: &QueryManager,
        username: &str,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(username) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to delete a user";
        let query = parse_query(
            format!(
                "match $uc isa user-credentials, links (user: $u, credentials: $p);
                $u isa user, has name '{username}';
                delete $u; $p; $uc;",
                username = username
            )
            .as_str(),
        )
        .expect(unexpected_error_msg);
        let (_, snapshot) =
            execute_write_pipeline(snapshot, type_manager, thing_manager, function_manager, query_manager, &query.into_pipeline());
        (Ok(()), snapshot)
    }

    pub fn is_valid_typeql_value(value: &str) -> bool {
        is_valid_identifier(value)
    }

    typedb_error!(
        pub SystemDBError(component = "System database", prefix = "SDB") {
            IllegalQueryInput(1, "The specified input contains one or more illegal character(s)"),
        }
    );
}

// TODO: read from a file at compile time
pub const SCHEMA: &str = "
define
    attribute name value string;
    attribute uuid value string;
    attribute hash value string;

    entity user,
        owns uuid @card(1),
        owns name @card(1),
        plays user-password:user;

    entity password,
        owns hash @card(1),
        plays user-password:password;

    relation user-password,
        relates user @card(1),
        relates password @card(1);
";

pub mod user_repository {
    use std::{collections::HashMap, sync::Arc};

    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager, type_::type_manager::TypeManager};
    use database::transaction::{TransactionRead, TransactionWrite};
    use function::function_manager::FunctionManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use thing_manager::ThingManager;
    use typeql::parse_query;
    use uuid::Uuid;

    use crate::{
        concepts::{Credential, PasswordHash, User},
        util::{
            answer_util::get_string,
            query_util::{execute_read_pipeline, execute_write_pipeline},
        },
    };

    pub fn list(tx: TransactionRead<WALClient>) -> Vec<User> {
        let unexpected_error_msg = "An unexpected error occurred when acquiring the list of users";
        let query = parse_query("match (user: $u, password: $p) isa user-password; $u has name $n;")
            .expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, &query.into_pipeline());
        let rows = result.expect(unexpected_error_msg);
        let users = rows.iter().map(|row| User::new(get_string(&tx, &row, "n"))).collect();
        users
    }

    pub fn get(tx: TransactionRead<WALClient>, username: &str) -> Option<(User, Credential)> {
        let unexpected_error_msg = "An unexpected error occurred when attempting to retrieve a user";
        let query = parse_query(
            format!(
                "match
                (user: $u, password: $p) isa user-password;
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
            Some((User::new(username.to_string()), Credential::PasswordType { password_hash: PasswordHash::new(hash) }))
        } else {
            None
        }
    }

    pub fn create(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        user: &User,
        credential: &Credential,
    ) -> Arc<WriteSnapshot<WALClient>> {
        let unexpected_error_msg = "An unexpected error occurred when attempting to create a new user";
        let query = match credential {
            Credential::PasswordType { password_hash: PasswordHash { value: hash } } => {
                let uuid = Uuid::new_v4().to_string();
                parse_query(
                    format!(
                        "insert $u isa user, has uuid '{uuid}', has name '{name}';
                        $p isa password, has hash '{hash}';
                        (user: $u, password: $p) isa user-password;",
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
            execute_write_pipeline(snapshot, type_manager, thing_manager, function_manager, &query.into_pipeline());
        snapshot
    }

    pub fn update(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        username: &str,
        user: &Option<User>,
        credential: &Option<Credential>,
    ) -> Arc<WriteSnapshot<WALClient>> {
        if user.is_some() {
            todo!("update user detail")
        }

        if credential.is_some() {
            todo!("update credential detail")
        }

        todo!()
    }

    pub fn delete(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        username: &str,
    ) -> Arc<WriteSnapshot<WALClient>> {
        let unexpected_error_msg = "An unexpected error occurred when attempting to delete a user";
        let query = parse_query(
            format!(
                "match $up isa user-password, links (user: $u, password: $p);
                $u isa user, has name '{username}';
                delete $u; $p; $up;",
                username = username
            )
            .as_str(),
        )
        .expect(unexpected_error_msg);
        let (_, snapshot) =
            execute_write_pipeline(snapshot, type_manager, thing_manager, function_manager, &query.into_pipeline());
        snapshot
    }
}

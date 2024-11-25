pub mod user_repository {
    use crate::concepts::{Credential, PasswordHash, User};
    use crate::util::query_util::{execute_read_pipeline, execute_write_pipeline};
    use answer::variable_value::VariableValue;
    use concept::thing::thing_manager;
    use concept::type_::type_manager::TypeManager;
    use database::transaction::{TransactionRead, TransactionWrite};
    use function::function_manager::FunctionManager;
    use std::collections::HashMap;
    use std::sync::Arc;
    use storage::durability_client::WALClient;
    use storage::snapshot::WriteSnapshot;
    use thing_manager::ThingManager;
    use typeql::parse_query;
    use uuid::Uuid;
    use crate::util::answer_util::get_string;

    pub fn list(tx: TransactionRead<WALClient>) -> Vec<User> {
        let query = parse_query(
            "match (user: $u, password: $p) isa user-password; $u has name $n;"
        ).unwrap();
        let (tx, result) =
            execute_read_pipeline(tx, &query.into_pipeline());
        let rows = result.unwrap();
        let mut users: Vec<User> = vec![];
        for row in rows {
            let name  = get_string(&tx, &row, "n");
            users.push(User::new(name))
        }
        users
    }

    pub fn get(tx: TransactionRead<WALClient>, username: &str) -> Option<(User, Credential)> {
        let query = parse_query(
            format!(
                "match
                (user: $u, password: $p) isa user-password;
                $u has name '{username}';
                $p has hash $h",
                username = username
            ).as_str()
        ).unwrap();
        let (tx, result) =
            execute_read_pipeline(tx, &query.into_pipeline());
        let mut rows: Vec<HashMap<String, VariableValue>> = result.unwrap();
        if !rows.is_empty() {
            let row = rows.pop().unwrap();
            let hash = get_string(&tx, &row, "h");
            Some((
                User::new(username.to_string()),
                Credential::PasswordType { password_hash: PasswordHash::new(hash)}
            ))
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
        credential: &Credential
    ) -> Arc<WriteSnapshot<WALClient>> {
        let query = match credential {
            Credential::PasswordType { password_hash: PasswordHash { value: hash}} => {
                let uuid = Uuid::new_v4().to_string();
                parse_query(
                    format!(
                        "insert $u isa user, has uuid '{uuid}', has name '{name}';
                        $p isa password, has hash '{hash}';
                        (user: $u, password: $p) isa user-password;",
                        uuid = uuid, name = user.name, hash = hash
                    ).as_str()
                ).unwrap()
            }
        };
        let (_, snapshot) = execute_write_pipeline(
            snapshot, type_manager, thing_manager, function_manager, &query.into_pipeline()
        );
        snapshot
    }

    pub fn update(
        tx: &TransactionWrite<WALClient>, username: &str, update: &User
    ) {
        todo!()
    }

    pub fn delete(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        username: &str
    ) -> Arc<WriteSnapshot<WALClient>>{
        let query = parse_query(
            format!(
                "match $up isa user-password, links (user: $u, password: $p);
                $u isa user, has name {username};
                delete $u; $p; $up;",
                username = username
            ).as_str()
        ).unwrap();
        let (_, snapshot) = execute_write_pipeline(
            snapshot, type_manager, thing_manager, function_manager, &query.into_pipeline()
        );
        snapshot
    }
}

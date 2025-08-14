/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub(crate) mod connection {
    use std::time::Instant;

    use crate::service::grpc::ConnectionID;

    pub(crate) fn connection_open_res(
        connection_id: ConnectionID,
        receive_time: Instant,
        servers_all_res: typedb_protocol::server_manager::all::Res,
        token_create_res: typedb_protocol::authentication::token::create::Res,
    ) -> typedb_protocol::connection::open::Res {
        let processing_millis = Instant::now().duration_since(receive_time).as_millis();
        typedb_protocol::connection::open::Res {
            connection_id: Some(typedb_protocol::ConnectionId { id: Vec::from(connection_id) }),
            server_duration_millis: processing_millis as u64,
            servers_all: Some(servers_all_res),
            authentication: Some(token_create_res),
        }
    }
}

pub(crate) mod authentication {
    pub(crate) fn token_create_res(token: String) -> typedb_protocol::authentication::token::create::Res {
        typedb_protocol::authentication::token::create::Res { token }
    }
}

pub(crate) mod server_manager {
    pub(crate) fn servers_all_res(servers: Vec<typedb_protocol::Server>) -> typedb_protocol::server_manager::all::Res {
        typedb_protocol::server_manager::all::Res { servers }
    }

    pub(crate) fn servers_get_res(server: typedb_protocol::Server) -> typedb_protocol::server_manager::get::Res {
        typedb_protocol::server_manager::get::Res { server: Some(server) }
    }
}

pub(crate) mod server {
    use resource::distribution_info::DistributionInfo;

    pub(crate) fn server_version_res(distribution_info: DistributionInfo) -> typedb_protocol::server::version::Res {
        typedb_protocol::server::version::Res {
            distribution: distribution_info.distribution.to_string(),
            version: distribution_info.version.to_string(), // todo
        }
    }

    pub(crate) fn servers_register_res() -> typedb_protocol::server_manager::register::Res {
        typedb_protocol::server_manager::register::Res {}
    }

    pub(crate) fn servers_deregister_res() -> typedb_protocol::server_manager::deregister::Res {
        typedb_protocol::server_manager::deregister::Res {}
    }
}

pub(crate) mod database_manager {
    fn database(name: String) -> typedb_protocol::Database {
        typedb_protocol::Database { name }
    }

    pub(crate) fn database_get_res(database_name: String) -> typedb_protocol::database_manager::get::Res {
        typedb_protocol::database_manager::get::Res { database: Some(database(database_name)) }
    }

    pub(crate) fn database_all_res(database_names: Vec<String>) -> typedb_protocol::database_manager::all::Res {
        typedb_protocol::database_manager::all::Res {
            databases: database_names.into_iter().map(|name| database(name)).collect(),
        }
    }

    pub(crate) fn database_contains_res(contains: bool) -> typedb_protocol::database_manager::contains::Res {
        typedb_protocol::database_manager::contains::Res { contains }
    }

    pub(crate) fn database_create_res(name: String) -> typedb_protocol::database_manager::create::Res {
        typedb_protocol::database_manager::create::Res { database: Some(database(name)) }
    }

    pub(crate) fn database_import_res_done() -> typedb_protocol::database_manager::import::Server {
        typedb_protocol::database_manager::import::Server {
            server: Some(typedb_protocol::migration::import::Server {
                done: Some(typedb_protocol::migration::import::server::Done {}),
            }),
        }
    }
}

pub(crate) mod database {
    pub(crate) fn database_delete_res() -> typedb_protocol::database::delete::Res {
        typedb_protocol::database::delete::Res {}
    }

    pub(crate) fn database_schema_res(schema: String) -> typedb_protocol::database::schema::Res {
        typedb_protocol::database::schema::Res { schema }
    }

    pub(crate) fn database_type_schema_res(schema: String) -> typedb_protocol::database::type_schema::Res {
        typedb_protocol::database::type_schema::Res { schema }
    }

    pub(crate) fn database_export_initial_res_ok(schema: String) -> typedb_protocol::database::export::Server {
        let message = typedb_protocol::migration::export::InitialRes { schema };
        database_export_server_initial_res(message)
    }

    pub(crate) fn database_export_res_part_items(
        items: Vec<typedb_protocol::migration::Item>,
    ) -> typedb_protocol::database::export::Server {
        let message = typedb_protocol::migration::export::ResPart { items };
        database_export_server_res_part(message)
    }

    pub(crate) fn database_export_res_done() -> typedb_protocol::database::export::Server {
        let message = typedb_protocol::migration::export::Done {};
        database_export_server_done(message)
    }

    #[inline]
    fn database_export_server_initial_res(
        message: typedb_protocol::migration::export::InitialRes,
    ) -> typedb_protocol::database::export::Server {
        database_export_server_res(typedb_protocol::migration::export::server::Server::InitialRes(message))
    }

    #[inline]
    fn database_export_server_res_part(
        message: typedb_protocol::migration::export::ResPart,
    ) -> typedb_protocol::database::export::Server {
        database_export_server_res(typedb_protocol::migration::export::server::Server::ResPart(message))
    }

    #[inline]
    fn database_export_server_done(
        message: typedb_protocol::migration::export::Done,
    ) -> typedb_protocol::database::export::Server {
        database_export_server_res(typedb_protocol::migration::export::server::Server::Done(message))
    }

    #[inline]
    fn database_export_server_res(
        message: typedb_protocol::migration::export::server::Server,
    ) -> typedb_protocol::database::export::Server {
        typedb_protocol::database::export::Server {
            server: Some(typedb_protocol::migration::export::Server { server: Some(message) }),
        }
    }
}

pub(crate) mod transaction {
    use uuid::Uuid;

    pub(crate) fn transaction_open_res(
        req_id: Uuid,
        server_processing_millis: u64,
    ) -> typedb_protocol::transaction::Server {
        let message = typedb_protocol::transaction::res::Res::OpenRes(typedb_protocol::transaction::open::Res {
            server_duration_millis: server_processing_millis,
        });
        transaction_server_res(req_id, message)
    }

    pub(crate) fn query_res_ok_done(
        query_type: typedb_protocol::query::Type,
    ) -> typedb_protocol::query::initial_res::ok::Ok {
        typedb_protocol::query::initial_res::ok::Ok::Done(typedb_protocol::query::initial_res::ok::Done {
            query_type: query_type.into(),
        })
    }

    pub(crate) fn query_res_ok_concept_row_stream(
        column_variable_names: Vec<String>,
        query_type: typedb_protocol::query::Type,
    ) -> typedb_protocol::query::initial_res::ok::Ok {
        typedb_protocol::query::initial_res::ok::Ok::ConceptRowStream(
            typedb_protocol::query::initial_res::ok::ConceptRowStream {
                column_variable_names,
                query_type: query_type.into(),
            },
        )
    }

    pub(crate) fn query_res_ok_concept_document_stream(
        query_type: typedb_protocol::query::Type,
    ) -> typedb_protocol::query::initial_res::ok::Ok {
        typedb_protocol::query::initial_res::ok::Ok::ConceptDocumentStream(
            typedb_protocol::query::initial_res::ok::ConceptDocumentStream { query_type: query_type.into() },
        )
    }

    pub(crate) fn query_initial_res_ok_from_query_res_ok_ok(
        message: typedb_protocol::query::initial_res::ok::Ok,
    ) -> typedb_protocol::query::initial_res::Ok {
        typedb_protocol::query::initial_res::Ok { ok: Some(message) }
    }

    pub(crate) fn query_initial_res_from_query_res_ok(
        message: typedb_protocol::query::initial_res::Ok,
    ) -> typedb_protocol::query::InitialRes {
        typedb_protocol::query::InitialRes { res: Some(typedb_protocol::query::initial_res::Res::Ok(message)) }
    }

    pub(crate) fn query_initial_res_from_error(error: typedb_protocol::Error) -> typedb_protocol::query::InitialRes {
        typedb_protocol::query::InitialRes { res: Some(typedb_protocol::query::initial_res::Res::Error(error)) }
    }

    pub(crate) fn query_res_part_from_concept_rows(
        messages: Vec<typedb_protocol::ConceptRow>,
    ) -> typedb_protocol::query::ResPart {
        typedb_protocol::query::ResPart {
            res: Some(typedb_protocol::query::res_part::Res::RowsRes(
                typedb_protocol::query::res_part::ConceptRowsRes { rows: messages },
            )),
        }
    }

    pub(crate) fn query_res_part_from_concept_documents(
        messages: Vec<typedb_protocol::ConceptDocument>,
    ) -> typedb_protocol::query::ResPart {
        typedb_protocol::query::ResPart {
            res: Some(typedb_protocol::query::res_part::Res::DocumentsRes(
                typedb_protocol::query::res_part::ConceptDocumentsRes { documents: messages },
            )),
        }
    }

    // -----------

    #[inline]
    fn transaction_res_part_res_part_stream_signal_done() -> typedb_protocol::transaction::res_part::ResPart {
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Done(
                    typedb_protocol::transaction::stream_signal::res_part::Done {},
                )),
            },
        )
    }

    #[inline]
    fn transaction_res_part_res_part_stream_signal_continue() -> typedb_protocol::transaction::res_part::ResPart {
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Continue(
                    typedb_protocol::transaction::stream_signal::res_part::Continue {},
                )),
            },
        )
    }

    #[inline]
    fn transaction_res_part_res_part_stream_signal_error(
        error_message: typedb_protocol::Error,
    ) -> typedb_protocol::transaction::res_part::ResPart {
        typedb_protocol::transaction::res_part::ResPart::StreamRes(
            typedb_protocol::transaction::stream_signal::ResPart {
                state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Error(error_message)),
            },
        )
    }

    #[inline]
    fn transaction_server_res(
        req_id: Uuid,
        message: typedb_protocol::transaction::res::Res,
    ) -> typedb_protocol::transaction::Server {
        typedb_protocol::transaction::Server {
            server: Some(typedb_protocol::transaction::server::Server::Res(typedb_protocol::transaction::Res {
                req_id: req_id.as_bytes().to_vec(),
                res: Some(message),
            })),
        }
    }

    #[inline]
    fn transaction_server_res_part(
        req_id: Uuid,
        message: typedb_protocol::transaction::res_part::ResPart,
    ) -> typedb_protocol::transaction::Server {
        typedb_protocol::transaction::Server {
            server: Some(typedb_protocol::transaction::server::Server::ResPart(
                typedb_protocol::transaction::ResPart { req_id: req_id.as_bytes().to_vec(), res_part: Some(message) },
            )),
        }
    }

    // helpers

    pub(crate) fn transaction_server_res_query_res(
        req_id: Uuid,
        message: typedb_protocol::query::InitialRes,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res(req_id, typedb_protocol::transaction::res::Res::QueryInitialRes(message))
    }

    #[inline]
    pub(crate) fn transaction_server_res_parts_query_part(
        req_id: Uuid,
        res_part: typedb_protocol::query::ResPart,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res_part(req_id, typedb_protocol::transaction::res_part::ResPart::QueryRes(res_part))
    }

    #[inline]
    pub(crate) fn transaction_server_res_part_stream_signal_continue(
        req_id: Uuid,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res_part(req_id, transaction_res_part_res_part_stream_signal_continue())
    }

    #[inline]
    pub(crate) fn transaction_server_res_part_stream_signal_done(req_id: Uuid) -> typedb_protocol::transaction::Server {
        transaction_server_res_part(req_id, transaction_res_part_res_part_stream_signal_done())
    }

    #[inline]
    pub(crate) fn transaction_server_res_part_stream_signal_error(
        req_id: Uuid,
        error: typedb_protocol::Error,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res_part(req_id, transaction_res_part_res_part_stream_signal_error(error))
    }

    pub(crate) fn transaction_server_res_rollback_res(
        req_id: Uuid,
        message: typedb_protocol::transaction::rollback::Res,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res(req_id, typedb_protocol::transaction::res::Res::RollbackRes(message))
    }

    pub(crate) fn transaction_server_res_commit_res(
        req_id: Uuid,
        message: typedb_protocol::transaction::commit::Res,
    ) -> typedb_protocol::transaction::Server {
        transaction_server_res(req_id, typedb_protocol::transaction::res::Res::CommitRes(message))
    }
}

pub(crate) mod user_manager {
    use system::concepts::User;

    pub(crate) fn users_all_res(users: Vec<User>) -> typedb_protocol::user_manager::all::Res {
        let mut users_proto: Vec<typedb_protocol::User> = vec![];
        for user in users {
            users_proto.push(new_user(user));
        }
        typedb_protocol::user_manager::all::Res { users: users_proto }
    }

    pub(crate) fn users_get_res(user: User) -> typedb_protocol::user_manager::get::Res {
        typedb_protocol::user_manager::get::Res { user: Some(new_user(user)) }
    }

    pub(crate) fn users_contains_res(contains: bool) -> typedb_protocol::user_manager::contains::Res {
        typedb_protocol::user_manager::contains::Res { contains }
    }

    pub(crate) fn user_create_res() -> typedb_protocol::user_manager::create::Res {
        typedb_protocol::user_manager::create::Res {}
    }

    pub(crate) fn user_update_res() -> typedb_protocol::user::update::Res {
        typedb_protocol::user::update::Res {}
    }

    pub(crate) fn users_delete_res() -> typedb_protocol::user::delete::Res {
        typedb_protocol::user::delete::Res {}
    }

    fn new_user(user: User) -> typedb_protocol::User {
        typedb_protocol::User { name: user.name, password: None }
    }
}

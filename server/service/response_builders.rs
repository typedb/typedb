/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use uuid::Uuid;

pub(crate) fn query_res_ok_empty() -> typedb_protocol::query::res::ok::Ok {
    typedb_protocol::query::res::ok::Ok::Empty(typedb_protocol::query::res::ok::Empty {})
}

pub(crate) fn query_res_ok_values(
    values: Vec<typedb_protocol::query::res::ok::values::OptionalValue>,
) -> typedb_protocol::query::res::ok::Ok {
    typedb_protocol::query::res::ok::Ok::Values(typedb_protocol::query::res::ok::Values { values })
}

pub(crate) fn query_res_ok_concept_row_stream(
    column_variable_names: Vec<String>,
) -> typedb_protocol::query::res::ok::Ok {
    typedb_protocol::query::res::ok::Ok::ConceptMapStream(typedb_protocol::query::res::ok::AnswerRowStream {
        column_variable_names,
    })
}

pub(crate) fn query_res_ok_readable_concept_tree_stream() -> typedb_protocol::query::res::ok::Ok {
    typedb_protocol::query::res::ok::Ok::ReadableConceptTreeStream(
        typedb_protocol::query::res::ok::ReadableConceptTreeStream {},
    )
}

pub(crate) fn query_res_ok_from_query_res_ok_ok(
    message: typedb_protocol::query::res::ok::Ok,
) -> typedb_protocol::query::res::Ok {
    typedb_protocol::query::res::Ok { ok: Some(message) }
}

pub(crate) fn query_res_from_query_res_ok(message: typedb_protocol::query::res::Ok) -> typedb_protocol::query::Res {
    typedb_protocol::query::Res { res: Some(typedb_protocol::query::res::Res::Ok(message)) }
}

pub(crate) fn query_res_from_error(error: typedb_protocol::Error) -> typedb_protocol::query::Res {
    typedb_protocol::query::Res { res: Some(typedb_protocol::query::res::Res::Error(error)) }
}

pub(crate) fn query_res_part_from_res_part_res(
    message: typedb_protocol::query::res_part::res::Res,
) -> typedb_protocol::query::res_part::Res {
    typedb_protocol::query::res_part::Res { res: Some(message) }
}

pub(crate) fn query_res_part_from_concept_tree() {
    todo!()
}

pub(crate) fn query_res_part_from_parts(
    messages: Vec<typedb_protocol::query::res_part::Res>,
) -> typedb_protocol::query::ResPart {
    typedb_protocol::query::ResPart { res: messages }
}

// -----------

#[inline]
fn transaction_res_part_query_res_part_parts(
    messages: Vec<typedb_protocol::query::res_part::Res>,
) -> typedb_protocol::transaction::res_part::ResPart {
    typedb_protocol::transaction::res_part::ResPart::QueryRes(typedb_protocol::query::ResPart { res: messages })
}

#[inline]
fn transaction_res_part_res_part_stream_signal_done() -> typedb_protocol::transaction::res_part::ResPart {
    typedb_protocol::transaction::res_part::ResPart::StreamRes(typedb_protocol::transaction::stream_signal::ResPart {
        state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Done(
            typedb_protocol::transaction::stream_signal::res_part::Done {},
        )),
    })
}

#[inline]
fn transaction_res_part_res_part_stream_signal_continue() -> typedb_protocol::transaction::res_part::ResPart {
    typedb_protocol::transaction::res_part::ResPart::StreamRes(typedb_protocol::transaction::stream_signal::ResPart {
        state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Continue(
            typedb_protocol::transaction::stream_signal::res_part::Continue {},
        )),
    })
}

#[inline]
fn transaction_res_part_res_part_stream_signal_error(
    error_message: typedb_protocol::Error,
) -> typedb_protocol::transaction::res_part::ResPart {
    typedb_protocol::transaction::res_part::ResPart::StreamRes(typedb_protocol::transaction::stream_signal::ResPart {
        state: Some(typedb_protocol::transaction::stream_signal::res_part::State::Error(error_message)),
    })
}

#[inline]
pub(crate) fn transaction_res_query_res(
    message: typedb_protocol::query::res::Res,
) -> typedb_protocol::transaction::res::Res {
    typedb_protocol::transaction::res::Res::QueryRes(typedb_protocol::query::Res { res: Some(message) })
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
        server: Some(typedb_protocol::transaction::server::Server::ResPart(typedb_protocol::transaction::ResPart {
            req_id: req_id.as_bytes().to_vec(),
            res_part: Some(message),
        })),
    }
}

// helpers

pub(crate) fn transaction_server_res_query_res(
    req_id: Uuid,
    message: typedb_protocol::query::Res,
) -> typedb_protocol::transaction::Server {
    transaction_server_res(req_id, typedb_protocol::transaction::res::Res::QueryRes(message))
}

#[inline]
pub(crate) fn transaction_server_res_parts_query_part(
    req_id: Uuid,
    res_parts: Vec<typedb_protocol::query::res_part::Res>,
) -> typedb_protocol::transaction::Server {
    transaction_server_res_part(req_id, transaction_res_part_query_res_part_parts(res_parts))
}

#[inline]
pub(crate) fn transaction_server_res_part_stream_signal_continue(req_id: Uuid) -> typedb_protocol::transaction::Server {
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

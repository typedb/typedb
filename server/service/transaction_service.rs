/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::{Streaming};
use typedb_protocol::transaction::{Client};

pub(crate) struct TransactionService {
    request_stream: Streaming<Client>,
    response_sender: Sender<()>,

    is_open: bool,
}

impl TransactionService {
    pub(crate) fn new(request_stream: Streaming<Client>, response_sender: Sender<_>) -> _ {
        Self { request_stream, response_sender, is_open: false }
    }

    pub(crate) async fn listen(&mut self) {
        loop {
            let next = self.request_stream.next().await;
            match next {
                None => {
                    // TODO: network stream/transaction has ended
                    return
                },
                Some(Err(error)) => {
                    // TODO: grpc error
                    todo!()
                }
                Some(Ok(message)) => {
                    for request in message.reqs {
                        let request_id = request.req_id;
                        let metadata = request.metadata;
                        match request.req {
                            None => {
                                // TODO: unexpected state: oneof in Protobuf should always be populated.
                            }
                            Some(req) => {
                                match (self.is_open, req) {
                                    (false, typedb_protocol::transaction::req::Req::OpenReq(open_req)) => {
                                        // TODO open txn
                                    },
                                    (true, typedb_protocol::transaction::req::Req::OpenReq(_)) => {
                                        // TODO: unexpected state: already open
                                    },
                                    (true, typedb_protocol::transaction::req::Req::QueryReq(query_req)) => {
                                        // TODO: compile query, create executor, respond with initial message and then await initial answers to send
                                    }
                                    (true, typedb_protocol::transaction::req::Req::StreamReq(stream_req)) => {
                                        //
                                    }
                                    (true, typedb_protocol::transaction::req::Req::CommitReq(commit_req)) => {}
                                    (true, typedb_protocol::transaction::req::Req::RollbackReq(rollback_req)) => {}
                                    (true, typedb_protocol::transaction::req::Req::CloseReq(close_req)) => {
                                    }
                                    (false, _) => {
                                        // TODO: unexpected state: already closed
                                    }
                                }
                            }
                        }
                    }
                }
            }



            let result = self.response_sender.send(()).await;
        }
    }
}

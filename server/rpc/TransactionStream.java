/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.rpc;

import grabl.tracing.client.GrablTracingThreadStatic;
import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static grabl.tracing.client.GrablTracingThreadStatic.continueTraceOnThread;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_NOT_OPENED;
import static grakn.core.server.rpc.util.ResponseBuilder.exception;

/**
 * A StreamObserver that implements the transaction connection between a client
 * and the server. This class receives a stream of {@code Transaction.Req} and
 * returns a stream of {@code Transaction.Res}.
 */
public class TransactionStream implements StreamObserver<Transaction.Req> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionStream.class);

    private final GraknRPCService graknRPCService;
    private final StreamObserver<Transaction.Res> responder;
    private final AtomicBoolean isOpen;

    @Nullable
    private TransactionRPC transactionRPC;

    TransactionStream(GraknRPCService graknRPCService, StreamObserver<Transaction.Res> responder) {
        this.graknRPCService = graknRPCService;
        this.responder = responder;
        isOpen = new AtomicBoolean(false);
    }

    @Override
    public void onNext(Transaction.Req request) {
        try {
            LOG.trace("Request: {}", request);

            if (GrablTracingThreadStatic.isTracingEnabled()) {
                final Map<String, String> metadata = request.getMetadataMap();
                final String rootId = metadata.get("traceRootId");
                final String parentId = metadata.get("traceParentId");
                if (rootId != null && parentId != null) {
                    handleRequestWithTracing(request, rootId, parentId);
                    return;
                }
            }
            handleRequest(request);
        } catch (Exception e) {
            closeWithError(e);
        }
    }

    @Override
    public void onCompleted() {
        close();
    }

    /**
     * This method is invoked when a client abruptly terminates a connection to
     * the server. So, we want to make sure to also close and delete the current
     * session and all the transactions associated to the same client.
     *
     * This might create issues if a session is used by other concurrent clients,
     * the better approach would be to signal a closure intent to the session
     * and the session should be able to detect whether it's been used by other
     * connections, if not close it completely.
     *
     * TODO: improve by sending closure intent to the session
     */
    @Override
    public void onError(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            assert transactionRPC != null;
            transactionRPC.sessionRPC().closeWithError(error);
        }
    }

    private void handleRequest(Transaction.Req request) {
        if (request.getReqCase() == Transaction.Req.ReqCase.OPEN_REQ) {
            open(request);
        } else {
            if (!isOpen.get()) throw new GraknException(TRANSACTION_NOT_OPENED);
            assert transactionRPC != null;
            transactionRPC.handleRequest(request);
        }
    }

    private void handleRequestWithTracing(Transaction.Req request, String rootId, String parentId) {
        try (ThreadTrace ignored = continueTraceOnThread(UUID.fromString(rootId), UUID.fromString(parentId), "handle")) {
            handleRequest(request);
        }
    }

    private void open(Transaction.Req request) {
        final Instant processingStartTime = Instant.now();
        final Transaction.Open.Req openReq = request.getOpenReq();
        final UUID sessionID = bytesToUUID(openReq.getSessionId().toByteArray());
        final SessionRPC sessionRPC = graknRPCService.rpcSessions().get(sessionID);
        if (sessionRPC == null) throw new GraknException(SESSION_NOT_FOUND.message(sessionID));

        if (isOpen.compareAndSet(false, true)) {
            transactionRPC = sessionRPC.transaction(this, openReq);
            final int processingTimeMillis = (int) Duration.between(processingStartTime, Instant.now()).toMillis();
            responder.onNext(TransactionProto.Transaction.Res.newBuilder().setId(request.getId())
                    .setOpenRes(TransactionProto.Transaction.Open.Res.newBuilder().setProcessingTimeMillis(processingTimeMillis)).build());
        } else {
            throw new GraknException(TRANSACTION_ALREADY_OPENED);
        }
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            responder.onCompleted();
        }
    }

    void closeWithError(Throwable error) {
        LOG.error(error.getMessage(), error);
        if (isOpen.compareAndSet(true, false)) {
            responder.onError(exception(error));
        }
    }

    StreamObserver<Transaction.Res> responder() {
        return responder;
    }
}

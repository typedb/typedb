/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.protocol.TransactionProto.Transaction;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static grabl.tracing.client.GrablTracingThreadStatic.continueTraceOnThread;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_NOT_OPENED;
import static grakn.core.server.rpc.common.ResponseBuilder.exception;

/**
 * A StreamObserver that implements the transaction connection between a client
 * and the server. This class receives a stream of {@code Transaction.Req} and
 * returns a stream of {@code Transaction.Res}.
 */
public class TransactionStream implements StreamObserver<Transaction.Req> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionStream.class);

    private final GraknRPCService graknRPCService;
    private final StreamObserver<Transaction.Res> responder;
    /**
     * Whether or not the {@link StreamObserver} is currently open and capable of transmitting responses.
     * This does not necessarily correspond to the {@link Grakn.Transaction} being open.
     */
    private final AtomicBoolean isOpen;
    private final AtomicReference<TransactionRPC> transactionRPC;

    TransactionStream(GraknRPCService graknRPCService, StreamObserver<Transaction.Res> responder) {
        this.graknRPCService = graknRPCService;
        this.responder = responder;
        isOpen = new AtomicBoolean(true);
        transactionRPC = new AtomicReference<>();
    }

    @Override
    public void onNext(Transaction.Req request) {
        try {
            LOG.trace("Request: {}", request);

            if (GrablTracingThreadStatic.isTracingEnabled()) {
                Map<String, String> metadata = request.getMetadataMap();
                String rootId = metadata.get("traceRootId");
                String parentId = metadata.get("traceParentId");
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
        try {
            TransactionRPC t;
            if ((t = transactionRPC.get()) != null) t.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            close();
        }
    }

    @Override
    public void onError(Throwable error) {
        try {
            TransactionRPC t;
            if ((t = transactionRPC.get()) != null) t.closeWithError(error);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeWithError(error);
        }
    }

    private void handleRequest(Transaction.Req request) {
        if (request.getReqCase() == Transaction.Req.ReqCase.OPEN_REQ) {
            open(request);
        } else {
            TransactionRPC t;
            if ((t = transactionRPC.get()) == null) throw GraknException.of(TRANSACTION_NOT_OPENED);
            t.handleRequest(request);
        }
    }

    private void handleRequestWithTracing(Transaction.Req request, String rootId, String parentId) {
        try (ThreadTrace ignored = continueTraceOnThread(UUID.fromString(rootId), UUID.fromString(parentId), "handle")) {
            handleRequest(request);
        }
    }

    private void open(Transaction.Req request) {
        Instant processingStartTime = Instant.now();
        Transaction.Open.Req openReq = request.getOpenReq();
        UUID sessionID = bytesToUUID(openReq.getSessionId().toByteArray());
        SessionRPC sessionRPC = graknRPCService.getSession(sessionID);
        if (sessionRPC == null) throw GraknException.of(SESSION_NOT_FOUND, sessionID);

        if (!transactionRPC.compareAndSet(null, sessionRPC.transaction(this, openReq))) {
            throw GraknException.of(TRANSACTION_ALREADY_OPENED);
        }

        int processingTimeMillis = (int) Duration.between(processingStartTime, Instant.now()).toMillis();
        responder.onNext(Transaction.Res.newBuilder().setId(request.getId()).setOpenRes(
                Transaction.Open.Res.newBuilder().setProcessingTimeMillis(processingTimeMillis)
        ).build());
    }

    /**
     * Sends an OK response that terminates the stream if it is open. Otherwise, performs no action.
     */
    void close() {
        if (isOpen.compareAndSet(true, false)) {
            responder.onCompleted();
        }
    }

    /**
     * Sends an error response that terminates the stream if it is open. Otherwise, performs no action.
     */
    void closeWithError(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            LOG.error(error.getMessage(), error);
            responder.onError(exception(error));
        }
    }

    StreamObserver<Transaction.Res> responder() {
        return responder;
    }
}

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

package grakn.core.server.transaction;

import grabl.tracing.client.GrablTracingThreadStatic;
import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.exception.GraknException;
import grakn.core.server.GraknService;
import grakn.core.server.common.ResponseBuilder;
import grakn.core.server.common.TracingData;
import grakn.core.server.session.SessionService;
import grakn.protocol.TransactionProto.Transaction;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static grabl.tracing.client.GrablTracingThreadStatic.continueTraceOnThread;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Server.EMPTY_TRANSACTION_REQUEST;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_NOT_OPENED;


public class TransactionStream implements StreamObserver<Transaction.Reqs> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionStream.class);
    private static final String TRACE_PREFIX = "transaction_stream.";

    private final GraknService graknService;
    private final SynchronizedStreamObserver<Transaction.Res> responder;
    private final AtomicReference<TransactionService> transactionSrv;
    private final AtomicBoolean isOpen;

    public TransactionStream(GraknService graknService, StreamObserver<Transaction.Res> responder) {
        this.graknService = graknService;
        this.responder = new SynchronizedStreamObserver<>(responder);
        isOpen = new AtomicBoolean(true);
        transactionSrv = new AtomicReference<>();
    }

    SynchronizedStreamObserver<Transaction.Res> responder() {
        return responder;
    }

    @Override
    public void onNext(Transaction.Reqs requests) {
        if (requests.getTransactionReqsList().isEmpty()) throw GraknException.of(EMPTY_TRANSACTION_REQUEST);
        else if (transactionSrv.get() == null) init(requests);
        else transactionSrv.get().execute(requests);
    }

    @Override
    public void onCompleted() {
        try {
            TransactionService transactionSrv = this.transactionSrv.get();
            if (transactionSrv != null) transactionSrv.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            close();
        }
    }

    @Override
    public void onError(Throwable error) {
        try {
            TransactionService transactionSrv = this.transactionSrv.get();
            if (transactionSrv != null) transactionSrv.close(error);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            close(error);
        }
    }

    void init(Transaction.Reqs requests) {
        if (requests.getTransactionReqsList().size() == 1 &&
                requests.getTransactionReqs(0).getReqCase() == Transaction.Req.ReqCase.OPEN_REQ) {
            open(requests.getTransactionReqs(0));
        } else {
            throw GraknException.of(TRANSACTION_NOT_OPENED);
        }
    }

    private void open(Transaction.Req request) {
        ThreadTrace trace = mayStartTrace(request, TRACE_PREFIX + "open");
        Transaction.Open.Req openReq = request.getOpenReq();
        UUID sessionID = bytesToUUID(openReq.getSessionId().toByteArray());
        SessionService sessionSrv = graknService.session(sessionID);
        if (sessionSrv == null) throw GraknException.of(SESSION_NOT_FOUND, sessionID);
        TransactionService newTransactionSrv = sessionSrv.transaction(this, openReq);
        if (!transactionSrv.compareAndSet(null, newTransactionSrv)) {
            newTransactionSrv.close();
            throw GraknException.of(TRANSACTION_ALREADY_OPENED);
        }
        responder.onNext(Transaction.Res.newBuilder().setId(request.getId()).setOpenRes(
                Transaction.Open.Res.getDefaultInstance()
        ).build());
        mayCloseTrace(trace);
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
    void close(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            LOG.error(error.getMessage(), error);
            responder.onError(ResponseBuilder.exception(error));
        }
    }

    private Optional<TracingData> getTracingData(Transaction.Req request) {
        if (GrablTracingThreadStatic.isTracingEnabled()) {
            Map<String, String> metadata = request.getMetadataMap();
            String rootID = metadata.get("traceRootId");
            String parentID = metadata.get("traceParentId");
            if (rootID != null && parentID != null) {
                return Optional.of(new TracingData(rootID, parentID));
            }
        }
        return Optional.empty();
    }

    @Nullable
    private ThreadTrace mayStartTrace(Transaction.Req request, String name) {
        ThreadTrace trace = null;
        Optional<TracingData> tracingData = getTracingData(request);
        if (tracingData.isPresent()) {
            trace = continueTraceOnThread(tracingData.get().rootID(), tracingData.get().parentID(), name);
        }
        return trace;
    }

    private void mayCloseTrace(@Nullable ThreadTrace trace) {
        if (trace != null) trace.close();
    }

    public static class SynchronizedStreamObserver<T> {

        private final StreamObserver<T> streamObserver;

        SynchronizedStreamObserver(StreamObserver<T> stream) {
            streamObserver = stream;
        }

        synchronized void onNext(T value) {
            streamObserver.onNext(value);
        }

        synchronized void onCompleted() {
            streamObserver.onCompleted();
        }

        synchronized void onError(Throwable t) {
            streamObserver.onError(t);
        }
    }
}

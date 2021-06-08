/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.server.common.RequestReader;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.core.server.common.SynchronizedStreamObserver;
import com.vaticle.typedb.core.server.common.TracingData;
import com.vaticle.typedb.core.server.concept.ConceptService;
import com.vaticle.typedb.core.server.concept.ThingService;
import com.vaticle.typedb.core.server.concept.TypeService;
import com.vaticle.typedb.core.server.logic.LogicService;
import com.vaticle.typedb.core.server.logic.RuleService;
import com.vaticle.typedb.core.server.query.QueryService;
import com.vaticle.typedb.protocol.TransactionProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.continueTraceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DUPLICATE_REQUEST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.EMPTY_TRANSACTION_REQUEST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ITERATION_WITH_UNKNOWN_ID;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.BAD_TRANSACTION_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.RPC_PREFETCH_SIZE_TOO_SMALL;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_NOT_OPENED;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Transaction.serverMsg;
import static com.vaticle.typedb.protocol.TransactionProto.Transaction.Stream.State.CONTINUE;
import static com.vaticle.typedb.protocol.TransactionProto.Transaction.Stream.State.DONE;

public class TransactionService implements StreamObserver<TransactionProto.Transaction.Client>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);
    private static final String TRACE_PREFIX = "transaction_services.";
    private static final int MAX_NETWORK_LATENCY_MILLIS = 3_000;

    private final TypeDBService typeDBSvc;
    private final StreamObserver<TransactionProto.Transaction.Server> responder;
    private final ConcurrentMap<UUID, ResponseStream<?>> streams;
    private final AtomicBoolean isRPCAlive;
    private final AtomicBoolean isTransactionOpen;

    private volatile SessionService sessionSvc;
    private volatile TypeDB.Transaction transaction;
    private volatile Services services;
    private volatile int networkLatencyMillis;

    private class Services {
        private final ConceptService concept = new ConceptService(TransactionService.this, transaction.concepts());
        private final LogicService logic = new LogicService(TransactionService.this, transaction.logic());
        private final QueryService query = new QueryService(TransactionService.this, transaction.query());
        private final ThingService thing = new ThingService(TransactionService.this, transaction.concepts());
        private final TypeService type = new TypeService(TransactionService.this, transaction.concepts());
        private final RuleService rule = new RuleService(TransactionService.this, transaction.logic());
    }

    public TransactionService(TypeDBService typeDBSvc, StreamObserver<TransactionProto.Transaction.Server> responder) {
        this.typeDBSvc = typeDBSvc;
        this.responder = SynchronizedStreamObserver.of(responder);
        this.streams = new ConcurrentHashMap<>();
        this.isRPCAlive = new AtomicBoolean(true);
        this.isTransactionOpen = new AtomicBoolean(false);
    }

    public Context.Transaction context() {
        return transaction.context();
    }

    @Override
    public void onNext(TransactionProto.Transaction.Client requests) {
        if (requests.getReqsList().isEmpty()) close(TypeDBException.of(EMPTY_TRANSACTION_REQUEST));
        else for (TransactionProto.Transaction.Req req : requests.getReqsList()) execute(req);
    }

    @Override
    public void onCompleted() { close(); }

    @Override
    public void onError(Throwable error) { close(error); }

    private synchronized void execute(TransactionProto.Transaction.Req request) {
        FactoryTracingThreadStatic.ThreadTrace trace = null;
        try {
            trace = mayStartTrace(request, TRACE_PREFIX + request.getReqCase().name().toLowerCase());
            switch (request.getReqCase()) {
                case REQ_NOT_SET:
                    throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
                case OPEN_REQ:
                    open(request);
                    break;
                default:
                    executeRequest(request);
            }
        } catch (Throwable error) {
            close(error);
        } finally {
            mayCloseTrace(trace);
        }
    }

    private void executeRequest(TransactionProto.Transaction.Req req) {
        if (!isRPCAlive.get()) throw TypeDBException.of(TRANSACTION_CLOSED);
        if (!isTransactionOpen.get()) throw TypeDBException.of(TRANSACTION_NOT_OPENED);

        switch (req.getReqCase()) {
            case ROLLBACK_REQ:
                rollback(byteStringAsUUID(req.getReqId()));
                break;
            case COMMIT_REQ:
                commit(byteStringAsUUID(req.getReqId()));
                break;
            case STREAM_REQ:
                stream(byteStringAsUUID(req.getReqId()));
                break;
            case QUERY_MANAGER_REQ:
                services.query.execute(req);
                break;
            case CONCEPT_MANAGER_REQ:
                services.concept.execute(req);
                break;
            case LOGIC_MANAGER_REQ:
                services.logic.execute(req);
                break;
            case THING_REQ:
                services.thing.execute(req);
                break;
            case TYPE_REQ:
                services.type.execute(req);
                break;
            case RULE_REQ:
                services.rule.execute(req);
                break;
            default:
                throw TypeDBException.of(ILLEGAL_ARGUMENT);
        }
    }

    private void open(TransactionProto.Transaction.Req request) {
        if (isTransactionOpen.get()) throw TypeDBException.of(TRANSACTION_ALREADY_OPENED);
        TransactionProto.Transaction.Open.Req openReq = request.getOpenReq();
        networkLatencyMillis = Math.min(openReq.getNetworkLatencyMillis(), MAX_NETWORK_LATENCY_MILLIS);
        sessionSvc = sessionService(typeDBSvc, openReq);
        sessionSvc.register(this);
        transaction = transaction(sessionSvc, openReq);
        services = new Services();
        respond(ResponseBuilder.Transaction.open(byteStringAsUUID(request.getReqId())));
        isTransactionOpen.set(true);
    }

    private static SessionService sessionService(TypeDBService typeDBSvc, TransactionProto.Transaction.Open.Req req) {
        UUID sessionID = byteStringAsUUID(req.getSessionId());
        SessionService sessionSvc = typeDBSvc.session(sessionID);
        if (sessionSvc == null) throw TypeDBException.of(SESSION_NOT_FOUND, sessionID);
        return sessionSvc;
    }

    private static TypeDB.Transaction transaction(SessionService sessionSvc, TransactionProto.Transaction.Open.Req req) {
        Arguments.Transaction.Type type = Arguments.Transaction.Type.of(req.getType().getNumber());
        if (type == null) throw TypeDBException.of(BAD_TRANSACTION_TYPE, req.getType());
        Options.Transaction options = new Options.Transaction().parent(sessionSvc.options());
        applyDefaultOptions(options, req.getOptions());
        return sessionSvc.session().transaction(type, options);
    }

    private void commit(UUID requestID) {
        transaction.commit();
        respond(ResponseBuilder.Transaction.commit(requestID));
        close();
    }

    private void rollback(UUID requestID) {
        transaction.rollback();
        respond(ResponseBuilder.Transaction.rollback(requestID));
    }

    public void respond(TransactionProto.Transaction.Res response) {
        responder.onNext(serverMsg(response));
    }

    public void respond(TransactionProto.Transaction.ResPart partialResponse) {
        responder.onNext(serverMsg(partialResponse));
    }

    public <T> void stream(Iterator<T> iterator, UUID requestID,
                           Function<List<T>, TransactionProto.Transaction.ResPart> resPartFn) {
        int size = transaction.context().options().prefetchSize();
        stream(iterator, requestID, size, true, resPartFn);
    }

    public <T> void stream(Iterator<T> iterator, UUID requestID, Options.Query options,
                           Function<List<T>, TransactionProto.Transaction.ResPart> resPartFn) {
        stream(iterator, requestID, options.prefetchSize(), options.prefetch(), resPartFn);
    }

    private <T> void stream(Iterator<T> iterator, UUID requestID, int prefetchSize, boolean prefetch,
                            Function<List<T>, TransactionProto.Transaction.ResPart> resPartFn) {
        ResponseStream<T> stream = new ResponseStream<>(iterator, requestID, prefetchSize, resPartFn);
        streams.compute(requestID, (key, oldValue) -> {
            if (oldValue == null) return stream;
            else throw TypeDBException.of(DUPLICATE_REQUEST, requestID);
        });
        if (prefetch) stream.streamResParts();
        else respond(ResponseBuilder.Transaction.stream(requestID, CONTINUE));
    }

    private void stream(UUID requestId) {
        ResponseStream<?> stream = streams.get(requestId);
        if (stream == null) throw TypeDBException.of(ITERATION_WITH_UNKNOWN_ID, requestId);
        stream.streamResParts();
    }

    @Nullable
    private FactoryTracingThreadStatic.ThreadTrace mayStartTrace(TransactionProto.Transaction.Req request, String name) {
        FactoryTracingThreadStatic.ThreadTrace trace = null;
        Optional<TracingData> tracingData = RequestReader.getTracingData(request);
        if (tracingData.isPresent()) {
            trace = continueTraceOnThread(tracingData.get().rootID(), tracingData.get().parentID(), name);
        }
        return trace;
    }

    private void mayCloseTrace(@Nullable FactoryTracingThreadStatic.ThreadTrace trace) {
        if (trace != null) trace.close();
    }

    @Override
    public synchronized void close() {
        if (isRPCAlive.compareAndSet(true, false)) {
            if (isTransactionOpen.compareAndSet(true, false)) {
                transaction.close();
                sessionSvc.remove(this);
            }
            responder.onCompleted();
        }
    }

    public synchronized void close(Throwable error) {
        if (isRPCAlive.compareAndSet(true, false)) {
            if (isTransactionOpen.compareAndSet(true, false)) {
                transaction.close();
                sessionSvc.remove(this);
            }
            responder.onError(ResponseBuilder.exception(error));
            // TODO: We should restrict the type of errors that we log.
            //       Expected error handling from the server side does not need to be logged - they create noise.
            if (isClientCancelled(error)) LOG.debug(error.getMessage(), error);
            else LOG.error(error.getMessage(), error);
        }
    }

    private boolean isClientCancelled(Throwable error) {
        return error instanceof StatusRuntimeException &&
                ((StatusRuntimeException) error).getStatus().getCode().equals(Status.CANCELLED.getCode());
    }

    private class ResponseStream<T> {

        private final Function<List<T>, TransactionProto.Transaction.ResPart> resPartFn;
        private final Iterator<T> iterator;
        private final UUID requestID;
        private final int prefetchSize;

        ResponseStream(Iterator<T> iterator, UUID requestID, int prefetchSize,
                       Function<List<T>, TransactionProto.Transaction.ResPart> resPartFn) {
            this.iterator = iterator;
            this.requestID = requestID;
            if (prefetchSize < 1) throw TypeDBException.of(RPC_PREFETCH_SIZE_TOO_SMALL, prefetchSize);
            this.prefetchSize = prefetchSize;
            this.resPartFn = resPartFn;
        }

        private void streamResParts() {
            streamResPartsWhile(i -> i < prefetchSize && iterator.hasNext());
            if (mayClose()) return;

            respondStreamState(CONTINUE);
            Instant compensationEndTime = Instant.now().plusMillis(networkLatencyMillis);
            streamResPartsWhile(i -> iterator.hasNext() && Instant.now().isBefore(compensationEndTime));
            mayClose();
        }

        private void streamResPartsWhile(Predicate<Integer> predicate) {
            List<T> answers = new ArrayList<>();
            Instant startTime = Instant.now();
            for (int i = 0; predicate.test(i); i++) {
                answers.add(iterator.next());
                Instant currentTime = Instant.now();
                if (Duration.between(startTime, currentTime).toMillis() >= 1) {
                    respond(resPartFn.apply(answers));
                    answers.clear();
                    startTime = currentTime;
                }
            }
            if (!answers.isEmpty()) respond(resPartFn.apply(answers));
        }

        private boolean mayClose() {
            if (!iterator.hasNext()) respondStreamState(DONE);
            return !iterator.hasNext();
        }

        private void respondStreamState(TransactionProto.Transaction.Stream.State state) {
            respond(ResponseBuilder.Transaction.stream(requestID, state));
        }
    }
}

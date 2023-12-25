/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.core.server.common.SynchronizedStreamObserver;
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
import io.sentry.ITransaction;
import io.sentry.Sentry;
import io.sentry.SpanStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DUPLICATE_REQUEST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.EMPTY_TRANSACTION_REQUEST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ITERATION_WITH_UNKNOWN_ID;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.TRANSACTION_EXCEEDED_MAX_SECONDS;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.BAD_TRANSACTION_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.RPC_PREFETCH_SIZE_TOO_SMALL;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_CLOSED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.TRANSACTION_NOT_OPENED;
import static com.vaticle.typedb.core.concurrent.executor.Executors.scheduled;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Transaction.serverMsg;
import static com.vaticle.typedb.protocol.TransactionProto.Transaction.Stream.State.CONTINUE;
import static com.vaticle.typedb.protocol.TransactionProto.Transaction.Stream.State.DONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TransactionService implements StreamObserver<TransactionProto.Transaction.Client>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionService.class);
    private static final String TRACE_PREFIX = "transaction_services.";
    private static final int MAX_NETWORK_LATENCY_MILLIS = 3_000;

    private final TypeDBService typeDBSvc;
    private final StreamObserver<TransactionProto.Transaction.Server> responder;
    private final ConcurrentMap<UUID, ResponseStream<?>> streams;
    private final AtomicBoolean isRPCAlive;
    private final AtomicBoolean isTransactionOpen;
    private final ReadWriteLock requestLock;

    private volatile SessionService sessionSvc;
    private volatile TypeDB.Transaction transaction;
    private volatile Options.Transaction options;
    private volatile Services services;
    private volatile ScheduledFuture<?> scheduledTimeout;
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
        this.requestLock = new StampedLock().asReadWriteLock();
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
    public void onCompleted() {
        close();
    }

    @Override
    public void onError(Throwable error) {
        close(error);
    }

    private void execute(TransactionProto.Transaction.Req request) {
        Lock accessLock = null;
        ITransaction tx = Sentry.startTransaction("transaction_req", requestType(request));
        try {
            accessLock = acquireRequestLock(request);
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
            tx.setThrowable(error);
            tx.setStatus(SpanStatus.INTERNAL_ERROR);
            close(error);
        } finally {
            tx.finish();
            if (accessLock != null) accessLock.unlock();
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
                executeQueryRequest(req);
                break;
            case CONCEPT_MANAGER_REQ:
                executeConceptRequest(req);
                break;
            case LOGIC_MANAGER_REQ:
                executeLogicRequest(req);
                break;
            case THING_REQ:
                executeThingRequest(req);
                break;
            case TYPE_REQ:
                executeTypeRequest(req);
                break;
            case RULE_REQ:
                executeRuleRequest(req);
                break;
            default:
                throw TypeDBException.of(ILLEGAL_ARGUMENT);
        }
    }

    private String requestType(TransactionProto.Transaction.Req req) {
        switch (req.getReqCase()) {
            case QUERY_MANAGER_REQ: return req.getQueryManagerReq().getReqCase().name();
            case CONCEPT_MANAGER_REQ: return req.getConceptManagerReq().getReqCase().name();
            case LOGIC_MANAGER_REQ: return req.getLogicManagerReq().getReqCase().name();
            case THING_REQ: return req.getThingReq().getReqCase().name();
            case TYPE_REQ: return req.getTypeReq().getReqCase().name();
            case RULE_REQ: return req.getRuleReq().getReqCase().name();
            default: return req.getReqCase().name();
        }
    }

    protected void open(TransactionProto.Transaction.Req request) {
        if (isTransactionOpen.get()) throw TypeDBException.of(TRANSACTION_ALREADY_OPENED);
        TransactionProto.Transaction.Open.Req openReq = request.getOpenReq();
        networkLatencyMillis = Math.min(openReq.getNetworkLatencyMillis(), MAX_NETWORK_LATENCY_MILLIS);
        sessionSvc = sessionService(openReq);
        sessionSvc.register(this);
        options = new Options.Transaction().parent(sessionSvc.options());
        applyDefaultOptions(options, openReq.getOptions());
        transaction = transaction(sessionSvc, openReq, options);
        services = new Services();
        respond(ResponseBuilder.Transaction.open(byteStringAsUUID(request.getReqId())));
        isTransactionOpen.set(true);
        scheduledTimeout = scheduled().schedule(this::timeout, options.transactionTimeoutMillis(), MILLISECONDS);
    }

    protected SessionService sessionService(TransactionProto.Transaction.Open.Req req) {
        UUID sessionID = byteStringAsUUID(req.getSessionId());
        SessionService sessionSvc = typeDBSvc.session(sessionID);
        if (sessionSvc == null) throw TypeDBException.of(SESSION_NOT_FOUND, sessionID);
        return sessionSvc;
    }

    private static TypeDB.Transaction transaction(SessionService sessionSvc, TransactionProto.Transaction.Open.Req req,
                                                  Options.Transaction options) {
        Arguments.Transaction.Type type = Arguments.Transaction.Type.of(req.getType().getNumber());
        if (type == null) throw TypeDBException.of(BAD_TRANSACTION_TYPE, req.getType());
        return sessionSvc.session().transaction(type, options);
    }

    protected void commit(UUID requestID) {
        transaction.commit();
        respond(ResponseBuilder.Transaction.commit(requestID));
        close();
    }

    protected void rollback(UUID requestID) {
        transaction.rollback();
        respond(ResponseBuilder.Transaction.rollback(requestID));
    }

    protected void executeQueryRequest(TransactionProto.Transaction.Req req) {
        services.query.execute(req);
    }

    protected void executeConceptRequest(TransactionProto.Transaction.Req req) {
        services.concept.execute(req);
    }

    protected void executeLogicRequest(TransactionProto.Transaction.Req req) {
        services.logic.execute(req);
    }

    protected void executeThingRequest(TransactionProto.Transaction.Req req) {
        services.thing.execute(req);
    }

    protected void executeTypeRequest(TransactionProto.Transaction.Req req) {
        services.type.execute(req);
    }

    protected void executeRuleRequest(TransactionProto.Transaction.Req req) {
        services.rule.execute(req);
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

    protected void stream(UUID requestId) {
        ResponseStream<?> stream = streams.get(requestId);
        if (stream == null) throw TypeDBException.of(ITERATION_WITH_UNKNOWN_ID, requestId);
        stream.streamResParts();
    }

    private Lock acquireRequestLock(TransactionProto.Transaction.Req request) {
        Lock accessLock = isWriteRequest(request) ? requestLock.writeLock() : requestLock.readLock();
        accessLock.lock();
        return accessLock;
    }

    private static boolean isWriteRequest(TransactionProto.Transaction.Req request) {
        switch (request.getReqCase()) {
            case OPEN_REQ:
            case COMMIT_REQ:
            case ROLLBACK_REQ:
            case RULE_REQ:
                return true;
            case CONCEPT_MANAGER_REQ:
                switch (request.getConceptManagerReq().getReqCase()) {
                    case GET_ENTITY_TYPE_REQ:
                    case GET_RELATION_TYPE_REQ:
                    case GET_ATTRIBUTE_TYPE_REQ:
                    case GET_ENTITY_REQ:
                    case GET_RELATION_REQ:
                    case GET_ATTRIBUTE_REQ:
                    case GET_SCHEMA_EXCEPTIONS_REQ:
                        return false;
                    case PUT_ENTITY_TYPE_REQ:
                    case PUT_RELATION_TYPE_REQ:
                    case PUT_ATTRIBUTE_TYPE_REQ:
                        return true;
                }
                break;
            case LOGIC_MANAGER_REQ:
                switch (request.getLogicManagerReq().getReqCase()) {
                    case GET_RULE_REQ:
                    case GET_RULES_REQ:
                        return false;
                    case PUT_RULE_REQ:
                        return true;
                }
                break;
            case TYPE_REQ:
                switch (request.getTypeReq().getReqCase()) {
                    case THING_TYPE_REQ:
                        switch (request.getTypeReq().getThingTypeReq().getReqCase()) {
                            case THING_TYPE_DELETE_REQ:
                            case THING_TYPE_SET_LABEL_REQ:
                            case THING_TYPE_SET_ABSTRACT_REQ:
                            case THING_TYPE_UNSET_ABSTRACT_REQ:
                            case THING_TYPE_SET_OWNS_REQ:
                            case THING_TYPE_UNSET_OWNS_REQ:
                            case THING_TYPE_SET_PLAYS_REQ:
                            case THING_TYPE_UNSET_PLAYS_REQ:
                            case ENTITY_TYPE_CREATE_REQ:
                            case ENTITY_TYPE_SET_SUPERTYPE_REQ:
                            case RELATION_TYPE_CREATE_REQ:
                            case RELATION_TYPE_SET_SUPERTYPE_REQ:
                            case RELATION_TYPE_SET_RELATES_REQ:
                            case RELATION_TYPE_UNSET_RELATES_REQ:
                            case ATTRIBUTE_TYPE_PUT_REQ:
                            case ATTRIBUTE_TYPE_SET_SUPERTYPE_REQ:
                            case ATTRIBUTE_TYPE_SET_REGEX_REQ:
                                return true;
                            case THING_TYPE_GET_OWNS_REQ:
                            case THING_TYPE_GET_OWNS_OVERRIDDEN_REQ:
                            case THING_TYPE_GET_PLAYS_REQ:
                            case THING_TYPE_GET_PLAYS_OVERRIDDEN_REQ:
                            case THING_TYPE_GET_SYNTAX_REQ:
                            case ENTITY_TYPE_GET_SUPERTYPE_REQ:
                            case ENTITY_TYPE_GET_SUPERTYPES_REQ:
                            case ENTITY_TYPE_GET_SUBTYPES_REQ:
                            case ENTITY_TYPE_GET_INSTANCES_REQ:
                            case RELATION_TYPE_GET_SUPERTYPE_REQ:
                            case RELATION_TYPE_GET_SUPERTYPES_REQ:
                            case RELATION_TYPE_GET_SUBTYPES_REQ:
                            case RELATION_TYPE_GET_INSTANCES_REQ:
                            case RELATION_TYPE_GET_RELATES_REQ:
                            case RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ:
                            case RELATION_TYPE_GET_RELATES_OVERRIDDEN_REQ:
                            case ATTRIBUTE_TYPE_GET_REQ:
                            case ATTRIBUTE_TYPE_GET_SUPERTYPE_REQ:
                            case ATTRIBUTE_TYPE_GET_SUPERTYPES_REQ:
                            case ATTRIBUTE_TYPE_GET_SUBTYPES_REQ:
                            case ATTRIBUTE_TYPE_GET_INSTANCES_REQ:
                            case ATTRIBUTE_TYPE_GET_REGEX_REQ:
                            case ATTRIBUTE_TYPE_GET_OWNERS_REQ:
                                return false;
                        }
                        break;
                    case ROLE_TYPE_REQ:
                        switch (request.getTypeReq().getRoleTypeReq().getReqCase()) {
                            case ROLE_TYPE_DELETE_REQ:
                            case ROLE_TYPE_SET_LABEL_REQ:
                                return true;
                            case ROLE_TYPE_GET_SUPERTYPE_REQ:
                            case ROLE_TYPE_GET_SUPERTYPES_REQ:
                            case ROLE_TYPE_GET_SUBTYPES_REQ:
                            case ROLE_TYPE_GET_RELATION_TYPES_REQ:
                            case ROLE_TYPE_GET_PLAYER_TYPES_REQ:
                            case ROLE_TYPE_GET_RELATION_INSTANCES_REQ:
                            case ROLE_TYPE_GET_PLAYER_INSTANCES_REQ:
                                return false;
                        }
                        break;
                }
                break;
            case THING_REQ:
                switch (request.getThingReq().getReqCase()) {
                    case THING_DELETE_REQ:
                    case THING_SET_HAS_REQ:
                    case THING_UNSET_HAS_REQ:
                    case RELATION_ADD_ROLE_PLAYER_REQ:
                    case RELATION_REMOVE_ROLE_PLAYER_REQ:
                        return true;
                    case THING_GET_HAS_REQ:
                    case THING_GET_RELATIONS_REQ:
                    case THING_GET_PLAYING_REQ:
                    case RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ:
                    case RELATION_GET_ROLE_PLAYERS_REQ:
                    case RELATION_GET_RELATING_REQ:
                    case ATTRIBUTE_GET_OWNERS_REQ:
                        return false;
                }
                break;
            case STREAM_REQ:
                return false;
            case QUERY_MANAGER_REQ:
                switch (request.getQueryManagerReq().getReqCase()) {
                    case DEFINE_REQ:
                    case UNDEFINE_REQ:
                    case INSERT_REQ:
                    case DELETE_REQ:
                    case UPDATE_REQ:
                        return true;
                    case GET_REQ:
                    case GET_AGGREGATE_REQ:
                    case GET_GROUP_REQ:
                    case GET_GROUP_AGGREGATE_REQ:
                    case FETCH_REQ:
                    case EXPLAIN_REQ:
                        return false;
                }
                break;
        }
        throw TypeDBException.of(ILLEGAL_ARGUMENT);
    }

    private void timeout() {
        close(TypeDBException.of(TRANSACTION_EXCEEDED_MAX_SECONDS, MILLISECONDS.toSeconds(options.transactionTimeoutMillis())));
    }

    @Override
    public void close() {
        if (isRPCAlive.compareAndSet(true, false)) {
            if (isTransactionOpen.compareAndSet(true, false)) {
                transaction.close();
                sessionSvc.closed(this);
            }
            if (scheduledTimeout != null) scheduledTimeout.cancel(false);
            responder.onCompleted();
        }
    }

    public void close(Throwable error) {
        if (isRPCAlive.compareAndSet(true, false)) {
            if (isTransactionOpen.compareAndSet(true, false)) {
                transaction.close();
                sessionSvc.closed(this);
            }
            if (scheduledTimeout != null) scheduledTimeout.cancel(false);
            responder.onError(ResponseBuilder.exception(error));
            // TODO: We should restrict the type of errors that we log.
            //       Expected error handling from the server side does not need to be logged - they create noise.
            if (isClientCancelled(error)) LOG.debug(error.getMessage(), error);
            else {
                LOG.error(error.getMessage().trim());
            }
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

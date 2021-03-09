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

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.server.common.ResponseBuilder;
import grakn.core.server.concept.ConceptService;
import grakn.core.server.concept.ThingService;
import grakn.core.server.concept.TypeService;
import grakn.core.server.logic.LogicService;
import grakn.core.server.logic.RuleService;
import grakn.core.server.query.QueryService;
import grakn.core.server.session.SessionService;
import grakn.protocol.TransactionProto;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static grakn.core.common.exception.ErrorMessage.Server.DUPLICATE_REQUEST;
import static grakn.core.common.exception.ErrorMessage.Server.ITERATION_WITH_UNKNOWN_ID;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.BAD_TRANSACTION_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.server.common.RequestReader.setDefaultOptions;

public class TransactionService {

    private final Grakn.Transaction transaction;
    private final SessionService sessionSrv;
    private final TransactionStream stream;
    private final Iterators iterators;
    private final Services services;
    private final AtomicBoolean isOpen;
    private final AtomicBoolean commitRequested;
    private final AtomicInteger ongoingRequests;
    private final int latencyMillis;
    private volatile String commitRequestID;

    public TransactionService(SessionService sessionSrv, TransactionStream stream, TransactionProto.Transaction.Open.Req request) {
        this.sessionSrv = sessionSrv;
        this.stream = stream;
        this.latencyMillis = request.getLatencyMillis();

        Arguments.Transaction.Type transactionType = Arguments.Transaction.Type.of(request.getType().getNumber());
        if (transactionType == null) throw GraknException.of(BAD_TRANSACTION_TYPE, request.getType());
        Options.Transaction options = setDefaultOptions(new Options.Transaction().parent(sessionSrv.options()), request.getOptions());

        transaction = sessionSrv.session().transaction(transactionType, options);
        isOpen = new AtomicBoolean(true);
        iterators = new Iterators();
        services = new Services();
        commitRequested = new AtomicBoolean(false);
        ongoingRequests = new AtomicInteger(0);
    }

    public Context.Transaction context() {
        return transaction.context();
    }

    Event event(TransactionProto.Transaction.Req request) {
        register(request);
        return new Event(this, request);
    }

    private void register(TransactionProto.Transaction.Req request) {
        try {
            switch (request.getReqCase()) {
                case OPEN_REQ:
                    throw GraknException.of(TRANSACTION_ALREADY_OPENED);
                case COMMIT_REQ:
                    commitRequestID = request.getId();
                    commitRequested.set(true);
                    break;
                case ITERATE_REQ:
                case ROLLBACK_REQ:
                case QUERY_REQ:
                case CONCEPT_MANAGER_REQ:
                case LOGIC_MANAGER_REQ:
                case THING_REQ:
                case TYPE_REQ:
                case RULE_REQ:
                    ongoingRequests.incrementAndGet();
                    break;
                default:
                case REQ_NOT_SET:
                    throw GraknException.of(UNKNOWN_REQUEST_TYPE);
            }
        } catch (Exception ex) {
            close(ex);
        }
    }

    void execute(TransactionProto.Transaction.Reqs requests) {
        requests.getTransactionReqsList().forEach(this::execute);
    }

    void execute(TransactionProto.Transaction.Req request) {
        try {
            switch (request.getReqCase()) {
                case REQ_NOT_SET:
                    throw GraknException.of(UNKNOWN_REQUEST_TYPE);
                case OPEN_REQ:
                    throw GraknException.of(TRANSACTION_ALREADY_OPENED);
                case COMMIT_REQ:
                    commitRequestID = request.getId();
                    commit();
                    break;
                default:
                    executeRequest(request);
            }
        } catch (Exception ex) {
            close(ex);
        }
    }

    // TODO: Enable this along with AsyncTransactionExecutor
    void executeAsync(TransactionProto.Transaction.Req request) {
        try {
            switch (request.getReqCase()) {
                case REQ_NOT_SET:
                    throw GraknException.of(UNKNOWN_REQUEST_TYPE);
                case OPEN_REQ:
                    throw GraknException.of(TRANSACTION_ALREADY_OPENED);
                case COMMIT_REQ:
                    mayExecuteCommit();
                    break;
                default:
                    executeRequestAndMayCommit(request);
            }
        } catch (Exception ex) {
            close(ex);
        }
    }

    private void mayExecuteCommit() {
        if (ongoingRequests.get() == 0 && commitRequested.compareAndSet(true, false)) commit();
    }

    private void executeRequestAndMayCommit(TransactionProto.Transaction.Req request) {
        executeRequest(request);
        if (ongoingRequests.decrementAndGet() == 0 && commitRequested.compareAndSet(true, false)) commit();
    }

    private void executeRequest(TransactionProto.Transaction.Req request) {
        switch (request.getReqCase()) {
            case ITERATE_REQ:
                iterators.continueIteration(request.getId());
                break;
            case ROLLBACK_REQ:
                rollback(request.getId());
                break;
            case QUERY_REQ:
                services.query.execute(request);
                break;
            case CONCEPT_MANAGER_REQ:
                services.concept.execute(request);
                break;
            case LOGIC_MANAGER_REQ:
                services.logic.execute(request);
                break;
            case THING_REQ:
                services.thing.execute(request);
                break;
            case TYPE_REQ:
                services.type.execute(request);
                break;
            case RULE_REQ:
                services.rule.execute(request);
                break;
            default:
                throw GraknException.of(ILLEGAL_ARGUMENT);
        }
    }

    private void commit() {
        transaction.commit();
        respond(TransactionProto.Transaction.Res.newBuilder().setId(commitRequestID).setCommitRes(
                TransactionProto.Transaction.Commit.Res.getDefaultInstance()
        ).build());
        close();
    }

    private void rollback(String requestId) {
        transaction.rollback();
        respond(TransactionProto.Transaction.Res.newBuilder().setId(requestId).setRollbackRes(
                TransactionProto.Transaction.Rollback.Res.getDefaultInstance()
        ).build());
    }

    public void close() {
        if (isOpen.compareAndSet(true, false)) {
            transaction.close();
            stream.close();
            sessionSrv.remove(this);
        }
    }

    public void close(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            transaction.close();
            stream.close(error);
            sessionSrv.remove(this);
        }
    }

    public void respond(TransactionProto.Transaction.Res response) {
        stream.responder().onNext(response);
    }

    public <T> void respond(TransactionProto.Transaction.Req request, Iterator<T> iterator,
                            Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn) {
        iterators.iterate(request, iterator, responseBuilderFn);
    }

    public <T> void respond(TransactionProto.Transaction.Req request, Iterator<T> iterator, Context.Query context,
                            Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn) {
        iterators.iterate(request, iterator, context.options().prefetch(),
                          context.options().responseBatchSize(), responseBuilderFn);
    }

    private class Services {

        private final ConceptService concept;
        private final LogicService logic;
        private final QueryService query;
        private final ThingService thing;
        private final TypeService type;
        private final RuleService rule;

        private Services() {
            concept = new ConceptService(TransactionService.this, transaction.concepts());
            logic = new LogicService(TransactionService.this, transaction.logic());
            query = new QueryService(TransactionService.this, transaction.query());
            thing = new ThingService(TransactionService.this, transaction.concepts());
            type = new TypeService(TransactionService.this, transaction.concepts());
            rule = new RuleService(TransactionService.this, transaction.logic());
        }
    }

    static class Event {

        private final TransactionService transactionSrv;
        private final TransactionProto.Transaction.Req request;

        private Event(TransactionService transactionSrv, TransactionProto.Transaction.Req request) {
            this.transactionSrv = transactionSrv;
            this.request = request;
        }

        TransactionService transactionService() {
            return transactionSrv;
        }

        TransactionProto.Transaction.Req request() {
            return request;
        }
    }

    /**
     * Contains a mutable map of iterators of TransactionProto.Transaction.Res for GRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
     * The iterators operate by batching results to reduce total round-trips.
     */
    private class Iterators {

        private final ConcurrentMap<String, BatchingIterator<?>> iterators = new ConcurrentHashMap<>();

        /**
         * Spin up a new iterator and begin streaming responses immediately.
         */
        <T> void iterate(TransactionProto.Transaction.Req request, Iterator<T> iterator,
                         Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn) {
            int size = transaction.context().options().responseBatchSize();
            iterate(request, iterator, true, size, responseBuilderFn);
        }

        /**
         * Spin up a new iterator.
         *
         * @param request           The request that this iterator is serving.
         * @param iterator          The iterator that contains the raw answers from the database.
         * @param prefetch          If set to true, the first batch will be streamed to the client immediately.
         * @param batchSize         The base batch size, before network latency is accounted for.
         * @param responseBuilderFn The projection function that serialises raw answers to RPC messages.
         * @param <T>               The type of answers being fetched.
         */
        <T> void iterate(TransactionProto.Transaction.Req request, Iterator<T> iterator, boolean prefetch, int batchSize,
                         Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn) {
            String requestId = request.getId();
            BatchingIterator<T> batchingIterator =
                    new BatchingIterator<>(requestId, iterator, responseBuilderFn, batchSize, latencyMillis);
            iterators.compute(requestId, (key, oldValue) -> {
                if (oldValue == null) return batchingIterator;
                else throw GraknException.of(DUPLICATE_REQUEST, requestId);
            });
            if (prefetch) batchingIterator.iterateBatch();
            else respond(ResponseBuilder.Transaction.iterate(requestId, true));
        }

        /**
         * Instruct an existing iterator to iterate its next batch.
         */
        void continueIteration(String requestId) {
            BatchingIterator<?> iterator = iterators.get(requestId);
            if (iterator == null) throw GraknException.of(ITERATION_WITH_UNKNOWN_ID, requestId);
            iterator.iterateBatch();
        }

        private class BatchingIterator<T> {

            private static final int MAX_LATENCY_MILLIS = 3000;

            private final String id;
            private final Iterator<T> iterator;
            private final Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn;
            private final int batchSize;
            private final int latencyMillis;

            BatchingIterator(String id, Iterator<T> iterator, Function<List<T>, TransactionProto.Transaction.Res> responseBuilderFn, int batchSize, int latencyMillis) {
                this.id = id;
                this.iterator = iterator;
                this.responseBuilderFn = responseBuilderFn;
                this.batchSize = batchSize;
                this.latencyMillis = Math.min(latencyMillis, MAX_LATENCY_MILLIS);
            }

            // TODO: this needs to broken into multiple functions
            synchronized void iterateBatch() {
                List<T> answers = new ArrayList<>();
                Instant startTime = Instant.now();
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                    // TODO: what if the next answer comes long after 1ms? We would wait inefficiently while holding onto answers we can already send sooner
                    answers.add(iterator.next());
                    Instant currTime = Instant.now();
                    if (Duration.between(currTime, startTime).toMillis() >= 1) {
                        respond(responseBuilderFn.apply(answers));
                        answers.clear();
                        startTime = currTime;
                    }
                }

                if (!answers.isEmpty()) {
                    respond(responseBuilderFn.apply(answers));
                    answers.clear();
                }

                if (!iterator.hasNext()) {
                    respond(ResponseBuilder.Transaction.iterate(id, false));
                    return;
                }

                respond(ResponseBuilder.Transaction.iterate(id, true));

                // Compensate for network latency
                answers.clear();
                Instant endTime = Instant.now().plusMillis(latencyMillis);
                while (iterator.hasNext() && Instant.now().isBefore(endTime)) {
                    answers.add(iterator.next());
                    Instant currTime = Instant.now();
                    if (Duration.between(currTime, startTime).toMillis() >= 1) {
                        respond(responseBuilderFn.apply(answers));
                        answers.clear();
                        startTime = currTime;
                    }
                }

                if (!answers.isEmpty()) {
                    respond(responseBuilderFn.apply(answers));
                    answers.clear();
                }

                if (!iterator.hasNext()) {
                    respond(ResponseBuilder.Transaction.iterate(id, false));
                }
            }
        }
    }
}

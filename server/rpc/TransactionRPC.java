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
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.server.rpc.common.RequestReader;
import grakn.core.server.rpc.concept.ConceptManagerHandler;
import grakn.core.server.rpc.concept.ThingHandler;
import grakn.core.server.rpc.concept.TypeHandler;
import grakn.core.server.rpc.logic.LogicManagerHandler;
import grakn.core.server.rpc.logic.RuleHandler;
import grakn.core.server.rpc.query.QueryHandler;
import grakn.protocol.TransactionProto;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Server.DUPLICATE_REQUEST;
import static grakn.core.common.exception.ErrorMessage.Server.ITERATION_WITH_UNKNOWN_ID;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.BAD_TRANSACTION_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.server.rpc.common.ResponseBuilder.Transaction.continueRes;
import static grakn.core.server.rpc.common.ResponseBuilder.Transaction.done;

public class TransactionRPC {

    private final Grakn.Transaction transaction;
    private final SessionRPC sessionRPC;
    private final TransactionStream stream;
    private final Iterators iterators;
    private final RequestHandlers handlers;
    private final AtomicBoolean isOpen;

    TransactionRPC(SessionRPC sessionRPC, TransactionStream stream, TransactionProto.Transaction.Open.Req request) {
        this.sessionRPC = sessionRPC;
        this.stream = stream;

        Arguments.Transaction.Type transactionType = Arguments.Transaction.Type.of(request.getType().getNumber());
        if (transactionType == null) throw GraknException.of(BAD_TRANSACTION_TYPE, request.getType());
        Options.Transaction transactionOptions = RequestReader.getOptions(Options.Transaction::new, request.getOptions());

        transaction = sessionRPC.session().transaction(transactionType, transactionOptions);
        isOpen = new AtomicBoolean(true);
        iterators = new Iterators();
        handlers = new RequestHandlers();
    }

    public Context.Transaction context() {
        return transaction.context();
    }

    SessionRPC sessionRPC() {
        return sessionRPC;
    }

    void handleRequest(TransactionProto.Transaction.Req request) {
        try {
            switch (request.getReqCase()) {
                case CONTINUE:
                    iterators.continueIteration(request.getId());
                    return;
                case OPEN_REQ:
                    throw GraknException.of(TRANSACTION_ALREADY_OPENED);
                case COMMIT_REQ:
                    commit(request.getId());
                    return;
                case ROLLBACK_REQ:
                    rollback(request.getId());
                    return;
                case QUERY_REQ:
                    try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread("query")) {
                        handlers.query.handleRequest(request);
                    }
                    return;
                case CONCEPT_MANAGER_REQ:
                    handlers.conceptMgr.handleRequest(request);
                    return;
                case LOGIC_MANAGER_REQ:
                    handlers.logicMgr.handleRequest(request);
                    return;
                case THING_REQ:
                    handlers.thing.handleRequest(request);
                    return;
                case TYPE_REQ:
                    handlers.type.handleRequest(request);
                    return;
                case RULE_REQ:
                    handlers.rule.handleRequest(request);
                    return;
//            case EXPLANATION_REQ:
//                explanation(request.getExplanationReq());
//                return;
                default:
                case REQ_NOT_SET:
                    throw GraknException.of(UNKNOWN_REQUEST_TYPE);
            }
        } catch (Exception ex) {
            closeWithError(ex);
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

    private void commit(String requestId) {
        transaction.commit();
        respond(TransactionProto.Transaction.Res.newBuilder().setId(requestId).setCommitRes(
                TransactionProto.Transaction.Commit.Res.getDefaultInstance()).build());
        close();
    }

    private void rollback(String requestId) {
        transaction.rollback();
        respond(TransactionProto.Transaction.Res.newBuilder().setId(requestId).setRollbackRes(
                TransactionProto.Transaction.Rollback.Res.getDefaultInstance()).build());
    }

    void close() {
        if (isOpen.compareAndSet(true, false)) {
            stream.close();
            transaction.close();
            sessionRPC.remove(this);
        }
    }

    void closeWithError(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            stream.closeWithError(error);
            transaction.close();
            sessionRPC.remove(this);
        }
    }

//    /**
//     * Reconstruct local ConceptMap and return the explanation associated with the ConceptMap provided by the user
//     */
//    private void explanation(AnswerProto.Explanation.Req explanationReq) {
//        responseSender.onError(Status.UNIMPLEMENTED.asException());
//        // TODO: implement TransactionListener.explanation()
//    }

    /**
     * Contains a mutable map of iterators of TransactionProto.Transaction.Res for gRPC. These iterators are used for returning
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
            int latencyMillis = request.getLatencyMillis();
            BatchingIterator<T> batchingIterator =
                    new BatchingIterator<>(requestId, iterator, responseBuilderFn, batchSize, latencyMillis);
            iterators.compute(requestId, (key, oldValue) -> {
                if (oldValue == null) return batchingIterator;
                else throw GraknException.of(DUPLICATE_REQUEST, requestId);
            });
            if (prefetch) batchingIterator.iterateBatch();
            else respond(continueRes(requestId));
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

            synchronized void iterateBatch() {
                List<T> answers = new ArrayList<>();
                Instant startTime = Instant.now();
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
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
                    respond(done(id));
                    return;
                }

                respond(continueRes(id));

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
                    respond(done(id));
                }
            }
        }
    }

    private class RequestHandlers {
        private final ConceptManagerHandler conceptMgr;
        private final LogicManagerHandler logicMgr;
        private final QueryHandler query;
        private final ThingHandler thing;
        private final TypeHandler type;
        private final RuleHandler rule;

        private RequestHandlers() {
            conceptMgr = new ConceptManagerHandler(TransactionRPC.this, transaction.concepts());
            logicMgr = new LogicManagerHandler(TransactionRPC.this, transaction.logic());
            query = new QueryHandler(TransactionRPC.this, transaction.query());
            thing = new ThingHandler(TransactionRPC.this, transaction.concepts());
            type = new TypeHandler(TransactionRPC.this, transaction.concepts());
            rule = new RuleHandler(TransactionRPC.this, transaction.logic());
        }
    }
}

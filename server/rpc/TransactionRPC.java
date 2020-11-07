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
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.server.rpc.concept.ConceptManagerHandler;
import grakn.core.server.rpc.concept.RuleHandler;
import grakn.core.server.rpc.concept.ThingHandler;
import grakn.core.server.rpc.concept.TypeHandler;
import grakn.core.server.rpc.query.QueryHandler;
import grakn.core.server.rpc.util.RequestReader;
import grakn.protocol.TransactionProto;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Server.DUPLICATE_REQUEST;
import static grakn.core.common.exception.ErrorMessage.Server.ITERATION_WITH_UNKNOWN_ID;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.BAD_TRANSACTION_TYPE;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.server.rpc.util.ResponseBuilder.Transaction.continueRes;
import static grakn.core.server.rpc.util.ResponseBuilder.Transaction.done;

public class TransactionRPC {

    private final Grakn.Transaction transaction;
    private final SessionRPC sessionRPC;
    private final TransactionStream stream;
    private final Iterators iterators;
    private final RequestHandlers handlers;

    TransactionRPC(SessionRPC sessionRPC, TransactionStream stream, TransactionProto.Transaction.Open.Req request) {
        this.sessionRPC = sessionRPC;
        this.stream = stream;

        final Arguments.Transaction.Type transactionType = Arguments.Transaction.Type.of(request.getType().getNumber());
        if (transactionType == null) throw new GraknException(BAD_TRANSACTION_TYPE.message(request.getType()));
        final Options.Transaction transactionOptions = RequestReader.getOptions(Options.Transaction::new, request.getOptions());

        transaction = sessionRPC.session().transaction(transactionType, transactionOptions);
        iterators = new Iterators();
        handlers = new RequestHandlers();
    }

    SessionRPC sessionRPC() {
        return sessionRPC;
    }

    void handleRequest(TransactionProto.Transaction.Req request) {
        switch (request.getReqCase()) {
            case CONTINUE:
                iterators.continueIteration(request.getId());
                return;
            case OPEN_REQ:
                throw new GraknException(TRANSACTION_ALREADY_OPENED);
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
                handlers.conceptManager.handleRequest(request);
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
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    public void respond(TransactionProto.Transaction.Res response) {
        stream.responder().onNext(response);
    }

    public void respond(TransactionProto.Transaction.Req request, Iterator<TransactionProto.Transaction.Res> iterator) {
        iterators.beginIteration(request, iterator);
    }

    public void respond(TransactionProto.Transaction.Req request, Iterator<TransactionProto.Transaction.Res> iterator, Options.Query queryOptions) {
        iterators.beginIteration(request, iterator, queryOptions.batchSize());
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
        stream.close();
        closeResources();
    }

    void closeWithError(Throwable error) {
        stream.closeWithError(error);
        closeResources();
    }

    private void closeResources() {
        transaction.close();
        sessionRPC.remove(this);
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

        private final ConcurrentMap<String, BatchingIterator> iterators = new ConcurrentHashMap<>();

        /**
         * Spin up an iterator and begin batch iterating.
         */
        void beginIteration(TransactionProto.Transaction.Req request, Iterator<TransactionProto.Transaction.Res> iterator) {
            beginIteration(request, iterator, transaction.options().batchSize());
        }

        void beginIteration(TransactionProto.Transaction.Req request, Iterator<TransactionProto.Transaction.Res> iterator, int batchSize) {
            final String requestId = request.getId();
            final int latencyMillis = request.getLatencyMillis();
            final BatchingIterator batchingIterator = new BatchingIterator(requestId, iterator, batchSize, latencyMillis);
            iterators.compute(requestId, (key, oldValue) -> {
                if (oldValue == null) return batchingIterator;
                else throw new GraknException(DUPLICATE_REQUEST.message(requestId));
            });
            batchingIterator.iterateBatch();
        }

        /**
         * Instruct an existing iterator to iterate another batch.
         */
        void continueIteration(String requestId) {
            final BatchingIterator iterator = iterators.get(requestId);
            if (iterator == null) throw new GraknException(ITERATION_WITH_UNKNOWN_ID.message(requestId));
            iterator.iterateBatch();
        }

        private class BatchingIterator {
            private static final int MAX_LATENCY_MILLIS = 3000;

            private final String id;
            private final Iterator<TransactionProto.Transaction.Res> iterator;
            private final int batchSize;
            private final int latencyMillis;

            BatchingIterator(String id, Iterator<TransactionProto.Transaction.Res> iterator, int batchSize, int latencyMillis) {
                this.id = id;
                this.iterator = iterator;
                this.batchSize = batchSize;
                this.latencyMillis = Math.min(latencyMillis, MAX_LATENCY_MILLIS);
            }

            synchronized void iterateBatch() {
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                    respond(iterator.next());
                }

                if (!iterator.hasNext()) {
                    iterators.remove(id);
                    respond(done(id));
                    return;
                }

                respond(continueRes(id));

                // Compensate for network latency
                final Instant endTime = Instant.now().plusMillis(latencyMillis);
                while (iterator.hasNext() && Instant.now().isBefore(endTime)) {
                    respond(iterator.next());
                }

                if (!iterator.hasNext()) {
                    iterators.remove(id);
                    respond(done(id));
                }
            }

        }
    }

    private class RequestHandlers {
        private final ConceptManagerHandler conceptManager;
        private final QueryHandler query;
        private final ThingHandler thing;
        private final TypeHandler type;
        private final RuleHandler rule;

        private RequestHandlers() {
            conceptManager = new ConceptManagerHandler(TransactionRPC.this, transaction.concepts());
            query = new QueryHandler(TransactionRPC.this, transaction.query());
            thing = new ThingHandler(TransactionRPC.this, transaction.concepts());
            type = new TypeHandler(TransactionRPC.this, transaction.concepts());
            rule = new RuleHandler(TransactionRPC.this, transaction.concepts());
        }
    }
}

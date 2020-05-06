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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grabl.tracing.client.GrablTracing;
import grabl.tracing.client.GrablTracingThreadStatic;
import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.concept.answer.Explanation;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.exception.TransactionException;
import grakn.protocol.session.AnswerProto;
import grakn.protocol.session.SessionProto;
import grakn.protocol.session.SessionProto.Transaction;
import grakn.protocol.session.SessionServiceGrpc;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlQuery;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;


/**
 * Grakn RPC Session Service
 */
public class SessionService extends SessionServiceGrpc.SessionServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);

    private static final int DEFAULT_BATCH_SIZE = 50;

    private final OpenRequest requestOpener;
    // Each client's connection obtains a unique ID, which we map to the shared session under the hood
    // if connecting to the same keyspace
    // Additionally, each client's remote session maps to a set of open transactions that we close when the client closes
    // their transaction
    private final Map<String, Session> openSessions;
    // The following map associates SessionId to a collection of TransactionListeners so that:
    //     - if the user wants to stop the server, we can forcefully close all the connections to clients using active transactions.
    //     - if a client abruptly closes a connection (usually because of crashes on client side) we can kill all the threads associated
    //       to transactions previously opened by the client and that were not properly closed before the abrupt closure (see onError()).
    private Map<String, Set<TransactionListener>> transactionListeners;

    public SessionService(OpenRequest requestOpener) {
        this.requestOpener = requestOpener;
        this.openSessions = new HashMap<>();
        this.transactionListeners = new HashMap<>();
    }

    /**
     * Close all open transactions, sessions and connections with clients - this is invoked by JVM shutdown hook
     */
    public void shutdown() {
        transactionListeners.values()
                .forEach(transactionListenerSet ->
                        transactionListenerSet.forEach(transactionListener -> transactionListener.close(null)));
        transactionListeners.clear();
        openSessions.values().forEach(Session::close);
    }

    @Override
    public StreamObserver<Transaction.Req> transaction(StreamObserver<Transaction.Res> responseSender) {
        return new TransactionListener(responseSender, openSessions);
    }

    @Override
    public void open(SessionProto.Session.Open.Req request, StreamObserver<SessionProto.Session.Open.Res> responseObserver) {
        try {
            String keyspace = request.getKeyspace();
            Session session = requestOpener.open(request);
            String sessionId = keyspace + UUID.randomUUID().toString();
            openSessions.put(sessionId, session);
            transactionListeners.put(sessionId, new HashSet<>());
            responseObserver.onNext(SessionProto.Session.Open.Res.newBuilder().setSessionId(sessionId).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.error("An error has occurred", e);
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void close(SessionProto.Session.Close.Req request, StreamObserver<SessionProto.Session.Close.Res> responseObserver) {
        try {
            String sessionId = request.getSessionId();
            transactionListeners.remove(sessionId).forEach(transactionListener -> transactionListener.close(null));
            openSessions.remove(sessionId).close();
            responseObserver.onNext(SessionProto.Session.Close.Res.newBuilder().build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.error("An error has occurred", e);
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }


    /**
     * A StreamObserver that implements the transaction-handling behaviour for io.grpc.Server.
     * Receives a stream of Transaction.Reqs and returning a stream of Transaction.Ress.
     */
    class TransactionListener implements StreamObserver<Transaction.Req> {
        final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);
        private final StreamObserver<Transaction.Res> responseSender;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final ExecutorService threadExecutor;
        private final Map<String, Session> openSessions;
        private final Iterators iterators = new Iterators(this::onNextResponse);

        @Nullable
        private grakn.core.kb.server.Transaction tx = null;
        private String sessionId;

        TransactionListener(StreamObserver<Transaction.Res> responseSender, Map<String, Session> openSessions) {
            this.responseSender = responseSender;
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("transaction-listener").build();
            this.threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
            this.openSessions = openSessions;
        }


        private <T> T nonNull(@Nullable T item) {
            if (item == null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            } else {
                return item;
            }
        }

        @Override
        public void onNext(Transaction.Req request) {
            // !important: this is the gRPC thread
            if (GrablTracingThreadStatic.isTracingEnabled()) {
                Map<String, String> metadata = request.getMetadataMap();
                String rootId = metadata.get("traceRootId");
                String parentId = metadata.get("traceParentId");
                if (rootId != null && parentId != null) {
                    submit(() -> handleRequest(request, GrablTracingThreadStatic.getGrablTracing()
                            .trace(UUID.fromString(rootId), UUID.fromString(parentId), "received")));
                    return;
                }
            }
            submit(() -> handleRequest(request));
        }

        @Override
        public void onError(Throwable t) {
            // This method is invoked when a client abruptly terminates a connection to the server
            // so we want to make sure to also close and delete the current session and
            // all the transactions associated to the same client.
            transactionListeners.remove(sessionId).forEach(transactionListener -> transactionListener.close(t));
            Session session = openSessions.remove(sessionId);
            session.close();


            // TODO: This might create issues if a session is used by other concurrent clients,
            // the better approach would be to signal a closure intent to the session and the session should be able to
            // detect whether it's been used by other connections, if not close it completely.
        }

        @Override
        public void onCompleted() {
            transactionListeners.get(sessionId).remove(this);
            close(null);
        }

        private void handleRequest(Transaction.Req request, GrablTracing.Trace queueTrace) {
            try (ThreadTrace trace = GrablTracingThreadStatic.continueTraceOnThread(
                    queueTrace.getRootId(), queueTrace.getId(), "handle")
            ) {
                queueTrace.end();
                handleRequest(request);
            }
        }

        private void handleRequest(Transaction.Req request) {
            try {
                switch (request.getReqCase()) {
                    case OPEN_REQ:
                        open(request.getOpenReq());
                        break;
                    case COMMIT_REQ:
                        commit();
                        break;
                    case ITER_REQ:
                        handleIterRequest(request.getIterReq());
                        break;
                    case GETSCHEMACONCEPT_REQ:
                        getSchemaConcept(request.getGetSchemaConceptReq());
                        break;
                    case GETCONCEPT_REQ:
                        getConcept(request.getGetConceptReq());
                        break;
                    case PUTENTITYTYPE_REQ:
                        putEntityType(request.getPutEntityTypeReq());
                        break;
                    case PUTATTRIBUTETYPE_REQ:
                        putAttributeType(request.getPutAttributeTypeReq());
                        break;
                    case PUTRELATIONTYPE_REQ:
                        putRelationType(request.getPutRelationTypeReq());
                        break;
                    case PUTROLE_REQ:
                        putRole(request.getPutRoleReq());
                        break;
                    case PUTRULE_REQ:
                        putRule(request.getPutRuleReq());
                        break;
                    case CONCEPTMETHOD_REQ:
                        conceptMethod(request.getConceptMethodReq());
                        break;
                    case EXPLANATION_REQ:
                        explanation(request.getExplanationReq());
                        break;
                    default:
                    case REQ_NOT_SET:
                        throw ResponseBuilder.exception(Status.INVALID_ARGUMENT);
                }
            } catch (Throwable e) {
                close(e);
            }
        }

        public void handleIterRequest(Transaction.Iter.Req request) {
            try {
                switch (request.getReqCase()) {
                    case ITERATORID:
                        iterators.resumeBatchIterating(request.getIteratorId(), request.getOptions());
                        break;
                    case QUERY_ITER_REQ:
                        query(request.getQueryIterReq(), request.getOptions());
                        break;
                    case CONCEPTMETHOD_ITER_REQ:
                        conceptIterMethod(request.getConceptMethodIterReq(), request.getOptions());
                        break;
                    case GETATTRIBUTES_ITER_REQ:
                        getAttributes(request.getGetAttributesIterReq(), request.getOptions());
                        break;
                    default:
                    case REQ_NOT_SET:
                        throw ResponseBuilder.exception(Status.INVALID_ARGUMENT);
                }
            } catch (Throwable e) {
                close(e);
            }
        }

        public void close(@Nullable Throwable error) {
            if (!terminated.getAndSet(true)) {
                if (tx != null) {
                    tx.close();
                }

                if (error != null) {
                    LOG.error("Runtime Exception in RPC TransactionListener: ", error);
                    responseSender.onError(ResponseBuilder.exception(error));
                } else {
                    responseSender.onCompleted();
                }

                threadExecutor.shutdownNow();
                try {
                    boolean terminated = threadExecutor.awaitTermination(30, TimeUnit.SECONDS);
                    if (!terminated) {
                        LOG.warn("Some tasks did not terminate within the timeout period.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void submit(Runnable runnable) {
            threadExecutor.submit(runnable);
        }

        private void open(Transaction.Open.Req request) {
            if (tx != null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            }

            sessionId = request.getSessionId();
            transactionListeners.get(sessionId).add(this);

            Session session = openSessions.get(sessionId);

            Transaction.Type type = request.getType();
            if (type != null && type.equals(Transaction.Type.WRITE)) {
                tx = session.transaction(grakn.core.kb.server.Transaction.Type.WRITE);
            } else if (type != null && type.equals(Transaction.Type.READ)) {
                tx = session.transaction(grakn.core.kb.server.Transaction.Type.READ);
            } else {
                throw TransactionException.create("Invalid Transaction Type");
            }

            Transaction.Res response = ResponseBuilder.Transaction.open();
            responseSender.onNext(response);        }

        private void commit() {
            tx().commit();
            onNextResponse(ResponseBuilder.Transaction.commit());
        }

        private void query(SessionProto.Transaction.Query.Iter.Req request, SessionProto.Transaction.Iter.Req.Options options) {
            Transaction.Res response;
            try (ThreadTrace trace = traceOnThread("query")) {
                GraqlQuery query;
                try (ThreadTrace parse = traceOnThread("parse")) {
                    query = Graql.parse(request.getQuery());
                }

                try (ThreadTrace stream = traceOnThread("stream")) {
                    Stream<Transaction.Res> responseStream = tx().stream(query, request.getInfer().equals(Transaction.Query.INFER.TRUE)).map(ResponseBuilder.Transaction.Iter::query);

                    iterators.startBatchIterating(responseStream.iterator(), options);
                }
            }
        }

        private void getSchemaConcept(Transaction.GetSchemaConcept.Req request) {
            Concept concept = tx().getSchemaConcept(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.getSchemaConcept(concept);
            onNextResponse(response);
        }

        private void getConcept(Transaction.GetConcept.Req request) {
            Concept concept = tx().getConcept(ConceptId.of(request.getId()));
            Transaction.Res response = ResponseBuilder.Transaction.getConcept(concept);
            onNextResponse(response);
        }

        private void getAttributes(Transaction.GetAttributes.Iter.Req request, Transaction.Iter.Req.Options options) {
            Object value = request.getValue().getAllFields().values().iterator().next();
            Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

            Iterator<Transaction.Res> iterator = attributes.stream().map(ResponseBuilder.Transaction.Iter::getAttributes).iterator();
            iterators.startBatchIterating(iterator, options);
        }

        private void putEntityType(Transaction.PutEntityType.Req request) {
            EntityType entityType = tx().putEntityType(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.putEntityType(entityType);
            onNextResponse(response);
        }

        private void putAttributeType(Transaction.PutAttributeType.Req request) {
            Label label = Label.of(request.getLabel());
            AttributeType.ValueType<?> valueType = ResponseBuilder.Concept.VALUE_TYPE(request.getValueType());

            AttributeType<?> attributeType = tx().putAttributeType(label, valueType);
            Transaction.Res response = ResponseBuilder.Transaction.putAttributeType(attributeType);
            onNextResponse(response);
        }

        private void putRelationType(Transaction.PutRelationType.Req request) {
            RelationType relationType = tx().putRelationType(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.putRelationType(relationType);
            onNextResponse(response);
        }

        private void putRole(Transaction.PutRole.Req request) {
            Role role = tx().putRole(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.putRole(role);
            onNextResponse(response);
        }

        private void putRule(Transaction.PutRule.Req request) {
            Label label = Label.of(request.getLabel());
            Pattern when = Graql.parsePattern(request.getWhen());
            Pattern then = Graql.parsePattern(request.getThen());

            Rule rule = tx().putRule(label, when, then);
            Transaction.Res response = ResponseBuilder.Transaction.putRule(rule);
            onNextResponse(response);
        }

        private grakn.core.kb.server.Transaction tx() {
            return nonNull(tx);
        }

        private void conceptMethod(Transaction.ConceptMethod.Req request) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(request.getId())));
            ConceptMethod.run(concept, request.getMethod(), iterators, tx(), this::onNextResponse);
        }

        private void conceptIterMethod(Transaction.ConceptMethod.Iter.Req request, Transaction.Iter.Req.Options options) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(request.getId())));
            ConceptMethod.iter(concept, request.getMethod(), iterators, tx(), this::onNextResponse, options);
        }

        /**
         * Reconstruct and return the explanation associated with the ConceptMap provided by the user
         */
        private void explanation(AnswerProto.Explanation.Req explanationReq) {
            // extract and reconstruct query pattern with the required IDs
            AnswerProto.ConceptMap explainable = explanationReq.getExplainable();
            Pattern queryPattern = Graql.parsePattern(explainable.getPattern());
            Explanation explanation = tx.explanation(queryPattern);
            Transaction.Res response = ResponseBuilder.Transaction.explanation(explanation);
            onNextResponse(response);
        }

        private void onNextResponse(Transaction.Res response) {
            responseSender.onNext(response);
        }
    }

    /**
     * Contains a mutable map of iterators of Transaction.Res for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
     * The iterators operate by batching results to reduce total round-trips.
     */
    static class Iterators {
        private final Consumer<Transaction.Res> responseSender;
        private final AtomicInteger iteratorIdCounter = new AtomicInteger(0);
        private final Map<Integer, BatchingIterator> iterators = new ConcurrentHashMap<>();

        public Iterators(Consumer<Transaction.Res> responseSender) {
            this.responseSender = responseSender;
        }

        /**
         * Hand off an iterator and begin batch iterating.
         */
        public void startBatchIterating(Iterator<Transaction.Res> iterator, @Nullable Transaction.Iter.Req.Options options) {
            new BatchingIterator(iterator).iterateBatch(options);
        }

        /**
         * Iterate the next batch of an existing iterator.
         */
        public void resumeBatchIterating(int iteratorId, Transaction.Iter.Req.Options options) {
            BatchingIterator iterator = iterators.get(iteratorId);
            if (iterator == null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            }
            iterator.iterateBatch(options);
        }

        public void stop(int iteratorId) {
            iterators.remove(iteratorId);
        }

        private int saveIterator(BatchingIterator iterator) {
            int id = iteratorIdCounter.incrementAndGet();
            iterators.put(id, iterator);
            return id;
        }

        public class BatchingIterator {
            private int id = 0;
            private final Iterator<Transaction.Res> iterator;

            public BatchingIterator(Iterator<Transaction.Res> iterator) {
                this.iterator = iterator;
            }

            private boolean isSaved() {
                return id != 0;
            }

            private void save() {
                if (!isSaved()) {
                    id = saveIterator(this);
                }
            }

            private void end() {
                if (isSaved()) {
                    stop(id);
                }
            }

            public void iterateBatch(@Nullable Transaction.Iter.Req.Options options) {
                int batchSize = getSizeFrom(options);
                for (int i = 0; i < batchSize && iterator.hasNext(); i++){
                    responseSender.accept(iterator.next());
                }

                if (iterator.hasNext()) {
                    save();
                    responseSender.accept(SessionProto.Transaction.Res.newBuilder()
                            .setIterRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                    .setIteratorId(id)).build());
                } else {
                    end();
                    responseSender.accept(SessionProto.Transaction.Res.newBuilder()
                            .setIterRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                    .setDone(true)).build());
                }
            }
        }

        private static int getSizeFrom(@Nullable Transaction.Iter.Req.Options options) {
            if (options == null) {
                return DEFAULT_BATCH_SIZE;
            }
            switch (options.getBatchSizeCase()) {
                case ALL:
                    return Integer.MAX_VALUE;
                case NUMBER:
                    return options.getNumber();
                case BATCHSIZE_NOT_SET:
                default:
                    return DEFAULT_BATCH_SIZE;
            }
        }
    }
}

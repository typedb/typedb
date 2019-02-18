/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

import brave.Span;
import brave.propagation.TraceContext;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.protocol.SessionProto;
import grakn.core.protocol.SessionProto.Transaction;
import grakn.core.protocol.SessionServiceGrpc;
import grakn.core.server.Transaction.Type;
import grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
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
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


/**
 * Grakn RPC Session Service
 */
public class SessionService extends SessionServiceGrpc.SessionServiceImplBase {
    private final OpenRequest requestOpener;
    private final Map<String, SessionImpl> openSessions;
    private AttributeDeduplicatorDaemon attributeDeduplicatorDaemon;

    public SessionService(OpenRequest requestOpener, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon) {
        this.requestOpener = requestOpener;
        this.attributeDeduplicatorDaemon = attributeDeduplicatorDaemon;
        this.openSessions = new HashMap<>();
    }

    public StreamObserver<Transaction.Req> transaction(StreamObserver<Transaction.Res> responseSender) {
        return new TransactionListener(responseSender, attributeDeduplicatorDaemon, openSessions);
    }

    @Override
    public void open(SessionProto.Session.Open.Req request, StreamObserver<SessionProto.Session.Open.Res> responseObserver) {
        try {
            String keyspace = request.getKeyspace();
            SessionImpl session = requestOpener.open(request);
            String sessionId = keyspace + UUID.randomUUID().toString();
            openSessions.put(sessionId, session);
            responseObserver.onNext(SessionProto.Session.Open.Res.newBuilder().setSessionId(sessionId).build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }

    @Override
    public void close(SessionProto.Session.Close.Req request, StreamObserver<SessionProto.Session.Close.Res> responseObserver) {
        try {
            SessionImpl session = openSessions.remove(request.getSessionId());
            session.close();
            responseObserver.onNext(SessionProto.Session.Close.Res.newBuilder().build());
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(ResponseBuilder.exception(e));
        }
    }


    /**
     * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link io.grpc.Server}.
     * Receives a stream of {@link Transaction.Req}s and returning a stream of {@link Transaction.Res}s.
     */
    static class TransactionListener implements StreamObserver<Transaction.Req> {
        final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);
        private final StreamObserver<Transaction.Res> responseSender;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final ExecutorService threadExecutor;
        private AttributeDeduplicatorDaemon attributeDeduplicatorDaemon;
        private final Map<String, SessionImpl> openSessions;
        private final Iterators iterators = Iterators.create();

        private TraceContext receivedTraceContext;

        @Nullable
        private TransactionOLTP tx = null;

        public TransactionListener(StreamObserver<Transaction.Res> responseSender, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, Map<String, SessionImpl> openSessions) {
            this.responseSender = responseSender;
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("transaction-listener-%s").build();
            this.threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
            this.attributeDeduplicatorDaemon = attributeDeduplicatorDaemon;
            this.openSessions = openSessions;
        }


        private static <T> T nonNull(@Nullable T item) {
            if (item == null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            } else {
                return item;
            }
        }

        @Override
        public void onNext(Transaction.Req request) {
            // !important: this is the gRPC thread
            try {
//                if (ServerTracingInstrumentation.tracingEnabledFromMessage(request)) {
//                    TraceContext receivedTraceContext = ServerTracingInstrumentation.extractTraceContext(request);
//                    Span queueSpan = ServerTracingInstrumentation.createChildSpanWithParentContext("Server receive queue", receivedTraceContext);
//                    queueSpan.start();
//                    queueSpan.tag("childNumber", "0");
//
//                    // hop context & active Span across thread boundaries
//                    submit(() -> handleRequest(request, queueSpan, receivedTraceContext));
//                } else {
                    submit(() -> handleRequest(request));
//                }
            } catch (RuntimeException e) {
                close(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            close(t);
        }

        @Override
        public void onCompleted() {
            close(null);
        }

        private void handleRequest(Transaction.Req request, Span queueSpan, TraceContext context) {
            /* this method variant is only called if tracing is active */

            // close the Span from gRPC thread
            queueSpan.finish(); // time spent in queue

            // create a new scoped span
//            ScopedSpan span = ServerTracingInstrumentation.startScopedChildSpanWithParentContext("Server handle request", context);
//            span.tag("childNumber", "1");
            handleRequest(request);
        }

        private void handleRequest(Transaction.Req request) {
            switch (request.getReqCase()) {
                case OPEN_REQ:
                    open(request.getOpenReq());
                    break;
                case COMMIT_REQ:
                    commit();
                    break;
                case QUERY_REQ:
                    query(request.getQueryReq());
                    break;
                case ITERATE_REQ:
                    next(request.getIterateReq());
                    break;
                case GETSCHEMACONCEPT_REQ:
                    getSchemaConcept(request.getGetSchemaConceptReq());
                    break;
                case GETCONCEPT_REQ:
                    getConcept(request.getGetConceptReq());
                    break;
                case GETATTRIBUTES_REQ:
                    getAttributes(request.getGetAttributesReq());
                    break;
                case PUTENTITYTYPE_REQ:
                    putEntityType(request.getPutEntityTypeReq());
                    break;
                case PUTATTRIBUTETYPE_REQ:
                    putAttributeType(request.getPutAttributeTypeReq());
                    break;
                case PUTRELATIONTYPE_REQ:
                    putRelationshipType(request.getPutRelationTypeReq());
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
                default:
                case REQ_NOT_SET:
                    throw ResponseBuilder.exception(Status.INVALID_ARGUMENT);
            }
        }

        public void close(@Nullable Throwable error) {
            submit(() -> {
                if (tx != null) {
                    tx.close();
                }
            });

            if (!terminated.getAndSet(true)) {
                if (error != null) {
                    LOG.error("Runtime Exception in RPC TransactionListener: ", error);
                    responseSender.onError(ResponseBuilder.exception(error));
                } else {
                    responseSender.onCompleted();
                }
            }

            threadExecutor.shutdown();
        }

        private void submit(Runnable runnable) {
            try {
                threadExecutor.submit(runnable).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assert cause instanceof RuntimeException : "No checked exceptions are thrown, because it's a `Runnable`";
                throw (RuntimeException) cause;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void open(Transaction.Open.Req request) {
            if (tx != null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            }

            tx = openSessions.get(request.getSessionId()).transaction(Type.of(request.getType().getNumber()));
            Transaction.Res response = ResponseBuilder.Transaction.open();
            onNextResponse(response);
        }

        private void commit() {
            tx().commitAndGetLogs().ifPresent(commitLog ->
                    commitLog.attributes().forEach((attributeIndex, conceptIds) ->
                            conceptIds.forEach(id -> attributeDeduplicatorDaemon.markForDeduplication(commitLog.keyspace(), attributeIndex, id))
                    ));
            onNextResponse(ResponseBuilder.Transaction.commit());
        }

        private void query(SessionProto.Transaction.Query.Req request) {
            GraqlQuery query = Graql.parse(request.getQuery());

            Stream<Transaction.Res> responseStream = tx().stream(query, request.getInfer().equals(Transaction.Query.INFER.TRUE)).map(ResponseBuilder.Transaction.Iter::query);
            Transaction.Res response = ResponseBuilder.Transaction.queryIterator(iterators.add(responseStream.iterator()));
            onNextResponse(response);
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

        private void getAttributes(Transaction.GetAttributes.Req request) {
            Object value = request.getValue().getAllFields().values().iterator().next();
            Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

            Iterator<Transaction.Res> iterator = attributes.stream().map(ResponseBuilder.Transaction.Iter::getAttributes).iterator();
            int iteratorId = iterators.add(iterator);

            Transaction.Res response = ResponseBuilder.Transaction.getAttributesIterator(iteratorId);
            onNextResponse(response);
        }

        private void putEntityType(Transaction.PutEntityType.Req request) {
            EntityType entityType = tx().putEntityType(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.putEntityType(entityType);
            onNextResponse(response);
        }

        private void putAttributeType(Transaction.PutAttributeType.Req request) {
            Label label = Label.of(request.getLabel());
            AttributeType.DataType<?> dataType = ResponseBuilder.Concept.DATA_TYPE(request.getDataType());

            AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
            Transaction.Res response = ResponseBuilder.Transaction.putAttributeType(attributeType);
            onNextResponse(response);
        }

        private void putRelationshipType(Transaction.PutRelationType.Req request) {
            RelationType relationshipType = tx().putRelationshipType(Label.of(request.getLabel()));
            Transaction.Res response = ResponseBuilder.Transaction.putRelationshipType(relationshipType);
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

        private TransactionOLTP tx() {
            return nonNull(tx);
        }

        private void conceptMethod(Transaction.ConceptMethod.Req request) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(request.getId())));
            Transaction.Res response = ConceptMethod.run(concept, request.getMethod(), iterators, tx());
            onNextResponse(response);
        }

        private void next(Transaction.Iter.Req iterate) {
            int iteratorId = iterate.getId();
            Transaction.Res response = iterators.next(iteratorId);
            if (response == null) throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            onNextResponse(response);
        }

        private void onNextResponse(Transaction.Res response) {
//            if (ServerTracingInstrumentation.tracingActive()) {
//                ServerTracingInstrumentation.currentSpan().finish();
//            }
            responseSender.onNext(response);
        }
    }

    /**
     * Contains a mutable map of iterators of {@link Transaction.Res}s for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     */
    public static class Iterators {
        private final AtomicInteger iteratorIdCounter = new AtomicInteger(1);
        private final Map<Integer, Iterator<Transaction.Res>> iterators = new ConcurrentHashMap<>();

        public static Iterators create() {
            return new Iterators();
        }

        public int add(Iterator<Transaction.Res> iterator) {
            int iteratorId = iteratorIdCounter.getAndIncrement();
            iterators.put(iteratorId, iterator);
            return iteratorId;
        }

        public Transaction.Res next(int iteratorId) {
            Iterator<Transaction.Res> iterator = iterators.get(iteratorId);
            if (iterator == null) return null;

            Transaction.Res response;
            if (iterator.hasNext()) {
                response = iterator.next();
            } else {
                response = SessionProto.Transaction.Res.newBuilder()
                        .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                                .setDone(true)).build();
                stop(iteratorId);
            }

            return response;
        }

        public void stop(int iteratorId) {
            iterators.remove(iteratorId);
        }
    }
}

/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.engine.ServerRPC;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Streamable;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.SessionGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.TxRequest;
import ai.grakn.rpc.proto.SessionProto.TxResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 *  Grakn RPC Session Service
 */
public class SessionService extends SessionGrpc.SessionImplBase {
    private final OpenRequest requestOpener;
    private PostProcessor postProcessor;

    public SessionService(OpenRequest requestOpener, PostProcessor postProcessor) {
        this.requestOpener = requestOpener;
        this.postProcessor = postProcessor;
    }

    public StreamObserver<TxRequest> transaction(StreamObserver<TxResponse> responseSender) {
        return TransactionListener.create(responseSender, requestOpener, postProcessor);
    }


    /**
     * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link ServerRPC}.
     * Receives a stream of {@link TxRequest}s and returning a stream of {@link TxResponse}s.
     */
    static class TransactionListener implements StreamObserver<TxRequest> {
        final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);
        private final StreamObserver<TxResponse> responseSender;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final ExecutorService threadExecutor;
        private final OpenRequest requestOpener;
        private final PostProcessor postProcessor;
        private final Iterators iterators = Iterators.create();

        @Nullable
        private EmbeddedGraknTx<?> tx = null;

        private TransactionListener(StreamObserver<TxResponse> responseSender, ExecutorService threadExecutor, OpenRequest requestOpener, PostProcessor postProcessor) {
            this.responseSender = responseSender;
            this.threadExecutor = threadExecutor;
            this.requestOpener = requestOpener;
            this.postProcessor = postProcessor;
        }

        public static TransactionListener create(StreamObserver<TxResponse> responseSender, OpenRequest requestOpener, PostProcessor postProcessor) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("transaction-listener-%s").build();
            ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
            return new TransactionListener(responseSender, threadExecutor, requestOpener, postProcessor);
        }

        private static <T> T nonNull(@Nullable T item) {
            if (item == null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            } else {
                return item;
            }
        }

        @Override
        public void onNext(TxRequest request) {
            try {
                submit(() -> handleRequest(request));
            } catch (RuntimeException e) {
                close(ResponseBuilder.exception(e));
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

        private void handleRequest(TxRequest request) {
            switch (request.getRequestCase()) {
                case OPEN:
                    open(request.getOpen());
                    break;
                case COMMIT:
                    commit();
                    break;
                case QUERY:
                    query(request.getQuery());
                    break;
                case NEXT:
                    next(request.getNext());
                    break;
                case STOP:
                    stop(request.getStop());
                    break;
                case RUNCONCEPTMETHOD:
                    runConceptMethod(request.getRunConceptMethod());
                    break;
                case GETCONCEPT:
                    getConcept(request.getGetConcept());
                    break;
                case GETSCHEMACONCEPT:
                    getSchemaConcept(request.getGetSchemaConcept());
                    break;
                case GETATTRIBUTESBYVALUE:
                    getAttributesByValue(request.getGetAttributesByValue());
                    break;
                case PUTENTITYTYPE:
                    putEntityType(request.getPutEntityType());
                    break;
                case PUTRELATIONSHIPTYPE:
                    putRelationshipType(request.getPutRelationshipType());
                    break;
                case PUTATTRIBUTETYPE:
                    putAttributeType(request.getPutAttributeType());
                    break;
                case PUTROLE:
                    putRole(request.getPutRole());
                    break;
                case PUTRULE:
                    putRule(request.getPutRule());
                    break;
                default:
                case REQUEST_NOT_SET:
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
                    responseSender.onError(error);
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

        private void open(SessionProto.Open.Req request) {
            if (tx != null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            }

            ServerOpenRequest.Arguments args = new ServerOpenRequest.Arguments(
                    Keyspace.of(request.getKeyspace()),
                    GraknTxType.of(request.getTxType().getNumber())
            );

            tx = requestOpener.open(args);
            responseSender.onNext(ResponseBuilder.Transaction.open());
        }

        private void commit() {
            tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
            responseSender.onNext(ResponseBuilder.Transaction.commit());
        }

        private void query(SessionProto.Query request) {
            String queryString = request.getQuery();
            QueryBuilder graql = tx().graql();
            TxResponse response;
            graql = graql.infer(request.getInfer());

            Query<?> query = graql.parse(queryString);

            if (query instanceof Streamable) {
                Stream<TxResponse> responseStream = ((Streamable<?>) query).stream().map(ResponseBuilder.Transaction::answer);
                IteratorProto.IteratorId iteratorId = iterators.add(responseStream.iterator());

                response = TxResponse.newBuilder().setIteratorId(iteratorId).build();
            } else {
                Object result = query.execute();

                if (result == null) response = ResponseBuilder.Transaction.done();
                else response = ResponseBuilder.Transaction.answer(result);
            }

            responseSender.onNext(response);
        }

        private void next(IteratorProto.Next next) {
            IteratorProto.IteratorId iteratorId = next.getIteratorId();
            TxResponse response = iterators.next(iteratorId);
            if (response == null) throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            responseSender.onNext(response);
        }

        private void stop(IteratorProto.Stop stop) {
            IteratorProto.IteratorId iteratorId = stop.getIteratorId();
            iterators.stop(iteratorId);
            responseSender.onNext(ResponseBuilder.Transaction.done());
        }

        private void runConceptMethod(SessionProto.RunConceptMethod runConceptMethod) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(runConceptMethod.getId())));
            TxResponse response = ConceptMethod.run(concept, runConceptMethod.getMethod(), iterators, tx());
            responseSender.onNext(response);
        }

        private void getConcept(String conceptId) {
            Concept concept = tx().getConcept(ConceptId.of(conceptId));
            if (concept == null) {
                responseSender.onNext(ResponseBuilder.Transaction.noResult());
            } else {
                responseSender.onNext(ResponseBuilder.Transaction.concept(concept));
            }
        }

        private void getSchemaConcept(String label) {
            Concept concept = tx().getSchemaConcept(Label.of(label));
            if (concept == null) {
                responseSender.onNext(ResponseBuilder.Transaction.noResult());
            } else {
                responseSender.onNext(ResponseBuilder.Transaction.concept(concept));
            }
        }

        private void getAttributesByValue(ConceptProto.AttributeValue attributeValue) {
            Object value = attributeValue.getAllFields().values().iterator().next();
            Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

            Iterator<TxResponse> iterator = attributes.stream().map(ResponseBuilder.Transaction::concept).iterator();
            IteratorProto.IteratorId iteratorId = iterators.add(iterator);

            responseSender.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
        }

        private void putEntityType(String label) {
            EntityType entityType = tx().putEntityType(Label.of(label));
            responseSender.onNext(ResponseBuilder.Transaction.concept(entityType));
        }

        private void putRelationshipType(String label) {
            RelationshipType relationshipType = tx().putRelationshipType(Label.of(label));
            responseSender.onNext(ResponseBuilder.Transaction.concept(relationshipType));
        }

        private void putAttributeType(SessionProto.AttributeType putAttributeType) {
            Label label = Label.of(putAttributeType.getLabel());
            AttributeType.DataType<?> dataType = dataType(putAttributeType.getDataType());

            AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
            responseSender.onNext(ResponseBuilder.Transaction.concept(attributeType));
        }

        private void putRole(String label) {
            Role role = tx().putRole(Label.of(label));
            responseSender.onNext(ResponseBuilder.Transaction.concept(role));
        }

        private void putRule(SessionProto.Rule putRule) {
            Label label = Label.of(putRule.getLabel());
            Pattern when = Graql.parser().parsePattern(putRule.getWhen());
            Pattern then = Graql.parser().parsePattern(putRule.getThen());

            Rule rule = tx().putRule(label, when, then);
            responseSender.onNext(ResponseBuilder.Transaction.concept(rule));
        }

        private EmbeddedGraknTx<?> tx() {
            return nonNull(tx);
        }

        public static AttributeType.DataType<?> dataType(ConceptProto.DataType dataType) {
            switch (dataType) {
                case String:
                    return AttributeType.DataType.STRING;
                case Boolean:
                    return AttributeType.DataType.BOOLEAN;
                case Integer:
                    return AttributeType.DataType.INTEGER;
                case Long:
                    return AttributeType.DataType.LONG;
                case Float:
                    return AttributeType.DataType.FLOAT;
                case Double:
                    return AttributeType.DataType.DOUBLE;
                case Date:
                    return AttributeType.DataType.DATE;
                default:
                case UNRECOGNIZED:
                    throw new IllegalArgumentException("Unrecognised " + dataType);
            }
        }
    }

    /**
     * Contains a mutable map of iterators of {@link TxResponse}s for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     */
    public static class Iterators {
        private final AtomicInteger iteratorIdCounter = new AtomicInteger();
        private final Map<IteratorProto.IteratorId, Iterator<TxResponse>> iterators = new ConcurrentHashMap<>();

        public static Iterators create() {
            return new Iterators();
        }

        public IteratorProto.IteratorId add(Iterator<TxResponse> iterator) {
            IteratorProto.IteratorId iteratorId = IteratorProto.IteratorId.newBuilder().setId(iteratorIdCounter.getAndIncrement()).build();
            iterators.put(iteratorId, iterator);
            return iteratorId;
        }

        public TxResponse next(IteratorProto.IteratorId iteratorId) {
            Iterator<TxResponse> iterator = iterators.get(iteratorId);
            if (iterator == null) return null;

            TxResponse response;
            if (iterator.hasNext()) {
                response = iterator.next();
            } else {
                response = ResponseBuilder.Transaction.done();
                stop(iteratorId);
            }

            return response;
        }

        public void stop(IteratorProto.IteratorId iteratorId) {
            iterators.remove(iteratorId);
        }
    }
}

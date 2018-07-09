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
import ai.grakn.graql.Streamable;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.SessionGrpc;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.rpc.proto.SessionProto.Transaction;
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

    public StreamObserver<Transaction.Req> transaction(StreamObserver<Transaction.Res> responseSender) {
        return TransactionListener.create(responseSender, requestOpener, postProcessor);
    }


    /**
     * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link ServerRPC}.
     * Receives a stream of {@link Transaction.Req}s and returning a stream of {@link Transaction.Res}s.
     */
    static class TransactionListener implements StreamObserver<Transaction.Req> {
        final Logger LOG = LoggerFactory.getLogger(TransactionListener.class);
        private final StreamObserver<Transaction.Res> responseSender;
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final ExecutorService threadExecutor;
        private final OpenRequest requestOpener;
        private final PostProcessor postProcessor;
        private final Iterators iterators = Iterators.create();

        @Nullable
        private EmbeddedGraknTx<?> tx = null;

        private TransactionListener(StreamObserver<Transaction.Res> responseSender, ExecutorService threadExecutor, OpenRequest requestOpener, PostProcessor postProcessor) {
            this.responseSender = responseSender;
            this.threadExecutor = threadExecutor;
            this.requestOpener = requestOpener;
            this.postProcessor = postProcessor;
        }

        public static TransactionListener create(StreamObserver<Transaction.Res> responseSender, OpenRequest requestOpener, PostProcessor postProcessor) {
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
        public void onNext(Transaction.Req request) {
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

        private void handleRequest(Transaction.Req request) {
            switch (request.getReqCase()) {
                case OPEN:
                    open(request.getOpen());
                    break;
                case COMMIT:
                    commit();
                    break;
                case QUERY:
                    query(request.getQuery());
                    break;
                case ITERATE:
                    next(request.getIterate());
                    break;
                case GETSCHEMACONCEPT:
                    getSchemaConcept(request.getGetSchemaConcept());
                    break;
                case GETCONCEPT:
                    getConcept(request.getGetConcept());
                    break;
                case GETATTRIBUTES:
                    getAttributes(request.getGetAttributes());
                    break;
                case PUTENTITYTYPE:
                    putEntityType(request.getPutEntityType());
                    break;
                case PUTATTRIBUTETYPE:
                    putAttributeType(request.getPutAttributeType());
                    break;
                case PUTRELATIONSHIPTYPE:
                    putRelationshipType(request.getPutRelationshipType());
                    break;
                case PUTROLE:
                    putRole(request.getPutRole());
                    break;
                case PUTRULE:
                    putRule(request.getPutRule());
                    break;
                case CONCEPTMETHOD:
                    conceptMethod(request.getConceptMethod());
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

        private void open(SessionProto.Transaction.Open.Req request) {
            if (tx != null) {
                throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
            }

            ServerOpenRequest.Arguments args = new ServerOpenRequest.Arguments(
                    Keyspace.of(request.getKeyspace()),
                    GraknTxType.of(request.getTxType())
            );

            tx = requestOpener.open(args);
            responseSender.onNext(ResponseBuilder.Transaction.open());
        }

        private void commit() {
            tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
            responseSender.onNext(ResponseBuilder.Transaction.commit());
        }

        private void query(SessionProto.Transaction.Query.Req request) {
            Query<?> query = tx().graql().infer(request.getInfer()).parse(request.getQuery());

            Stream<Transaction.Res> responseStream;
            int iteratorId;
            Transaction.Res response;
            if (query instanceof Streamable) {
                responseStream = ((Streamable<?>) query).stream().map(ResponseBuilder.Transaction.Iter::query);
                iteratorId = iterators.add(responseStream.iterator());
            } else {
                Object result = query.execute();
                if (result == null) {
                    iteratorId = -1;
                } else {
                    responseStream = Stream.of(ResponseBuilder.Transaction.Iter.query(result));
                    iteratorId = iterators.add(responseStream.iterator());
                }
            }

            response = ResponseBuilder.Transaction.queryIterator(iteratorId);
            responseSender.onNext(response);
        }

        private void getSchemaConcept(SessionProto.Transaction.GetSchemaConcept.Req request) {
            Concept concept = tx().getSchemaConcept(Label.of(request.getLabel()));
            responseSender.onNext(ResponseBuilder.Transaction.getSchemaConcept(concept));
        }

        private void getConcept(SessionProto.Transaction.GetConcept.Req request) {
            Concept concept = tx().getConcept(ConceptId.of(request.getId()));
            responseSender.onNext(ResponseBuilder.Transaction.getConcept(concept));
        }

        private void getAttributes(SessionProto.Transaction.GetAttributes.Req request) {
            Object value = request.getValue().getAllFields().values().iterator().next();
            Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

            Iterator<Transaction.Res> iterator = attributes.stream().map(ResponseBuilder.Transaction.Iter::getAttributes).iterator();
            int iteratorId = iterators.add(iterator);

            responseSender.onNext(ResponseBuilder.Transaction.getAttributesIterator(iteratorId));
        }

        private void putEntityType(SessionProto.Transaction.PutEntityType.Req request) {
            EntityType entityType = tx().putEntityType(Label.of(request.getLabel()));
            responseSender.onNext(ResponseBuilder.Transaction.putEntityType(entityType));
        }

        private void putAttributeType(SessionProto.Transaction.PutAttributeType.Req request) {
            Label label = Label.of(request.getLabel());
            AttributeType.DataType<?> dataType = ResponseBuilder.Concept.DATA_TYPE(request.getDataType());

            AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
            responseSender.onNext(ResponseBuilder.Transaction.putAttributeType(attributeType));
        }

        private void putRelationshipType(SessionProto.Transaction.PutRelationshipType.Req request) {
            RelationshipType relationshipType = tx().putRelationshipType(Label.of(request.getLabel()));
            responseSender.onNext(ResponseBuilder.Transaction.putRelationshipType(relationshipType));
        }

        private void putRole(SessionProto.Transaction.PutRole.Req request) {
            Role role = tx().putRole(Label.of(request.getLabel()));
            responseSender.onNext(ResponseBuilder.Transaction.putRole(role));
        }

        private void putRule(SessionProto.Transaction.PutRule.Req request) {
            Label label = Label.of(request.getLabel());
            Pattern when = Graql.parser().parsePattern(request.getWhen());
            Pattern then = Graql.parser().parsePattern(request.getThen());

            Rule rule = tx().putRule(label, when, then);
            responseSender.onNext(ResponseBuilder.Transaction.putRule(rule));
        }

        private EmbeddedGraknTx<?> tx() {
            return nonNull(tx);
        }

        private void conceptMethod(SessionProto.Transaction.ConceptMethod.Req request) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(request.getId())));
            Transaction.Res response = ConceptMethod.run(concept, request.getMethod(), iterators, tx());
            responseSender.onNext(response);
        }

        private void next(SessionProto.Transaction.Iter.Req iterate) {
            int iteratorId = iterate.getId();
            Transaction.Res response = iterators.next(iteratorId);
            if (response == null) throw ResponseBuilder.exception(Status.FAILED_PRECONDITION);
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
                        .setIterate(SessionProto.Transaction.Iter.Res.newBuilder()
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

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

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.util.EmbeddedConceptReader;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Streamable;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ResponseBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 *  Grakn RPC TransactionService
 */
public class TransactionService extends GraknGrpc.GraknImplBase {
    private final OpenRequest requestOpener;
    private PostProcessor postProcessor;

    public TransactionService(OpenRequest requestOpener, PostProcessor postProcessor) {
        this.requestOpener = requestOpener;
        this.postProcessor = postProcessor;
    }

    @Override
    public StreamObserver<TxRequest> tx(StreamObserver<TxResponse> responseSender) {
        return TransactionListener.create(responseSender, requestOpener, postProcessor);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseSender) {
        try {
            runAndConvertGraknExceptions(() -> {
                try (GraknTx tx = requestOpener.open(request.getOpen())) {
                    tx.admin().delete();
                }

                responseSender.onNext(ResponseBuilder.delete());
                responseSender.onCompleted();
            });
        } catch (StatusRuntimeException e) {
            responseSender.onError(e);
        }
    }

    static void runAndConvertGraknExceptions(Runnable runnable) {
        try {
            runnable.run();
        } catch (TemporaryWriteException e) {
            throw error(Status.RESOURCE_EXHAUSTED, e);
        } catch (GraknServerException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.GRAKN_SERVER_EXCEPTION);
        } catch (GraknBackendException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.GRAKN_BACKEND_EXCEPTION);
        } catch (PropertyNotUniqueException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.PROPERTY_NOT_UNIQUE_EXCEPTION);
        } catch (GraknTxOperationException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.GRAKN_TX_OPERATION_EXCEPTION);
        } catch (GraqlQueryException e) {
            throw error(Status.INVALID_ARGUMENT, e);
        } catch (GraqlSyntaxException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.GRAQL_SYNTAX_EXCEPTION);
        } catch (InvalidKBException e) {
            throw convertGraknException(e, ResponseBuilder.ErrorType.INVALID_KB_EXCEPTION);
        } catch (GraknException e) {
            // We shouldn't normally encounter this case unless someone adds a new exception class
            throw convertGraknException(e, ResponseBuilder.ErrorType.UNKNOWN);
        }
    }

    private static StatusRuntimeException convertGraknException(GraknException exception, ResponseBuilder.ErrorType errorType) {
        Metadata trailers = new Metadata();
        trailers.put(ResponseBuilder.ErrorType.KEY, errorType);
        return error(Status.UNKNOWN.withDescription(exception.getMessage()), trailers);
    }

    private static StatusRuntimeException error(Status status, GraknException e) {
        return new StatusRuntimeException(
                status.withDescription(e.getName() + " - " + e.getMessage() + "\nPlease check server logs for the stack trace."));
    }

    private static StatusRuntimeException error(Status status) {
        return new StatusRuntimeException(status);
    }

    private static StatusRuntimeException error(Status status, @Nullable Metadata trailers) {
        return new StatusRuntimeException(status, trailers);
    }


    /**
     * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link Server}.
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

        public static TransactionListener create(StreamObserver<TxResponse> responseObserver, OpenRequest requestExecutor, PostProcessor postProcessor) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
            ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
            return new TransactionListener(responseObserver, threadExecutor, requestExecutor, postProcessor);
        }

        private static <T> T nonNull(@Nullable T item) {
            if (item == null) {
                throw error(Status.FAILED_PRECONDITION);
            } else {
                return item;
            }
        }

        @Override
        public void onNext(TxRequest request) {
            try {
                submit(() -> {
                    runAndConvertGraknExceptions(() -> handleRequest(request));
                });
            } catch (StatusRuntimeException e) {
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
                    throw error(Status.INVALID_ARGUMENT);
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

        private void open(GrpcGrakn.Open request) {
            if (tx != null) {
                throw error(Status.FAILED_PRECONDITION);
            }
            tx = requestOpener.open(request);
            responseSender.onNext(ResponseBuilder.done());
        }

        private void commit() {
            tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
            responseSender.onNext(ResponseBuilder.done());
        }

        private void query(GrpcGrakn.Query request) {
            String queryString = request.getQuery();
            QueryBuilder graql = tx().graql();
            TxResponse response;
            graql = graql.infer(request.getInfer());

            Query<?> query = graql.parse(queryString);

            if (query instanceof Streamable) {
                Stream<TxResponse> responseStream = ((Streamable<?>) query).stream().map(ResponseBuilder::answer);
                GrpcIterator.IteratorId iteratorId = iterators.add(responseStream.iterator());

                response = TxResponse.newBuilder().setIteratorId(iteratorId).build();
            } else {
                Object result = query.execute();

                if (result == null) response = ResponseBuilder.done();
                else response = ResponseBuilder.answer(result);
            }

            responseSender.onNext(response);
        }

        private void next(GrpcIterator.Next next) {
            GrpcIterator.IteratorId iteratorId = next.getIteratorId();

            TxResponse response =
                    iterators.next(iteratorId).orElseThrow(() -> error(Status.FAILED_PRECONDITION));

            responseSender.onNext(response);
        }

        private void stop(GrpcIterator.Stop stop) {
            GrpcIterator.IteratorId iteratorId = stop.getIteratorId();
            iterators.stop(iteratorId);
            responseSender.onNext(ResponseBuilder.done());
        }

        private void runConceptMethod(GrpcGrakn.RunConceptMethod runConceptMethod) {
            Concept concept = nonNull(tx().getConcept(ConceptId.of(runConceptMethod.getId())));
            EmbeddedConceptReader txConceptReader = new EmbeddedConceptReader(tx());

            TxResponse response = ConceptMethod.run(concept, runConceptMethod.getMethod(), iterators, txConceptReader);
            responseSender.onNext(response);
        }

        private void getConcept(String conceptId) {
            Concept concept = tx().getConcept(ConceptId.of(conceptId));
            if (concept == null) {
                responseSender.onNext(ResponseBuilder.noResult());
            } else {
                responseSender.onNext(ResponseBuilder.concept(concept));
            }
        }

        private void getSchemaConcept(String label) {
            Concept concept = tx().getSchemaConcept(Label.of(label));
            if (concept == null) {
                responseSender.onNext(ResponseBuilder.noResult());
            } else {
                responseSender.onNext(ResponseBuilder.concept(concept));
            }
        }

        private void getAttributesByValue(GrpcConcept.AttributeValue attributeValue) {
            Object value = attributeValue.getAllFields().values().iterator().next();
            Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

            Iterator<TxResponse> iterator = attributes.stream().map(ResponseBuilder::concept).iterator();
            GrpcIterator.IteratorId iteratorId = iterators.add(iterator);

            responseSender.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
        }

        private void putEntityType(String label) {
            EntityType entityType = tx().putEntityType(Label.of(label));
            responseSender.onNext(ResponseBuilder.concept(entityType));
        }

        private void putRelationshipType(String label) {
            RelationshipType relationshipType = tx().putRelationshipType(Label.of(label));
            responseSender.onNext(ResponseBuilder.concept(relationshipType));
        }

        private void putAttributeType(GrpcGrakn.AttributeType putAttributeType) {
            Label label = Label.of(putAttributeType.getLabel());
            AttributeType.DataType<?> dataType = dataType(putAttributeType.getDataType());

            AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
            responseSender.onNext(ResponseBuilder.concept(attributeType));
        }

        private void putRole(String label) {
            Role role = tx().putRole(Label.of(label));
            responseSender.onNext(ResponseBuilder.concept(role));
        }

        private void putRule(GrpcGrakn.Rule putRule) {
            Label label = Label.of(putRule.getLabel());
            Pattern when = Graql.parser().parsePattern(putRule.getWhen());
            Pattern then = Graql.parser().parsePattern(putRule.getThen());

            Rule rule = tx().putRule(label, when, then);
            responseSender.onNext(ResponseBuilder.concept(rule));
        }

        private EmbeddedGraknTx<?> tx() {
            return nonNull(tx);
        }

        public static AttributeType.DataType<?> dataType(GrpcConcept.DataType dataType) {
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
        private final Map<GrpcIterator.IteratorId, Iterator<TxResponse>> iterators = new ConcurrentHashMap<>();

        public static Iterators create() {
            return new Iterators();
        }

        public GrpcIterator.IteratorId add(Iterator<TxResponse> iterator) {
            GrpcIterator.IteratorId iteratorId = GrpcIterator.IteratorId.newBuilder().setId(iteratorIdCounter.getAndIncrement()).build();
            iterators.put(iteratorId, iterator);
            return iteratorId;
        }

        public Optional<TxResponse> next(GrpcIterator.IteratorId iteratorId) {
            return Optional.ofNullable(iterators.get(iteratorId)).map(iterator -> {
                TxResponse response;
                if (iterator.hasNext()) {
                    response = iterator.next();
                } else {
                    response = ResponseBuilder.done();
                    stop(iteratorId);
                }
                return response;
            });
        }

        public void stop(GrpcIterator.IteratorId iteratorId) {
            iterators.remove(iteratorId);
        }
    }
}

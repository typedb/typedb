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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.GrpcUtil.ErrorType;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.GetConceptProperty;
import ai.grakn.rpc.generated.GraknOuterClass.IteratorId;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link GrpcServer}.
 * <p>
 * <p>
 * Receives a stream of {@link TxRequest}s and returning a stream of {@link TxResponse}s.
 * </p>
 *
 * @author Felix Chapman
 */
class TxObserver implements StreamObserver<TxRequest>, AutoCloseable {

    private final StreamObserver<TxResponse> responseObserver;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final ExecutorService threadExecutor;
    private final GrpcOpenRequestExecutor requestExecutor;
    private final PostProcessor postProcessor;
    private final AtomicInteger iteratorIdCounter = new AtomicInteger();
    private final Map<IteratorId, Iterator<QueryResult>> iterators = new ConcurrentHashMap<>();

    private @Nullable
    EmbeddedGraknTx<?> tx = null;

    private TxObserver(StreamObserver<TxResponse> responseObserver, ExecutorService threadExecutor, GrpcOpenRequestExecutor requestExecutor, PostProcessor postProcessor) {
        this.responseObserver = responseObserver;
        this.threadExecutor = threadExecutor;
        this.requestExecutor = requestExecutor;
        this.postProcessor = postProcessor;
    }

    public static TxObserver create(StreamObserver<TxResponse> responseObserver, GrpcOpenRequestExecutor requestExecutor, PostProcessor postProcessor) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
        ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
        return new TxObserver(responseObserver, threadExecutor, requestExecutor, postProcessor);
    }

    @Override
    public void onNext(TxRequest request) {
        submit(() -> {
            try {
                switch (request.getRequestCase()) {
                    case OPEN:
                        open(request.getOpen());
                        break;
                    case COMMIT:
                        commit();
                        break;
                    case EXECQUERY:
                        execQuery(request.getExecQuery());
                        break;
                    case NEXT:
                        next(request.getNext());
                        break;
                    case STOP:
                        stop(request.getStop());
                        break;
                    case GETCONCEPTPROPERTY:
                        getConceptProperty(request.getGetConceptProperty());
                        break;
                    default:
                    case REQUEST_NOT_SET:
                        throw error(Status.INVALID_ARGUMENT);
                }
            } catch (TemporaryWriteException e) {
                throw convertGraknException(e, ErrorType.TEMPORARY_WRITE_EXCEPTION);
            } catch (GraknServerException e) {
                throw convertGraknException(e, ErrorType.GRAKN_SERVER_EXCEPTION);
            } catch (GraknBackendException e) {
                throw convertGraknException(e, ErrorType.GRAKN_BACKEND_EXCEPTION);
            } catch (PropertyNotUniqueException e) {
                throw convertGraknException(e, ErrorType.PROPERTY_NOT_UNIQUE_EXCEPTION);
            } catch (GraknTxOperationException e) {
                throw convertGraknException(e, ErrorType.GRAKN_TX_OPERATION_EXCEPTION);
            } catch (GraqlQueryException e) {
                throw convertGraknException(e, ErrorType.GRAQL_QUERY_EXCEPTION);
            } catch (GraqlSyntaxException e) {
                throw convertGraknException(e, ErrorType.GRAQL_SYNTAX_EXCEPTION);
            } catch (InvalidKBException e) {
                throw convertGraknException(e, ErrorType.INVALID_KB_EXCEPTION);
            } catch (GraknException e) {
                // We shouldn't normally encounter this case unless someone adds a new exception class
                throw convertGraknException(e, ErrorType.UNKNOWN);
            }
        });
    }

    @Override
    public void onError(Throwable t) {
        close();
    }

    @Override
    public void onCompleted() {
        close();
    }

    @Override
    public void close() {
        submit(() -> {
            if (tx != null) {
                tx.close();
            }

            if (!terminated.getAndSet(true)) {
                responseObserver.onCompleted();
            }
        });

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

    private void open(Open request) {
        if (tx != null) {
            throw error(Status.FAILED_PRECONDITION);
        }
        tx = requestExecutor.execute(request);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void commit() {
        tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void execQuery(ExecQuery request) {
        String queryString = request.getQuery().getValue();

        QueryBuilder graql = tx().graql();

        if (request.hasInfer()) {
            graql = graql.infer(request.getInfer().getValue());
        }

        Iterator<QueryResult> iterator = graql.parse(queryString).results(GrpcConverter.get()).iterator();
        IteratorId iteratorId =
                IteratorId.newBuilder().setId(iteratorIdCounter.getAndIncrement()).build();

        iterators.put(iteratorId, iterator);

        responseObserver.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
    }

    private void next(GraknOuterClass.Next next) {
        IteratorId iteratorId = next.getIteratorId();

        Iterator<QueryResult> iterator = nonNull(iterators.get(iteratorId));

        TxResponse response;

        if (iterator.hasNext()) {
            QueryResult queryResult = iterator.next();
            response = TxResponse.newBuilder().setQueryResult(queryResult).build();
        } else {
            response = GrpcUtil.doneResponse();
            iterators.remove(iteratorId);
        }

        responseObserver.onNext(response);
    }

    private void stop(GraknOuterClass.Stop stop) {
        nonNull(iterators.remove(stop.getIteratorId()));
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void getConceptProperty(GetConceptProperty getConceptProperty) {
        Concept concept = nonNull(tx().getConcept(GrpcUtil.getConceptId(getConceptProperty)));

        ConceptProperty<?> conceptProperty = ConceptProperty.fromGrpc(getConceptProperty.getConceptProperty());

        TxResponse response = conceptProperty.createTxResponse(concept);

        responseObserver.onNext(response);
    }

    private EmbeddedGraknTx<?> tx() {
        return nonNull(tx);
    }

    private <T> T nonNull(@Nullable T item) {
        if (item == null) {
            throw error(Status.FAILED_PRECONDITION);
        } else {
            return item;
        }
    }

    private StatusRuntimeException convertGraknException(GraknException exception, ErrorType errorType) {
        Metadata trailers = new Metadata();
        trailers.put(ErrorType.KEY, errorType);
        return error(Status.UNKNOWN.withDescription(exception.getMessage()), trailers);
    }

    private StatusRuntimeException error(Status status) {
        return error(status, null);
    }

    private StatusRuntimeException error(Status status, @Nullable Metadata trailers) {
        StatusRuntimeException exception = new StatusRuntimeException(status, trailers);
        if (!terminated.getAndSet(true)) {
            responseObserver.onError(exception);
        }
        return exception;
    }
}

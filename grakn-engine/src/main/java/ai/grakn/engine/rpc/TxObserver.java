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

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.generated.GraknOuterClass.Infer;
import ai.grakn.rpc.generated.GraknOuterClass.Open;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.grpc.GrpcUtil.doneResponse;

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
    private final EngineGraknTxFactory txFactory;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final ExecutorService executor;

    private @Nullable GraknTx tx = null;
    private @Nullable Iterator<QueryResult> queryResults = null;

    private TxObserver(
            EngineGraknTxFactory txFactory, StreamObserver<TxResponse> responseObserver, ExecutorService executor) {
        this.responseObserver = responseObserver;
        this.txFactory = txFactory;
        this.executor = executor;
    }

    public static TxObserver create(EngineGraknTxFactory txFactory, StreamObserver<TxResponse> responseObserver) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
        ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
        return new TxObserver(txFactory, responseObserver, executor);
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
                        next();
                        break;
                    case STOP:
                        stop();
                        break;
                    default:
                    case REQUEST_NOT_SET:
                        throw error(Status.INVALID_ARGUMENT);
                }
            } catch (GraknException e) {
                throw error(Status.UNKNOWN.withDescription(e.getMessage()));
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

        executor.shutdown();
    }

    private void submit(Runnable runnable) {
        try {
            executor.submit(runnable).get();
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

        Keyspace keyspace = GrpcUtil.convert(request.getKeyspace());
        GraknTxType txType = GrpcUtil.convert(request.getTxType());
        tx = txFactory.tx(keyspace, txType);

        responseObserver.onNext(doneResponse());
    }

    private void commit() {
        if (tx == null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        tx.commit();

        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void execQuery(ExecQuery request) {
        if (tx == null || queryResults != null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        String queryString = request.getQuery().getValue();

        QueryBuilder graql = setInfer(tx.graql(), request.getInfer());

        queryResults = graql.parse(queryString).results(GrpcConverter.get()).iterator();

        sendNextResult();
    }

    private void next() {
        if (queryResults == null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        sendNextResult();
    }

    private void stop() {
        if (queryResults == null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        queryResults = null;

        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private QueryBuilder setInfer(QueryBuilder queryBuilder, Infer infer) {
        if (infer.getIsSet()) {
            return queryBuilder.infer(infer.getValue());
        } else {
            return queryBuilder;
        }
    }

    private void sendNextResult() {
        assert queryResults != null : "Method is only called when queryResults is non-null";

        TxResponse response;

        if (queryResults.hasNext()) {
            QueryResult queryResult = queryResults.next();
            response = TxResponse.newBuilder().setQueryResult(queryResult).build();
        } else {
            response = GrpcUtil.doneResponse();
            queryResults = null;
        }

        responseObserver.onNext(response);
    }

    private StatusRuntimeException error(Status status) {
        StatusRuntimeException exception = new StatusRuntimeException(status);
        if (!terminated.getAndSet(true)) {
            responseObserver.onError(exception);
        }
        return exception;
    }

}

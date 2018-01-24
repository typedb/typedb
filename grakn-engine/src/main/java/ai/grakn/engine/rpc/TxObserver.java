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
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.Done;
import ai.grakn.rpc.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.GraknOuterClass.Infer;
import ai.grakn.rpc.GraknOuterClass.Next;
import ai.grakn.rpc.GraknOuterClass.Open;
import ai.grakn.rpc.GraknOuterClass.QueryResult;
import ai.grakn.rpc.GraknOuterClass.Stop;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link GrpcServer}.
 *
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

    private @Nullable GraknTx tx = null;
    private @Nullable Iterator<QueryResult> queryResults = null;

    private TxObserver(EngineGraknTxFactory txFactory, StreamObserver<TxResponse> responseObserver) {
        this.responseObserver = responseObserver;
        this.txFactory = txFactory;
    }

    public static TxObserver create(EngineGraknTxFactory txFactory, StreamObserver<TxResponse> responseObserver) {
        return new TxObserver(txFactory, responseObserver);
    }

    @Override
    public void onNext(TxRequest request) {
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
                default:
                case REQUEST_NOT_SET:
                    throw error(Status.INVALID_ARGUMENT);
            }
        } catch (GraknException e) {
            Metadata trailers = new Metadata();
            trailers.put(GrpcServer.MESSAGE, e.getMessage());
            throw error(Status.UNKNOWN, trailers);
        }
    }

    private void open(Open request) {
        if (tx != null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        String keyspaceString = request.getKeyspace().getValue();
        Keyspace keyspace = Keyspace.of(keyspaceString);
        GraknTxType txType = getTxType(request.getTxType());
        tx = txFactory.tx(keyspace, txType);
    }

    private void commit() {
        if (tx == null) {
            throw error(Status.FAILED_PRECONDITION);
        }
        tx.commit();
    }

    private void execQuery(ExecQuery request) {
        if (tx == null || queryResults != null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        String queryString = request.getQuery().getValue();

        QueryBuilder graql = setInfer(tx.graql(), request.getInfer());

        queryResults = graql.parse(queryString).results(GrpcConverter.get()).iterator();
    }

    private void next(Next next) {
        if (queryResults == null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        TxResponse response;

        if (queryResults.hasNext()) {
            QueryResult queryResult = queryResults.next();
            response = TxResponse.newBuilder().setQueryResult(queryResult).build();
        } else {
            response = TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
        }

        responseObserver.onNext(response);
    }

    private void stop(Stop stop) {
        if (queryResults == null) {
            throw error(Status.FAILED_PRECONDITION);
        }

        queryResults = null;
    }

    private QueryBuilder setInfer(QueryBuilder queryBuilder, Infer infer) {
        if (infer.getIsSet()) {
            return queryBuilder.infer(infer.getValue());
        } else {
            return queryBuilder;
        }
    }

    private GraknTxType getTxType(GraknOuterClass.TxType txType) {
        switch (txType) {
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
            default:
            case UNRECOGNIZED:
                throw error(Status.INVALID_ARGUMENT);
        }
    }

    @Override
    public void onError(Throwable t) {
        close();
    }

    @Override
    public void onCompleted() {
        close();
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

    @Override
    public void close() {
        if (tx != null) tx.close();

        if (!terminated.getAndSet(true)) {
            responseObserver.onCompleted();
        }
    }
}

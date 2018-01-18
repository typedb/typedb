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
import ai.grakn.rpc.GraknOuterClass.ExecQuery;
import ai.grakn.rpc.GraknOuterClass.Infer;
import ai.grakn.rpc.GraknOuterClass.Open;
import ai.grakn.rpc.GraknOuterClass.QueryComplete;
import ai.grakn.rpc.GraknOuterClass.QueryResult;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

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
    private @Nullable Boolean infer = null;

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
                case INFER:
                    infer(request.getInfer());
                    break;
                case REQUEST_NOT_SET:
                    break;
            }
        } catch (GraknException e) {
            Metadata trailers = new Metadata();
            trailers.put(GrpcServer.MESSAGE, e.getMessage());
            error(Status.UNKNOWN, trailers);
        }
    }

    private void open(Open request) {
        if (tx != null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }

        String keyspaceString = request.getKeyspace().getValue();
        Keyspace keyspace = Keyspace.of(keyspaceString);
        GraknTxType txType = getTxType(request.getTxType());
        tx = txFactory.tx(keyspace, txType);
    }

    private void commit() {
        if (tx == null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }
        tx.commit();
    }

    private void execQuery(ExecQuery request) {
        if (tx == null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }

        String queryString = request.getQuery().getValue();

        QueryBuilder graql = tx.graql();

        if (infer != null) {
            graql.infer(infer);
            // We reset `infer` so the next query will use the default inference setting again, so this works:
            // ```
            // tx.execute("match ...", infer=False)  # should run with inference off
            // tx.execute("match ...")               # should run with inference set to server default
            // ```
            infer = null;
        }

        Stream<QueryResult> results = graql.parse(queryString).results(GrpcConverter.get());

        results.forEach(result -> {
            responseObserver.onNext(TxResponse.newBuilder().setQueryResult(result).build());
        });

        responseObserver.onNext(TxResponse.newBuilder().setQueryComplete(QueryComplete.getDefaultInstance()).build());
    }

    private void infer(Infer request) {
        this.infer = request.getValue();
    }

    private GraknTxType getTxType(GraknOuterClass.TxType txType) {
        switch (txType) {
            default:
            case UNRECOGNIZED:  // Unrecognised indicates a newer client and is treated as "read"
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
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

    private void error(Status status) {
        error(status, null);
    }

    private void error(Status status, @Nullable Metadata trailers) {
        if (!terminated.getAndSet(true)) {
            responseObserver.onError(new StatusRuntimeException(status, trailers));
        }
    }

    @Override
    public void close() {
        if (tx != null) tx.close();

        if (!terminated.getAndSet(true)) {
            responseObserver.onCompleted();
        }
    }
}

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
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknException;
import ai.grakn.graql.Printer;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import ai.grakn.rpc.GraknOuterClass.TxResponse.QueryResult;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
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
class TxObserver implements StreamObserver<TxRequest> {
    private final StreamObserver<TxResponse> responseObserver;
    private final EngineGraknTxFactory txFactory;
    private @Nullable TxThread tx = null;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    private static final Printer<?> PRINTER = Printers.json();

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
                    open(request);
                    break;
                case COMMIT:
                    commit();
                    break;
                case EXECQUERY:
                    execQuery(request);
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

    private void open(TxRequest request) {
        if (tx != null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }

        String keyspaceString = request.getOpen().getKeyspace().getValue();
        Keyspace keyspace = Keyspace.of(keyspaceString);
        tx = TxThread.open(txFactory, keyspace);
    }

    private void commit() {
        if (tx == null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }
        tx.run(GraknTx::commit);
    }

    private void execQuery(TxRequest request) {
        if (tx == null) {
            error(Status.FAILED_PRECONDITION);
            return;
        }

        boolean infer = request.getExecQuery().getInfer();
        boolean setInfer = request.getExecQuery().getSetInfer();

        String queryString = request.getExecQuery().getQuery().getValue();

        Object result = tx.runAndReturn(t -> {
            QueryBuilder graql = t.graql();

            if (setInfer) {
                graql.infer(infer);
            }

            return graql.parse(queryString).execute();
        });

        QueryResult rpcResult = QueryResult.newBuilder().setValue(PRINTER.graqlString(result)).build();

        responseObserver.onNext(TxResponse.newBuilder().setQueryResult(rpcResult).build());
    }

    @Override
    public void onError(Throwable t) {

    }

    @Override
    public void onCompleted() {
        if (tx != null) tx.close();

        if (!terminated.getAndSet(true)) {
            responseObserver.onCompleted();
        }
    }

    private void error(Status status) {
        error(status, null);
    }

    private void error(Status status, @Nullable Metadata trailers) {
        if (!terminated.getAndSet(true)) {
            responseObserver.onError(new StatusRuntimeException(status, trailers));
        }
    }
}

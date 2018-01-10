/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
 *
 */

package ai.grakn.engine.rpc;

import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.CloseTxRequest;
import ai.grakn.rpc.GraknOuterClass.CloseTxResponse;
import ai.grakn.rpc.GraknOuterClass.OpenTxRequest;
import ai.grakn.rpc.GraknOuterClass.OpenTxResponse;
import ai.grakn.rpc.GraknOuterClass.TxId;
import com.google.common.collect.Maps;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Felix Chapman
 */
public class GrpcServer implements AutoCloseable {

    private final Server server;

    private GrpcServer(Server server) {
        this.server = server;
    }

    /**
     * @throws IOException if unable to bind
     */
    public static GrpcServer create(int port, EngineGraknTxFactory txFactory) throws IOException {
        Server server = ServerBuilder.forPort(port)
                .addService(new GraknImpl(txFactory))
                .build()
                .start();
        return new GrpcServer(server);
    }

    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    static class GraknImpl extends GraknGrpc.GraknImplBase {

        private final EngineGraknTxFactory txFactory;
        private final Map<TxId, TxThread> txThreads = Maps.newConcurrentMap();
        private final AtomicInteger counter = new AtomicInteger();

        private GraknImpl(EngineGraknTxFactory txFactory) {
            this.txFactory = txFactory;
        }

        @Override
        public void openTx(OpenTxRequest request, StreamObserver<OpenTxResponse> responseObserver) {
            GraknOuterClass.Keyspace requestKeyspace = request.getKeyspace();

            Keyspace keyspace;
            try {
                keyspace = Keyspace.of(requestKeyspace.getValue());
            } catch (GraknTxOperationException e) {
                OpenTxResponse.InvalidKeyspaceError error = OpenTxResponse.InvalidKeyspaceError.getDefaultInstance();
                responseObserver.onNext(OpenTxResponse.newBuilder().setInvalidKeyspace(error).build());
                responseObserver.onCompleted();
                return;
            }

            TxId txId = TxId.newBuilder().setValue(counter.getAndIncrement()).build();

            TxThread txThread = TxThread.open(txFactory, keyspace);

            assert !txThreads.containsKey(txId) : "There should not be a transaction with this ID yet";
            txThreads.put(txId, txThread);

            OpenTxResponse.Success.Builder success = OpenTxResponse.Success.newBuilder().setTx(txId);
            OpenTxResponse response = OpenTxResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void closeTx(CloseTxRequest request, StreamObserver<CloseTxResponse> responseObserver) {
            TxId txId = request.getTx();

            @Nullable TxThread txThread = txThreads.remove(txId);

            if (txThread == null) {
                responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION));
            } else {
                txThread.close();
                responseObserver.onNext(CloseTxResponse.newBuilder().build());
                responseObserver.onCompleted();
            }
        }
    }

}


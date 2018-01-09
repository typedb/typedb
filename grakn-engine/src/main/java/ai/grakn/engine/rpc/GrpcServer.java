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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.OpenTxRequest;
import ai.grakn.rpc.GraknOuterClass.OpenTxResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

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

        private GraknImpl(EngineGraknTxFactory txFactory) {
            this.txFactory = txFactory;
        }

        @Override
        public void openTx(OpenTxRequest request, StreamObserver<OpenTxResponse> responseObserver) {
            GraknOuterClass.Keyspace requestKeyspace = request.getKeyspace();
            Keyspace keyspace = Keyspace.of(requestKeyspace.getValue());

            txFactory.tx(keyspace, GraknTxType.WRITE);

            OpenTxResponse response = OpenTxResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

    }
}


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

import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
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

    @Override
    public void close() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

    private static class GraknImpl extends GraknGrpc.GraknImplBase {

        private final EngineGraknTxFactory txFactory;

        private GraknImpl(EngineGraknTxFactory txFactory) {
            this.txFactory = txFactory;
        }

        @Override
        public StreamObserver<TxRequest> tx(StreamObserver<TxResponse> responseObserver) {
            return TxObserver.create(txFactory, responseObserver);
        }
    }
}


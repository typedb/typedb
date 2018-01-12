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

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Felix Chapman
 */
public class GrpcServer implements AutoCloseable {

    public static final Metadata.Key<String> MESSAGE = Metadata.Key.of("message", new Metadata.AsciiMarshaller<String>() {
        @Override
        public String toAsciiString(String value) {
            return value;
        }

        @Override
        public String parseAsciiString(String serialized) {
            return serialized;
        }
    });

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
        public StreamObserver<TxRequest> tx(StreamObserver<TxResponse> responseObserver) {
            return new StreamObserver<TxRequest>() {
                @Nullable GraknTx tx;
                AtomicBoolean terminated = new AtomicBoolean(false);

                @Override
                public void onNext(TxRequest request) {
                    switch (request.getRequestCase()) {
                        case OPEN:
                            String keyspaceString = request.getOpen().getKeyspace().getValue();
                            Keyspace keyspace;
                            try {
                                keyspace = Keyspace.of(keyspaceString);
                            } catch (GraknTxOperationException e) {
                                Metadata trailers = new Metadata();
                                trailers.put(MESSAGE, e.getMessage());
                                if (!terminated.getAndSet(true)) {
                                    responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN, trailers));
                                }
                                return;
                            }

                            tx = txFactory.tx(keyspace, GraknTxType.WRITE);
                            break;
                        case COMMIT:
                            tx.commit();
                            break;
                        case EXECQUERY:
                            Object result = tx.graql().parse(request.getExecQuery().getQuery().getValue()).execute();
                            responseObserver.onNext(TxResponse.newBuilder().setQueryResult(TxResponse.QueryResult.getDefaultInstance()).build());
                            break;
                        case REQUEST_NOT_SET:
                            break;
                    }
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
            };
        }
    }

}


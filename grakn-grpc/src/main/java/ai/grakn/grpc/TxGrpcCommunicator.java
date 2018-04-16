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

package ai.grakn.grpc;

import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for making Tx calls to a gRPC server - handles sending a stream of {@link TxRequest}s and receiving a
 * stream of {@link TxResponse}s.
 *
 * A request is sent with the {@link #send(TxRequest)}} method, and you can block for a response with the
 * {@link #receive()} method.
 *
 * <pre>
 * {@code
 *
 *     try (TxGrpcCommunicator tx = TxGrpcCommunicator.create(stub) {
 *         tx.send(openMessage);
 *         TxResponse doneMessage = tx.receive().ok();
 *         tx.send(commitMessage);
 *         StatusRuntimeException validationError = tx.receive.error();
 *     }
 * }
 * </pre>
 *
 * @author Felix Chapman
 */
public class TxGrpcCommunicator implements AutoCloseable {

    private final StreamObserver<TxRequest> requests;
    private final QueueingObserver responses;

    private TxGrpcCommunicator(StreamObserver<TxRequest> requests, QueueingObserver responses) {
        this.requests = requests;
        this.responses = responses;
    }

    public static TxGrpcCommunicator create(GraknGrpc.GraknStub stub) {
        QueueingObserver responseListener = new QueueingObserver();
        StreamObserver<TxRequest> requestSender = stub.tx(responseListener);
        return new TxGrpcCommunicator(requestSender, responseListener);
    }

    /**
     * Send a request and return immediately.
     *
     * This method is non-blocking - it returns immediately.
     */
    public void send(TxRequest request) {
        if (responses.terminated.get()) {
            throw GraknTxOperationException.transactionClosed(null, "The gRPC connection closed");
        }
        requests.onNext(request);
    }

    /**
     * Block until a response is returned.
     */
    public Response receive() throws InterruptedException {
        Response response = responses.poll();
        if (response.type() != Response.Type.OK) {
            close();
        }
        return response;
    }

    @Override
    public void close() {
        try{
            requests.onCompleted();
        } catch (IllegalStateException e) {
            //IGNORED
            //This is needed to handle the fact that:
            //1. Commits can lead to transaction closures and
            //2. Error can lead to connection closures but the transaction may stay open
            //When this occurs a "half-closed" state is thrown which we can safely ignore
        }
        responses.close();
    }

    public boolean isClosed(){
        return responses.terminated.get();
    }

    /**
     * A {@link StreamObserver} that stores all responses in a blocking queue.
     *
     * A response can be polled with the {@link #poll()} method.
     */
    private static class QueueingObserver implements StreamObserver<TxResponse>, AutoCloseable {

        private final BlockingQueue<Response> queue = new LinkedBlockingDeque<>();
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        @Override
        public void onNext(TxResponse value) {
            queue.add(Response.ok(value));
        }

        @Override
        public void onError(Throwable throwable) {
            terminated.set(true);
            assert throwable instanceof StatusRuntimeException : "The server only yields these exceptions";
            queue.add(Response.error((StatusRuntimeException) throwable));
        }

        @Override
        public void onCompleted() {
            terminated.set(true);
            queue.add(Response.completed());
        }

        Response poll() throws InterruptedException {
            // First check for a response without blocking
            Response response = queue.poll();

            if (response != null) {
                return response;
            }

            // Only after checking for existing messages, we check if the connection was already terminated, so we don't
            // block for a response forever
            if (terminated.get()) {
                throw GraknTxOperationException.transactionClosed(null, "The gRPC connection closed");
            }

            // Block for a response (because we are confident there are no responses and the connection has not closed)
            return queue.take();
        }

        @Override
        public void close() {
            while (!terminated.get()) {
                try {
                    poll();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * A response from the gRPC server, that may be a successful response {@link #ok(TxResponse), an error
     * {@link #error(StatusRuntimeException)}} or a "completed" message {@link #completed()}.
     */
    @AutoValue
    public abstract static class Response {

        abstract @Nullable TxResponse nullableOk();
        abstract @Nullable StatusRuntimeException nullableError();

        public final Type type() {
            if (nullableOk() != null) {
                return Type.OK;
            } else if (nullableError() != null) {
                return Type.ERROR;
            } else {
                return Type.COMPLETED;
            }
        }

        /**
         * Enum indicating the type of {@link Response}.
         */
        public enum Type {
            OK, ERROR, COMPLETED;
        }

        /**
         * If this is a successful response, retrieve it.
         *
         * @throws IllegalStateException if this is not a successful response
         */
        public final TxResponse ok() {
            TxResponse response = nullableOk();
            if (response == null) {
                throw new IllegalStateException("Expected successful response not found: " + toString());
            } else {
                return response;
            }
        }

        /**
         * If this is an error, retrieve it.
         *
         * @throws IllegalStateException if this is not an error
         */
        public final StatusRuntimeException error() {
            StatusRuntimeException throwable = nullableError();
            if (throwable == null) {
                throw new IllegalStateException("Expected error not found: " + toString());
            } else {
                return throwable;
            }
        }

        private static Response create(@Nullable TxResponse response, @Nullable StatusRuntimeException error) {
            Preconditions.checkArgument(response == null || error == null);
            return new AutoValue_TxGrpcCommunicator_Response(response, error);
        }

        static Response completed() {
            return create(null, null);
        }

        static Response error(StatusRuntimeException error) {
            return create(null, error);
        }

        static Response ok(TxResponse response) {
            return create(response, null);
        }
    }
}

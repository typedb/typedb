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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Wrapper for synchronous bidirectional streaming communication - i.e. when there is a stream of {@link Request}s and
 * a stream of {@link Response}s.
 *
 * A request is sent with the {@link #send(Request)}} method, and you can block for a response with the
 * {@link #receive()} method.
 *
 * <pre>
 * {@code
 *
 *     try (SynchronousObserver<TxRequest, TxResponse> tx = SynchronousObserver.create(stub::tx) {
 *         tx.send(openMessage);
 *         TxResponse doneMessage = tx.receive().elem();
 *         tx.send(commitMessage);
 *         Throwable validationError = tx.receive.throwable();
 *     }
 * }
 * </pre>
 *
 * @author Felix Chapman
 *
 * @param <Request> The type of requests being received
 * @param <Response> The type of responses being sent
 */
public class SynchronousObserver<Request, Response> implements AutoCloseable {

    private final StreamObserver<Request> requests;
    private final QueueingObserver<Response> responses;

    private SynchronousObserver(StreamObserver<Request> requests, QueueingObserver<Response> responses) {
        this.requests = requests;
        this.responses = responses;
    }

    /**
     * Create a {@link SynchronousObserver} using a method that accepts a {@link StreamObserver} and returns
     * a {@link StreamObserver}.
     *
     * <p>
     * This looks super-weird because it is super-weird. The reason is that the gRPC-generated methods on client-stubs
     * for bidirectional streaming calls look like this:
     * </p>
     *
     * <p>
     *     {@code StreamObserver<TxRequest> tx(StreamObserver<TxResponse> responseObserver)}
     * </p>
     *
     * <p>
     *     So, we cannot get at the {@link Request} observer until we provide a {@link Response} observer.
     *     Unfortunately the latter is created within this method below - so we must pass in a method reference:
     * </p>
     *
     * {@code SynchronousObserver.create(stub::tx)}
     */
    public static <Request, Response> SynchronousObserver<Request, Response> create(
            Function<StreamObserver<Response>, StreamObserver<Request>> createRequestObserver
    ) {
        QueueingObserver<Response> responseListener = new QueueingObserver<>();
        StreamObserver<Request> requestSender = createRequestObserver.apply(responseListener);
        return new SynchronousObserver<>(requestSender, responseListener);
    }

    /**
     * Send a request and return immediately.
     *
     * This method is non-blocking - it returns immediately.
     */
    public void send(Request request) {
        requests.onNext(request);
    }

    /**
     * Block until a response is returned.
     */
    public QueueElem<Response> receive() {
        return responses.poll();
    }

    @Override
    public void close() {
        requests.onCompleted();
        responses.close();
    }

    /**
     * A {@link StreamObserver} that stores all responses in a blocking queue.
     *
     * A response can be polled with the {@link #poll()} method.
     */
    static class QueueingObserver<T> implements StreamObserver<T>, AutoCloseable {

        private final BlockingQueue<QueueElem<T>> queue = new LinkedBlockingDeque<>();
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        @Override
        public void onNext(T value) {
            queue.add(QueueElem.elem(value));
        }

        @Override
        public void onError(Throwable throwable) {
            terminated.set(true);
            queue.add(QueueElem.error(throwable));
        }

        @Override
        public void onCompleted() {
            terminated.set(true);
            queue.add(QueueElem.completed());
        }

        QueueElem<T> poll() {
            try {
                return queue.poll(100, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // TODO: If we move this out of test code, we should handle this correctly
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            while (!terminated.get()) {
                poll();
            }
        }
    }

    /**
     * A response, that may be an element, an error or a "completed" message.
     *
     * @param <T> The type of response elements
     */
    @AutoValue
    public abstract static class QueueElem<T> {

        abstract @Nullable T nullableElem();
        abstract @Nullable Throwable nullableThrowable();

        public boolean isCompleted() {
            return nullableElem() == null && nullableThrowable() == null;
        }

        public T elem() {
            T elem = nullableElem();
            if (elem == null) {
                throw new IllegalStateException("Expected elem not found: " + toString());
            } else {
                return elem;
            }
        }

        public Throwable throwable() {
            Throwable throwable = nullableThrowable();
            if (throwable == null) {
                throw new IllegalStateException("Expected throwable not found: " + toString());
            } else {
                return throwable;
            }
        }

        private static <T> QueueElem<T> create(@Nullable T elem, @Nullable Throwable throwable) {
            Preconditions.checkArgument(elem == null || throwable == null);
            return new ai.grakn.engine.rpc.AutoValue_SynchronousObserver_QueueElem<>(elem, throwable);
        }

        static <T> QueueElem<T> completed() {
            return create(null, null);
        }

        static <T> QueueElem<T> error(Throwable throwable) {
            return create(null, throwable);
        }

        static <T> QueueElem<T> elem(T elem) {
            return create(elem, null);
        }
    }
}

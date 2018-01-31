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

package ai.grakn.grpc;

import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GraknOuterClass.TxRequest;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for synchronous bidirectional streaming communication - i.e. when there is a stream of {@link TxRequest}s and
 * a stream of {@link TxResponse}s.
 *
 * A request is sent with the {@link #send(TxRequest)}} method, and you can block for a response with the
 * {@link #receive()} method.
 *
 * <pre>
 * {@code
 *
 *     try (SynchronousObserver tx = SynchronousObserver.create(stub) {
 *         tx.send(openMessage);
 *         TxResponse doneMessage = tx.receive().elem();
 *         tx.send(commitMessage);
 *         Throwable validationError = tx.receive.throwable();
 *     }
 * }
 * </pre>
 *
 * @author Felix Chapman
 */
public class SynchronousObserver implements AutoCloseable {

    private final StreamObserver<TxRequest> requests;
    private final QueueingObserver responses;

    private SynchronousObserver(StreamObserver<TxRequest> requests, QueueingObserver responses) {
        this.requests = requests;
        this.responses = responses;
    }

    public static SynchronousObserver create(GraknGrpc.GraknStub stub) {
        QueueingObserver responseListener = new QueueingObserver();
        StreamObserver<TxRequest> requestSender = stub.tx(responseListener);
        return new SynchronousObserver(requestSender, responseListener);
    }

    /**
     * Send a request and return immediately.
     *
     * This method is non-blocking - it returns immediately.
     */
    public void send(TxRequest request) {
        requests.onNext(request);
    }

    /**
     * Block until a response is returned.
     */
    public QueueElem receive() {
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
    static class QueueingObserver implements StreamObserver<TxResponse>, AutoCloseable {

        private final BlockingQueue<QueueElem> queue = new LinkedBlockingDeque<>();
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        @Override
        public void onNext(TxResponse value) {
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

        QueueElem poll() {
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
     */
    @AutoValue
    public abstract static class QueueElem {

        abstract @Nullable TxResponse nullableElem();
        abstract @Nullable Throwable nullableThrowable();

        public boolean isCompleted() {
            return nullableElem() == null && nullableThrowable() == null;
        }

        public TxResponse elem() {
            TxResponse elem = nullableElem();
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

        private static QueueElem create(@Nullable TxResponse elem, @Nullable Throwable throwable) {
            Preconditions.checkArgument(elem == null || throwable == null);
            return new AutoValue_SynchronousObserver_QueueElem(elem, throwable);
        }

        static QueueElem completed() {
            return create(null, null);
        }

        static QueueElem error(Throwable throwable) {
            return create(null, throwable);
        }

        static QueueElem elem(TxResponse elem) {
            return create(elem, null);
        }
    }
}

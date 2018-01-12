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
 * @author Felix Chapman
 */
public class BidirectionalObserver<Request, Response> implements AutoCloseable {

    private final StreamObserver<Request> requests;
    private final BlockingObserver<Response> responses;

    private BidirectionalObserver(StreamObserver<Request> requests, BlockingObserver<Response> responses) {
        this.requests = requests;
        this.responses = responses;
    }

    public static <Request, Response> BidirectionalObserver<Request, Response> create(StreamObserver<Request> requests) {
        return create(responses -> requests);
    }

    public static <Request, Response> BidirectionalObserver<Request, Response> create(Function<StreamObserver<Response>, StreamObserver<Request>> f) {
        BlockingObserver<Response> responses = new BlockingObserver<>();
        StreamObserver<Request> requests = f.apply(responses);
        return new BidirectionalObserver<>(requests, responses);
    }

    public void send(Request request) {
        requests.onNext(request);
    }

    public QueueElem<Response> receive() {
        return responses.poll();
    }

    @Override
    public void close() {
        requests.onCompleted();
        responses.close();
    }

    static class BlockingObserver<T> implements StreamObserver<T>, AutoCloseable {

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

        public QueueElem<T> poll() {
            try {
                return queue.poll(100, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // TODO
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

    @AutoValue
    abstract static class QueueElem<T> {

        abstract @Nullable T elem();
        abstract @Nullable Throwable throwable();

        boolean isCompleted() {
            return elem() == null && throwable() == null;
        }

        private static <T> QueueElem<T> create(@Nullable T elem, @Nullable Throwable throwable) {
            Preconditions.checkArgument(elem == null || throwable == null);
            return new AutoValue_BidirectionalObserver_QueueElem<>(elem, throwable);
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

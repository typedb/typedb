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

import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Felix Chapman
 */
class BidirectionalObserver<Request, Response> implements AutoCloseable {

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

    public Response receive() {
        return responses.poll();
    }

    public Optional<Throwable> error() {
        return Optional.ofNullable(responses.throwable);
    }

    @Override
    public void close() {
        requests.onCompleted();
        responses.close();
    }

    static class BlockingObserver<T> implements StreamObserver<T>, AutoCloseable {

        private final BlockingQueue<T> queue = new LinkedBlockingDeque<>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private @Nullable Throwable throwable = null;

        @Override
        public void onNext(T value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            this.throwable = throwable;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }

        public T poll() {
            if (isCompleted()) {
                if (throwable != null) {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    } else {
                        throw new IllegalStateException(throwable);
                    }
                } else {
                    throw new IllegalStateException("Observer has completed");
                }
            }

            try {
                return queue.poll(100, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        private boolean isCompleted() {
            return latch.getCount() == 0;
        }

        @Override
        public void close() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }
}

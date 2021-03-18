/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.concurrent.producer;

import grakn.common.collection.Either;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.AbstractFunctionalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_INTERRUPTION;

public class ProducerIterator<T> extends AbstractFunctionalIterator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerIterator.class);

    // TODO: why does 'producers' have to be ConcurrentLinkedQueue? see method recycle() below
    private final ConcurrentLinkedQueue<Producer<T>> producers;
    private final Executor executor;
    private final Queue queue;
    private final int batchSize;
    private final long limit;
    private long requested;
    private long consumed;

    private T next;
    private State state;

    public ProducerIterator(List<Producer<T>> producers, int batchSize, long limit, Executor executor) {
        // TODO: Could we optimise IterableProducer by accepting FunctionalIterator<Producer<T>> instead?
        assert !producers.isEmpty() && batchSize < Integer.MAX_VALUE / 2;
        this.producers = new ConcurrentLinkedQueue<>(producers);
        this.queue = new Queue();
        this.batchSize = batchSize;
        this.limit = limit;
        this.executor = executor;
        this.requested = 0;
        this.consumed = 0;
        this.state = State.EMPTY;
    }

    private void mayProduceBatch() {
        if (consumed % batchSize == 0) mayProduce();
    }

    private void mayProduce() {
        if (requested == limit) return;
        synchronized (queue) {
            if (producers.isEmpty()) return;
            final int request = batchSize < (limit - requested) ? batchSize : (int) (limit - requested);
            requested += request;
            Producer<T> producer = producers.peek();
            executor.execute(() -> producer.produce(queue, request, executor));
        }
    }

    private enum State {EMPTY, FETCHED, COMPLETED}

    @Override
    public boolean hasNext() {
        if (state == State.COMPLETED || consumed == limit) return false;
        else if (state == State.FETCHED) return true;
        else mayProduceBatch();

        Either<Result<T>, Done> result = queue.take();

        if (result.isFirst()) {
            next = result.first().value();
            state = State.FETCHED;
        } else {
            Done done = result.second();
            recycle();
            state = State.COMPLETED;
            if (done.error().isPresent()) {
                throw GraknException.of(done.error().get());
            }
        }

        return state == State.FETCHED;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        consumed++;
        return next;
    }

    @Override
    public void recycle() {
        // TODO: If this method is wrapped in synchronize(queue), we won't need producers to be ConcurrentLinkedQueue
        //       However, doing so would also cause a deadlock. Let's investigate this soon.
        producers.forEach(Producer::recycle);
    }

    private static class Result<T> {

        @Nullable
        private final T value;

        private Result(@Nullable T value) {
            this.value = value;
        }

        @Nullable
        private T value() {
            return value;
        }
    }

    private static class Done {
        @Nullable
        private final Throwable error;

        private Done(Throwable error) {
            this.error = error;
        }

        private static Done success() {
            return new Done(null);
        }

        private static Done error(Throwable e) {
            return new Done(e);
        }

        private Optional<Throwable> error() {
            return Optional.ofNullable(error);
        }
    }

    @ThreadSafe
    private class Queue implements Producer.Queue<T> {

        private final LinkedBlockingQueue<Either<Result<T>, Done>> blockingQueue;

        private Queue() {
            this.blockingQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public synchronized void put(T item) {
            try {
                blockingQueue.put(Either.first(new Result<>(item)));
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        @Override
        public synchronized void done() {
            done(null);
        }

        @Override
        public synchronized void done(@Nullable Throwable error) {
            assert !producers.isEmpty();
            producers.remove();
            try {
                if (error != null) blockingQueue.put(Either.second(Done.error(error)));
                else if (producers.isEmpty()) blockingQueue.put(Either.second(Done.success()));
                else mayProduce();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        private Either<Result<T>, Done> take() {
            try {
                return blockingQueue.take();
            } catch (InterruptedException e) {
                throw GraknException.of(UNEXPECTED_INTERRUPTION);
            }
        }

        private int size() {
            return blockingQueue.size();
        }
    }
}

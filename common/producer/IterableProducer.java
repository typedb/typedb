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

package grakn.core.common.producer;

import grakn.common.collection.Either;
import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.concurrent.ManagedBlockingQueue;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;

import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class IterableProducer<T> {

    private static final int BUFFER_MIN_SIZE = 32;
    private static final int BUFFER_MAX_SIZE = 64;

    private final ConcurrentLinkedQueue<Producer<T>> producers;
    private final Iterator iterator;
    private final Queue queue;
    private final AtomicInteger pending;
    private final int bufferMinSize;
    private final int bufferMaxSize;

    public IterableProducer(List<Producer<T>> producers) {
        this(producers, BUFFER_MIN_SIZE, BUFFER_MAX_SIZE);
    }

    public IterableProducer(List<Producer<T>> producers, int bufferMinSize, int bufferMaxSize) {
        // TODO: Could we optimise IterableProducer by accepting ResourceIterator<Producer<T>> instead?
        this.producers = new ConcurrentLinkedQueue<>(producers);
        this.iterator = new Iterator();
        this.queue = new Queue();
        this.pending = new AtomicInteger(0);
        this.bufferMinSize = bufferMinSize;
        this.bufferMaxSize = bufferMaxSize;
    }

    public IterableProducer<T>.Iterator iterator() {
        return iterator;
    }

    public void mayProduce() {
        int available = bufferMaxSize - queue.size() - pending.get();
        if (available > bufferMaxSize - bufferMinSize) {
            pending.addAndGet(available);
            ExecutorService.forkJoinPool().submit(() -> {
                assert !producers.isEmpty();
                producers.peek().produce(queue, available);
            });
        }
    }

    private static class Done {
        @Nullable
        private final Throwable error;

        private Done(Throwable error) {
            this.error = error;
        }

        public static Done success() {
            return new Done(null);
        }

        public static Done error(Throwable e) {
            return new Done(e);
        }

        public Optional<Throwable> error() {
            return Optional.ofNullable(error);
        }
    }

    private enum State {EMPTY, FETCHED, COMPLETED}

    public class Iterator implements ResourceIterator<T> {

        private T next;
        private State state;

        Iterator() {
            state = State.EMPTY;
        }

        @Override
        public boolean hasNext() {
            if (state == State.COMPLETED) return false;
            else if (state == State.FETCHED) return true;
            else mayProduce();

            Either<T, Done> result = queue.take();

            if (result.isFirst()) {
                next = result.first();
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
            return next;
        }

        @Override
        public void recycle() {
            producers.forEach(Producer::recycle);
        }
    }

    private class Queue implements Producer.Queue<T> {

        private final ManagedBlockingQueue<Either<T, Done>> blockingQueue;

        private Queue() {
            this.blockingQueue = new ManagedBlockingQueue<>();
        }

        @Override
        public void put(T item) {
            try {
                blockingQueue.put(Either.first(item));
                pending.decrementAndGet();
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
        }

        @Override
        public void done(Producer<T> caller) {
            done(caller, null);
        }

        @Override
        public void done(Producer<T> caller, @Nullable Throwable error) {
            assert !producers.isEmpty();
            if (producers.peek().equals(caller)) {
                producers.remove();
                pending.set(0);

                if (producers.isEmpty()) {
                    try {
                        Done done = error == null ? Done.success() : Done.error(error);
                        blockingQueue.put(Either.second(done));
                    } catch (InterruptedException e) {
                        throw GraknException.of(e);
                    }
                } else {
                    mayProduce();
                }
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private Either<T, Done> take() {
            try {
                return blockingQueue.take();
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
        }

        private int size() {
            return blockingQueue.size();
        }
    }
}

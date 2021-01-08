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

package grakn.core.common.iterator;

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.concurrent.ResizingBlockingQueue;

import java.util.List;
import java.util.NoSuchElementException;

public class ParallelIterators<T> implements ResourceIterator<T> {

    private final ResizingBlockingQueue<T> queue;
    private final List<ResourceIterator<T>> iterators;
    private State state;
    private T next;

    private enum State {EMPTY, FETCHED, COMPLETED}

    public ParallelIterators(List<ResourceIterator<T>> iterators) {
        this(new ResizingBlockingQueue<>(), iterators);
    }

    public ParallelIterators(List<ResourceIterator<T>> iterators, int bufferSize) {
        this(new ResizingBlockingQueue<>(bufferSize), iterators);
    }

    public ParallelIterators(List<ResourceIterator<T>> iterators, int bufferSize, int bufferMultiplier) {
        this(new ResizingBlockingQueue<>(bufferSize, bufferMultiplier), iterators);
    }

    private ParallelIterators(ResizingBlockingQueue<T> queue, List<ResourceIterator<T>> iterators) {
        this.queue = queue;
        this.state = State.EMPTY;
        this.next = null;
        this.iterators = iterators;
        this.iterators.forEach(iterator -> {
            queue.incrementPublisher();
            ExecutorService.forkJoinPool().submit(() -> {
                while (!queue.isCancelled() && iterator.hasNext()) queue.put(iterator.next());
                queue.decrementPublisher();
            });
        });
    }

    @Override
    public boolean hasNext() {
        if (state == State.FETCHED) return true;
        else if (state == State.COMPLETED) return false;
        else next = queue.take();

        if (next == null) state = State.COMPLETED;
        else state = State.FETCHED;

        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return next;
    }

    @Override
    public void recycle() {
        queue.cancel();
        this.iterators.forEach(ResourceIterator::recycle);
    }

    @Override
    protected void finalize() {
        queue.cancel();
    }
}

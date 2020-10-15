/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.common.concurrent.CommonExecutorService;
import grakn.core.common.concurrent.ResizingBlockingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinTask;

public class ParallelIterators<T> implements ComposableIterator<T> {

    private final ResizingBlockingQueue<T> queue;
    private final List<ForkJoinTask<?>> producers;
    private State state;
    private T next;

    private enum State {EMPTY, FETCHED, COMPLETED}

    public ParallelIterators(final List<ComposableIterator<T>> iterators) {
        queue = new ResizingBlockingQueue<>();
        producers = new ArrayList<>();
        state = State.EMPTY;
        next = null;
        iterators.forEach(iterator -> {
            queue.incrementPublisher();
            producers.add(CommonExecutorService.get().submit(() -> {
                while (iterator.hasNext()) queue.put(iterator.next());
                queue.decrementPublisher();
            }));
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
    protected void finalize() {
        queue.cancel();
        producers.forEach(t -> t.cancel(true));
    }
}

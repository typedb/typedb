/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

class MergeMappedIterator<T, U extends Comparable<? super U>> extends AbstractFunctionalIterator.Sorted<U> {

    private final FunctionalIterator<T> source;
    private final Function<T, FunctionalIterator.Sorted<U>> mappingFn;
    private final PriorityQueue<ComparableSortedIterator> queue;
    private final List<FunctionalIterator.Sorted<U>> notInQueue;
    private State state;
    private U last;

    private enum State {
        INIT, NOT_READY, FETCHED, COMPLETED;
    }

    MergeMappedIterator(FunctionalIterator<T> source, Function<T, FunctionalIterator.Sorted<U>> mappingFn) {
        this.source = source;
        this.mappingFn = mappingFn;
        this.queue = new PriorityQueue<>();
        this.state = State.INIT;
        this.notInQueue = new ArrayList<>();
        this.last = null;
    }

    private class ComparableSortedIterator implements Comparable<ComparableSortedIterator> {

        private final FunctionalIterator.Sorted<U> iter;

        private ComparableSortedIterator(FunctionalIterator.Sorted<U> iter){
            assert iter.hasNext();
            this.iter = iter;
        }

        @Override
        public int compareTo(ComparableSortedIterator other) {
            return iter.peek().compareTo(other.iter.peek());
        }
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                initialise();
                return state == State.FETCHED;
            case FETCHED:
                return true;
            case NOT_READY:
                tryFetch();
                return state == State.FETCHED;
            case COMPLETED:
                return false;
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private void tryFetch() {
        if (!notInQueue.isEmpty()) {
            notInQueue.forEach(sortedIterator -> {
                if (sortedIterator.hasNext()) queue.add(new ComparableSortedIterator(sortedIterator));
            });
            notInQueue.clear();
        }
        if (queue.isEmpty()) state = State.COMPLETED;
        else state = State.FETCHED;
    }

    private void initialise() {
        source.forEachRemaining(value -> {
            FunctionalIterator.Sorted<U> sortedIterator = mappingFn.apply(value);
            if (sortedIterator.hasNext()) queue.add(new ComparableSortedIterator(sortedIterator));
        });
        source.recycle();
        if (queue.isEmpty()) state = State.COMPLETED;
        else state = State.FETCHED;
    }

    @Override
    public U next() {
        if (!hasNext()) throw new NoSuchElementException();
        ComparableSortedIterator nextIter = this.queue.poll();
        assert nextIter != null;
        FunctionalIterator.Sorted<U> sortedIterator = nextIter.iter;
        last = sortedIterator.next();
        state = State.NOT_READY;
        notInQueue.add(sortedIterator);
        return last;
    }

    @Override
    public U peek() {
        if (!hasNext()) throw new NoSuchElementException();
        assert !queue.isEmpty();
        return queue.peek().iter.peek();
    }

    @Override
    public void forward(U target) {
        if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        notInQueue.forEach(iter -> iter.forward(target));
        queue.forEach(queueNode -> {
            FunctionalIterator.Sorted<U> iter = queueNode.iter;
            iter.forward(target);
            notInQueue.add(iter);
        });
        queue.clear();
        state = State.NOT_READY;
    }

    @Override
    public void recycle() {
        queue.forEach(queueNode -> queueNode.iter.recycle());
        queue.clear();
        notInQueue.forEach(FunctionalIterator::recycle);
        notInQueue.clear();
        source.recycle();
    }
}

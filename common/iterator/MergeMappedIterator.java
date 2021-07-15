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
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class MergeMappedIterator<T, U extends Comparable<? super U>, ITER extends FunctionalIterator.Sorted<U>>
        extends AbstractFunctionalIterator.Sorted<U> {

    private final Function<T, ITER> mappingFn;
    final FunctionalIterator<T> iterator;
    final PriorityQueue<ComparableSortedIterator> queue;
    final List<ITER> notInQueue;
    State state;
    U last;

    enum State {INIT, NOT_READY, FETCHED, COMPLETED}

    MergeMappedIterator(FunctionalIterator<T> iterator, Function<T, ITER> mappingFn) {
        this.iterator = iterator;
        this.mappingFn = mappingFn;
        this.queue = new PriorityQueue<>();
        this.state = State.INIT;
        this.notInQueue = new ArrayList<>();
        this.last = null;
    }

    private class ComparableSortedIterator implements Comparable<ComparableSortedIterator> {

        private final ITER iter;

        private ComparableSortedIterator(ITER iter) {
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
        iterator.forEachRemaining(value -> {
            ITER sortedIterator = mappingFn.apply(value);
            if (sortedIterator.hasNext()) queue.add(new ComparableSortedIterator(sortedIterator));
        });
        iterator.recycle();
        if (queue.isEmpty()) state = State.COMPLETED;
        else state = State.FETCHED;
    }

    @Override
    public U next() {
        if (!hasNext()) throw new NoSuchElementException();
        ComparableSortedIterator nextIter = this.queue.poll();
        assert nextIter != null;
        ITER sortedIterator = nextIter.iter;
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
    public void recycle() {
        queue.forEach(queueNode -> queueNode.iter.recycle());
        queue.clear();
        notInQueue.forEach(FunctionalIterator::recycle);
        notInQueue.clear();
        iterator.recycle();
    }

    static class Forwardable<T, U extends Comparable<? super U>>
            extends MergeMappedIterator<T, U, FunctionalIterator.Sorted.Forwardable<U>>
            implements FunctionalIterator.Sorted.Forwardable<U> {

        Forwardable(FunctionalIterator<T> source, Function<T, FunctionalIterator.Sorted.Forwardable<U>> mappingFn) {
            super(source, mappingFn);
        }

        @Override
        public void forward(U target) {
            if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            notInQueue.forEach(iter -> iter.forward(target));
            queue.forEach(queueNode -> {
                FunctionalIterator.Sorted.Forwardable<U> iter = queueNode.iter;
                iter.forward(target);
                notInQueue.add(iter);
            });
            queue.clear();
            state = State.NOT_READY;
        }

        @SafeVarargs
        @Override
        public final FunctionalIterator.Sorted.Forwardable<U> merge(FunctionalIterator.Sorted.Forwardable<U>... iterators) {
            return Iterators.Sorted.merge(this, iterators);
        }

        @Override
        public <V extends Comparable<? super V>> FunctionalIterator.Sorted.Forwardable<V> mapSorted(
                Function<U, V> mappingFn, Function<V, U> reverseMappingFn) {
            return Iterators.Sorted.mapSorted(this, mappingFn, reverseMappingFn);
        }

        @Override
        public FunctionalIterator.Sorted.Forwardable<U> distinct() {
            return Iterators.Sorted.distinct(this);
        }

        @Override
        public FunctionalIterator.Sorted.Forwardable<U> filter(Predicate<U> predicate) {
            return Iterators.Sorted.filter(this, predicate);
        }

    }
}

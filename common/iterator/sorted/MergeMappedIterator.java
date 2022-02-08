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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class MergeMappedIterator<T, U extends Comparable<? super U>, ORDER extends Order, ITER extends SortedIterator<U, ORDER>>
        extends AbstractSortedIterator<U, ORDER> {

    private final Function<T, ITER> mappingFn;
    final FunctionalIterator<T> iterator;
    final PriorityQueue<ComparableSortedIterator> queue;
    final List<ITER> notInQueue;
    State state;
    U last;

    enum State {INIT, NOT_READY, FETCHED, COMPLETED}

    public MergeMappedIterator(ORDER order, FunctionalIterator<T> iterator, Function<T, ITER> mappingFn) {
        super(order);
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
            return order.compare(iter.peek(), other.iter.peek());
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

    public static class Seekable<T, U extends Comparable<? super U>, ORDER extends Order>
            extends MergeMappedIterator<T, U, ORDER, SortedIterator.Seekable<U, ORDER>>
            implements SortedIterator.Seekable<U, ORDER> {

        public Seekable(ORDER order, FunctionalIterator<T> source, Function<T, SortedIterator.Seekable<U, ORDER>> mappingFn) {
            super(order, source, mappingFn);
        }

        @Override
        public void seek(U target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            notInQueue.forEach(iter -> iter.seek(target));
            queue.forEach(queueNode -> {
                SortedIterator.Seekable<U, ORDER> iter = queueNode.iter;
                iter.seek(target);
                notInQueue.add(iter);
            });
            queue.clear();
            state = State.NOT_READY;
        }

        @Override
        public final SortedIterator.Seekable<U, ORDER> merge(SortedIterator.Seekable<U, ORDER> iterator) {
            return Iterators.Sorted.Seekable.merge(this, iterator);
        }

        @Override
        public <V extends Comparable<? super V>, ORD extends Order> SortedIterator.Seekable<V, ORD> mapSorted(
                ORD order, Function<U, V> mappingFn, Function<V, U> reverseMappingFn) {
            return Iterators.Sorted.Seekable.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Seekable<U, ORDER> distinct() {
            return Iterators.Sorted.Seekable.distinct(this);
        }

        @Override
        public SortedIterator.Seekable<U, ORDER> filter(Predicate<U> predicate) {
            return Iterators.Sorted.Seekable.filter(this, predicate);
        }

        @Override
        public SortedIterator.Seekable<U, ORDER> limit(long limit) {
            return Iterators.Sorted.Seekable.limit(this, limit);
        }

        @Override
        public SortedIterator.Seekable<U, ORDER> onConsumed(Runnable function) {
            return Iterators.Sorted.Seekable.onConsume(this, function);
        }

        @Override
        public SortedIterator.Seekable<U, ORDER> onFinalised(Runnable function) {
            return Iterators.Sorted.Seekable.onFinalise(this, function);
        }
    }
}

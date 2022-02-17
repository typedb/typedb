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
    final PriorityQueue<ComparableSortedIterator> fetched;
    final List<ITER> unfetched;
    State state;
    U last;

    enum State {INIT, NOT_READY, FETCHED, COMPLETED}

    public MergeMappedIterator(FunctionalIterator<T> iterator, Function<T, ITER> mappingFn, ORDER order) {
        super(order);
        this.iterator = iterator;
        this.mappingFn = mappingFn;
        this.fetched = new PriorityQueue<>();
        this.state = State.INIT;
        this.unfetched = new ArrayList<>();
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
        if (!unfetched.isEmpty()) {
            unfetched.forEach(sortedIterator -> {
                if (sortedIterator.hasNext()) fetched.add(new ComparableSortedIterator(sortedIterator));
            });
            unfetched.clear();
        }
        if (fetched.isEmpty()) state = State.COMPLETED;
        else state = State.FETCHED;
    }

    void initialise() {
        iterator.forEachRemaining(value -> {
            ITER sortedIterator = initialiseIterator(value);
            if (sortedIterator.hasNext()) fetched.add(new ComparableSortedIterator(sortedIterator));
        });
        if (fetched.isEmpty()) state = State.COMPLETED;
        else state = State.FETCHED;
    }

    ITER initialiseIterator(T value) {
        return mappingFn.apply(value);
    }

    @Override
    public U next() {
        if (!hasNext()) throw new NoSuchElementException();
        ComparableSortedIterator nextIter = this.fetched.poll();
        assert nextIter != null;
        ITER sortedIterator = nextIter.iter;
        last = sortedIterator.next();
        state = State.NOT_READY;
        unfetched.add(sortedIterator);
        return last;
    }

    @Override
    public U peek() {
        if (!hasNext()) throw new NoSuchElementException();
        assert !fetched.isEmpty();
        return fetched.peek().iter.peek();
    }

    @Override
    public void recycle() {
        fetched.forEach(queueNode -> queueNode.iter.recycle());
        fetched.clear();
        unfetched.forEach(FunctionalIterator::recycle);
        unfetched.clear();
        iterator.recycle();
    }

    public static class Seekable<T, U extends Comparable<? super U>, ORDER extends Order>
            extends MergeMappedIterator<T, U, ORDER, SortedIterator.Seekable<U, ORDER>>
            implements SortedIterator.Seekable<U, ORDER> {

        private U initialSeek;

        public Seekable(FunctionalIterator<T> source, Function<T, SortedIterator.Seekable<U, ORDER>> mappingFn, ORDER order) {
            super(source, mappingFn, order);
        }

        @Override
        SortedIterator.Seekable<U, ORDER> initialiseIterator(T value) {
            SortedIterator.Seekable<U, ORDER> iterator = super.initialiseIterator(value);
            if (initialSeek != null) iterator.seek(initialSeek);
            return iterator;
        }

        @Override
        public void seek(U target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            if (state == State.INIT) {
                initialSeek = target;
            } else if (state == State.FETCHED) {
                if (order.isValidNext(target, peek())) return;
                seekFetched(target);
                state = State.NOT_READY;
            } else if (state == State.NOT_READY) {
                seekUnfetched(target);
                seekFetched(target);
            }
        }

        private void seekUnfetched(U target) {
            unfetched.forEach(iter -> iter.seek(target));
        }

        private void seekFetched(U target) {
            fetched.forEach(queueNode -> {
                queueNode.iter.seek(target);
                unfetched.add(queueNode.iter);
            });
            fetched.clear();
        }

        @Override
        public final SortedIterator.Seekable<U, ORDER> merge(SortedIterator.Seekable<U, ORDER> iterator) {
            return Iterators.Sorted.Seekable.merge(this, iterator);
        }

        @Override
        public <V extends Comparable<? super V>, ORD extends Order> SortedIterator.Seekable<V, ORD> mapSorted(
                Function<U, V> mappingFn, Function<V, U> reverseMappingFn, ORD order) {
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

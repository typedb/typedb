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

    private enum State {INIT, NOT_READY, FETCHED, COMPLETED}

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
            return order.orderer().compare(iter.peek(), other.iter.peek());
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

    public static class Forwardable<T, U extends Comparable<? super U>, ORDER extends Order>
            extends MergeMappedIterator<T, U, ORDER, SortedIterator.Forwardable<U, ORDER>>
            implements SortedIterator.Forwardable<U, ORDER> {

        private U initialForward;

        public Forwardable(FunctionalIterator<T> source, Function<T, SortedIterator.Forwardable<U, ORDER>> mappingFn, ORDER order) {
            super(source, mappingFn, order);
        }

        @Override
        SortedIterator.Forwardable<U, ORDER> initialiseIterator(T value) {
            SortedIterator.Forwardable<U, ORDER> iterator = super.initialiseIterator(value);
            if (initialForward != null) iterator.forward(initialForward);
            return iterator;
        }

        @Override
        public void forward(U target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            if (state == State.INIT) {
                initialForward = target;
            } else if (state == State.FETCHED) {
                if (order.isValidNext(target, peek())) return;
                forwardFetched(target);
                state = State.NOT_READY;
            } else if (state == State.NOT_READY) {
                forwardUnfetched(target);
                forwardFetched(target);
            }
        }

        private void forwardUnfetched(U target) {
            unfetched.forEach(iter -> iter.forward(target));
        }

        private void forwardFetched(U target) {
            fetched.forEach(queueNode -> {
                queueNode.iter.forward(target);
                unfetched.add(queueNode.iter);
            });
            fetched.clear();
        }

        @Override
        public final SortedIterator.Forwardable<U, ORDER> merge(SortedIterator.Forwardable<U, ORDER> iterator) {
            return SortedIterators.Forwardable.merge(this, iterator);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> intersect(SortedIterator.Forwardable<U, ORDER> iterator) {
            return SortedIterators.Forwardable.intersect(this, iterator);
        }

        @Override
        public <V extends Comparable<? super V>, ORD extends Order> SortedIterator.Forwardable<V, ORD> mapSorted(
                Function<U, V> mappingFn, Function<V, U> reverseMappingFn, ORD order) {
            return SortedIterators.Forwardable.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> distinct() {
            return SortedIterators.Forwardable.distinct(this);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> filter(Predicate<U> predicate) {
            return SortedIterators.Forwardable.filter(this, predicate);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> limit(long limit) {
            return SortedIterators.Forwardable.limit(this, limit);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> onConsumed(Runnable function) {
            return SortedIterators.Forwardable.onConsume(this, function);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> onFinalise(Runnable function) {
            return SortedIterators.Forwardable.onFinalise(this, function);
        }
    }
}

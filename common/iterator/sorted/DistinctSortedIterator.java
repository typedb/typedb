/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

/**
 * Distinct iterator implemented first by comparator, then by equality.
 *
 * Assumes elements which are equal by comparison MAY be non-equal, but that
 * all elements which are not-equal by comparator are NEVER equal.
 *
 * This leads to an efficient distinct iterator, which uses the comparator to cut the size of the deduplication set.
 * When the comparator and equality are perfectly aligned, the space complexity is O(1).
 */
public class DistinctSortedIterator<T extends Comparable<? super T>, ORDER extends Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    final ITER iterator;
    final Set<T> equalByComparator;
    T last;
    State state;

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    public DistinctSortedIterator(ITER iterator) {
        super(iterator.order());
        this.iterator = iterator;
        this.equalByComparator = new HashSet<>();
        last = null;
        state = State.INIT;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                return fetchFirst();
            case EMPTY:
                return fetchNext();
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private boolean fetchFirst() {
        if (iterator.hasNext()) {
            equalByComparator.add(iterator.peek());
            state = State.FETCHED;
        } else state = State.COMPLETED;
        return state == State.FETCHED;
    }

    private boolean fetchNext() {
        assert last != null;
        while (iterator.hasNext()) {
            T next = iterator.peek();
            int comparison = next.compareTo(last);
            if (comparison == 0 && equalByComparator.contains(next)) {
                iterator.next();
                continue;
            }
            if (comparison != 0) equalByComparator.clear();
            equalByComparator.add(next);
            state = State.FETCHED;
            break;
        }
        return state == State.FETCHED;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        last = iterator.next();
        state = State.EMPTY;
        return last;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterator.peek();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    public static class Forwardable<T extends Comparable<? super T>, ORDER extends Order>
            extends DistinctSortedIterator<T, ORDER, SortedIterator.Forwardable<T, ORDER>>
            implements SortedIterator.Forwardable<T, ORDER> {

        public Forwardable(SortedIterator.Forwardable<T, ORDER> source) {
            super(source);
        }

        @Override
        public void forward(T target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            iterator.forward(target);
            if (state == State.FETCHED) state = State.EMPTY;
        }

        @Override
        public final SortedIterator.Forwardable<T, ORDER> merge(SortedIterator.Forwardable<T, ORDER> iterator) {
            return SortedIterators.Forwardable.merge(this, iterator);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> intersect(SortedIterator.Forwardable<T, ORDER> iterator) {
            return SortedIterators.Forwardable.intersect(this, iterator);
        }

        @Override
        public <U extends Comparable<? super U>, ORD extends Order> SortedIterator.Forwardable<U, ORD> mapSorted(
                Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order) {
            return SortedIterators.Forwardable.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> distinct() {
            return SortedIterators.Forwardable.distinct(this);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> filter(Predicate<T> predicate) {
            return SortedIterators.Forwardable.filter(this, predicate);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> limit(long limit) {
            return SortedIterators.Forwardable.limit(this, limit);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> onConsumed(Runnable function) {
            return SortedIterators.Forwardable.onConsume(this, function);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> onFinalise(Runnable function) {
            return SortedIterators.Forwardable.onFinalise(this, function);
        }
    }
}

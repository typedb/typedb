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
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class WhileSortedIterator<T extends Comparable<? super T>, ORDER extends Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    final ITER source;
    private final Function<T, Boolean> condition;
    State state;
    T last;

    private enum State {EMPTY, FETCHED, COMPLETED}

    protected WhileSortedIterator(ITER source, Function<T, Boolean> condition) {
        super(source.order());
        this.source = source;
        this.condition = condition;
        this.state = State.EMPTY;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case EMPTY:
                return fetchAndCheck();
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private boolean fetchAndCheck() {
        if (!source.hasNext() || !condition.apply(source.peek())) {
            state = State.COMPLETED;
            recycle();
        } else {
            state = State.FETCHED;
        }
        return state == State.FETCHED;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return source.peek();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T next = source.next();
        assert last == null || order.inOrder(last, next) : "Sorted mapped iterator produces out of order values";
        state = State.EMPTY;
        last = next;
        return next;
    }

    @Override
    public void recycle() {
        source.recycle();
    }

    public static class Forwardable<T extends Comparable<? super T>, ORDER extends Order>
            extends WhileSortedIterator<T, ORDER, SortedIterator.Forwardable<T, ORDER>>
            implements SortedIterator.Forwardable<T, ORDER> {

        public Forwardable(SortedIterator.Forwardable<T, ORDER> source, Function<T, Boolean> condition) {
            super(source, condition);
        }

        @Override
        public void forward(T target) {
            if (last != null && !order.inOrder(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            if (state == State.EMPTY) {
                source.forward(target);
            } else if (state == State.FETCHED) {
                if (order.inOrder(target, peek())) return;
                last = peek();
                source.forward(target);
                state = State.EMPTY;
            }
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
        public SortedIterator.Forwardable<T, ORDER> takeWhile(Function<T, Boolean> condition) {
            return SortedIterators.Forwardable.takeWhile(this, condition);
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

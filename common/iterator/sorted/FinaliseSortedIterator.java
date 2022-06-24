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

import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class FinaliseSortedIterator<T extends Comparable<? super T>, ORDER extends Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    private final Runnable function;
    final ITER iterator;
    T last;

    public FinaliseSortedIterator(ITER iterator, Runnable function) {
        super(iterator.order());
        this.iterator = iterator;
        this.function = function;
        this.last = null;
    }

    @Override
    public T peek() {
        return iterator.peek();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        last = iterator.next();
        return last;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    @Override
    protected void finalize() {
        function.run();
    }

    public static class Forwardable<T extends Comparable<? super T>, ORDER extends Order>
            extends FinaliseSortedIterator<T, ORDER, SortedIterator.Forwardable<T, ORDER>>
            implements SortedIterator.Forwardable<T, ORDER> {

        public Forwardable(SortedIterator.Forwardable<T, ORDER> source, Runnable function) {
            super(source, function);
        }

        @Override
        public void forward(T target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            iterator.forward(target);
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

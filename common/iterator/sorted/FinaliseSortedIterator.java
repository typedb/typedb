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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class FinaliseSortedIterator<T extends Comparable<? super T>, ORDER extends Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    private final Runnable function;
    final ITER iterator;
    T last;

    FinaliseSortedIterator(ITER iterator, Runnable function) {
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

    public static class Seekable<T extends Comparable<? super T>, ORDER extends Order>
            extends FinaliseSortedIterator<T, ORDER, SortedIterator.Seekable<T, ORDER>>
            implements SortedIterator.Seekable<T, ORDER> {

        public Seekable(SortedIterator.Seekable<T, ORDER> source, Runnable function) {
            super(source, function);
        }

        @Override
        public void seek(T target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            iterator.seek(target);
        }

        @Override
        public final SortedIterator.Seekable<T, ORDER> merge(SortedIterator.Seekable<T, ORDER> iterator) {
            return Iterators.Sorted.merge(this, iterator);
        }

        @Override
        public <U extends Comparable<? super U>, ORD extends Order> SortedIterator.Seekable<U, ORD> mapSorted(
                ORD order, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            return Iterators.Sorted.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> distinct() {
            return Iterators.Sorted.distinct(this);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> filter(Predicate<T> predicate) {
            return Iterators.Sorted.filter(this, predicate);
        }
    }
}

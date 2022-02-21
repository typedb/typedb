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

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class LimitedSortedIterator<T extends Comparable<? super T>, ORDER extends SortedIterator.Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    private final long limit;
    private long counter;
    final ITER iterator;
    T last;


    public LimitedSortedIterator(ITER iterator, long limit) {
        super(iterator.order());
        this.iterator = iterator;
        this.limit = limit;
        this.counter = 0L;
    }

    @Override
    public boolean hasNext() {
        if (counter < limit && iterator.hasNext()) {
            return true;
        } else {
            recycle();
            return false;
        }
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        counter++;
        last = iterator.next();
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


    public static class Seekable<T extends Comparable<? super T>, ORDER extends Order>
            extends LimitedSortedIterator<T, ORDER, SortedIterator.Seekable<T, ORDER>>
            implements SortedIterator.Seekable<T, ORDER> {

        public Seekable(SortedIterator.Seekable<T, ORDER> source, long limit) {
            super(source, limit);
        }

        @Override
        public void seek(T target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            iterator.seek(target);
        }

        @Override
        public final SortedIterator.Seekable<T, ORDER> merge(SortedIterator.Seekable<T, ORDER> iterator) {
            return SortedIterators.Seekable.merge(this, iterator);
        }

        @Override
        public <U extends Comparable<? super U>, ORD extends Order> SortedIterator.Seekable<U, ORD> mapSorted(
                Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order) {
            return SortedIterators.Seekable.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> distinct() {
            return SortedIterators.Seekable.distinct(this);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> filter(Predicate<T> predicate) {
            return SortedIterators.Seekable.filter(this, predicate);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> limit(long limit) {
            return SortedIterators.Seekable.limit(this, limit);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> onConsumed(Runnable function) {
            return SortedIterators.Seekable.onConsume(this, function);
        }

        @Override
        public SortedIterator.Seekable<T, ORDER> onFinalise(Runnable function) {
            return SortedIterators.Seekable.onFinalise(this, function);
        }
    }
}

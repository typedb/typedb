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

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class BasedSortedIterator<T extends Comparable<? super T>, ORDER extends Order>
        extends AbstractSortedIterator<T, ORDER>
        implements SortedIterator.Seekable<T, ORDER> {

    private final NavigableSet<T> source;
    private Iterator<T> iterator;
    private T next;
    private T last;

    public BasedSortedIterator(ORDER order, NavigableSet<T> source) {
        super(order);
        this.source = source;
        this.iterator = order.iterateOrdered(source);
        this.last = null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        if (iterator.hasNext()) {
            next = iterator.next();
            return true;
        } else return false;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        last = next;
        next = null;
        return last;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public void seek(T target) {
        if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        this.iterator = order.iterateOrdered(source, target);
        this.next = null;
    }

    @Override
    public final Seekable<T, ORDER> merge(Seekable<T, ORDER> iterator) {
        return Iterators.Sorted.merge( this, iterator);
    }

    @Override
    public <U extends Comparable<? super U>, ORD extends Order> Seekable<U, ORD> mapSorted(
            ORD order, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
        return Iterators.Sorted.mapSorted(order, this, mappingFn, reverseMappingFn);
    }

    @Override
    public Seekable<T, ORDER> distinct() {
        return Iterators.Sorted.distinct(this);
    }

    @Override
    public Seekable<T, ORDER> filter(Predicate<T> predicate) {
        return Iterators.Sorted.filter(this, predicate);
    }

    @Override
    public void recycle() {
    }
}

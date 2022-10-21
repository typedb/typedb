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

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class BaseForwardableIterator<T extends Comparable<? super T>, ORDER extends Order>
        extends AbstractSortedIterator<T, ORDER>
        implements SortedIterator.Forwardable<T, ORDER> {

    private final NavigableSet<T> source;
    private Iterator<T> iterator;
    private T next;
    private T last;

    public BaseForwardableIterator(NavigableSet<T> source, ORDER order) {
        super(order);
        this.source = source;
        this.iterator = order.orderer().iterate(source);
        this.last = null;
        this.next = null;
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
    public void forward(T target) {
        if (last != null && !order.inOrder(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        if (next != null && order.inOrder(target, next)) return;
        this.iterator = order.orderer().iterate(source, target);
        this.next = null;
    }

    @Override
    public final Forwardable<T, ORDER> merge(Forwardable<T, ORDER> iterator) {
        return SortedIterators.Forwardable.merge(this, iterator);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> intersect(SortedIterator.Forwardable<T, ORDER> iterator) {
        return SortedIterators.Forwardable.intersect(this, iterator);
    }

    @Override
    public <U extends Comparable<? super U>, ORD extends Order> Forwardable<U, ORD> mapSorted(
            Function<T, U> mappingFn, Function<U, T> reverseMappingFn, ORD order) {
        return SortedIterators.Forwardable.mapSorted(order, this, mappingFn, reverseMappingFn);
    }

    @Override
    public Forwardable<T, ORDER> distinct() {
        return SortedIterators.Forwardable.distinct(this);
    }

    @Override
    public Forwardable<T, ORDER> filter(Predicate<T> predicate) {
        return SortedIterators.Forwardable.filter(this, predicate);
    }

    @Override
    public Forwardable<T, ORDER> limit(long limit) {
        return SortedIterators.Forwardable.limit(this, limit);
    }

    @Override
    public SortedIterator.Forwardable<T, ORDER> takeWhile(Function<T, Boolean> condition) {
        return SortedIterators.Forwardable.takeWhile(this, condition);
    }

    @Override
    public Forwardable<T, ORDER> onConsumed(Runnable function) {
        return SortedIterators.Forwardable.onConsume(this, function);
    }

    @Override
    public Forwardable<T, ORDER> onFinalise(Runnable function) {
        return SortedIterators.Forwardable.onFinalise(this, function);
    }

    @Override
    public void recycle() {
    }
}

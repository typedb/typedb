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

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BaseSortedIterator<T extends Comparable<? super T>, ORDER extends Order>
        extends AbstractSortedIterator<T, ORDER> {

    private final Iterator<T> iterator;
    private T next;

    public BaseSortedIterator(List<T> source, ORDER order) {
        super(order);
        assert isSorted(order, source);
        this.iterator = source.iterator();
    }

    private static <T extends Comparable<? super T>, ORD extends Order> boolean isSorted(ORD order, List<T> source) {
        if (source.size() <= 1) return true;
        T last = source.get(0);
        for (int i = 1; i < source.size(); i++) {
            T next = source.get(i);
            if (!order.isValidNext(last, next)) return false;
            last = next;
        }
        return true;
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
        T value = next;
        next = null;
        return value;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public void recycle() {
    }
}

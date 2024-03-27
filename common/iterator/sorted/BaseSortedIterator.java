/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.parameters.Order;

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
            if (!order.inOrder(last, next)) return false;
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

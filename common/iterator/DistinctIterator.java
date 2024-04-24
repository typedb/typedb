/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

// TODO: verify (and potentially fix) this class is able to handle null objects
public class DistinctIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Set<T> consumed;
    private T next;

    DistinctIterator(FunctionalIterator<T> iterator) {
        this(iterator, new HashSet<>());
    }

    public DistinctIterator(FunctionalIterator<T> iterator, Set<T> duplicates) {
        this.iterator = iterator;
        this.consumed = duplicates;
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (iterator.hasNext() && !consumed.add(next = iterator.next())) next = null;
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T result = next;
        next = null;
        return result;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class LinkedIterators<T> extends AbstractFunctionalIterator<T> {

    private final List<FunctionalIterator<T>> iterators;

    public LinkedIterators(List<FunctionalIterator<T>> iterators) {
        this.iterators = new LinkedList<>(iterators);
    }

    @Override
    public final LinkedIterators<T> link(FunctionalIterator<T> iterator) {
        iterators.add(iterator);
        return this;
    }

    @Override
    public boolean hasNext() {
        while (iterators.size() > 1 && !iterators.get(0).hasNext()) iterators.remove(0);
        return !iterators.isEmpty() && iterators.get(0).hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterators.get(0).next();
    }

    @Override
    public void recycle() {
        iterators.forEach(FunctionalIterator::recycle);
    }
}

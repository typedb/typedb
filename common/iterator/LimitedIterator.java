/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;

class LimitedIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final long limit;
    private long counter;

    LimitedIterator(FunctionalIterator<T> iterator, long limit) {
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
        return iterator.next();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

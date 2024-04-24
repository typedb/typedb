/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;

public class OffsetIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final long offset;
    private State state;

    private enum State {INIT, OFFSET}

    public OffsetIterator(FunctionalIterator<T> iterator, long offset) {
        this.iterator = iterator;
        this.offset = offset;
        state = State.INIT;
    }

    @Override
    public boolean hasNext() {
        if (state == State.INIT) offset();
        return iterator.hasNext();
    }

    public void offset() {
        for (int i = 0; i < offset && iterator.hasNext(); i++) iterator.next();
        state = State.OFFSET;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterator.next();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

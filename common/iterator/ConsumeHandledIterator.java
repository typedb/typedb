/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;

class ConsumeHandledIterator<T> extends AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Runnable function;
    private boolean isConsumed;

    ConsumeHandledIterator(FunctionalIterator<T> iterator, Runnable function) {
        this.iterator = iterator;
        this.function = function;
        this.isConsumed = false;
    }

    private void mayHandleConsume(boolean hasNext) {
        if (!hasNext && !isConsumed) {
            isConsumed = true;
            function.run();
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext;
        hasNext = iterator.hasNext();
        mayHandleConsume(hasNext);
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T next = iterator.next();
        mayHandleConsume(iterator.hasNext());
        return next;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

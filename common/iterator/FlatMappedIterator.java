/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;
import java.util.function.Function;

public class FlatMappedIterator<T, U> extends AbstractFunctionalIterator<U> {

    private final FunctionalIterator<T> sourceIterator;
    private final Function<T, FunctionalIterator<U>> mappingFn;
    private FunctionalIterator<U> currentIterator;
    private State state;

    private enum State {INIT, ACTIVE, COMPLETED}

    public FlatMappedIterator(FunctionalIterator<T> iterator, Function<T, FunctionalIterator<U>> mappingFn) {
        this.sourceIterator = iterator;
        this.mappingFn = mappingFn;
        this.state = State.INIT;
    }

    @Override
    public boolean hasNext() {
        if (state == State.COMPLETED) {
            return false;
        } else if (state == State.INIT) {
            initialiseAndSetState();
            if (state == State.COMPLETED) return false;
        }

        return fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (!currentIterator.hasNext() && sourceIterator.hasNext()) {
            currentIterator = mappingFn.apply(sourceIterator.next());
        }
        return currentIterator.hasNext();
    }

    private void initialiseAndSetState() {
        if (!sourceIterator.hasNext()) {
            state = State.COMPLETED;
        } else {
            currentIterator = mappingFn.apply(sourceIterator.next());
            state = State.ACTIVE;
        }
    }

    @Override
    public U next() {
        if (!hasNext()) throw new NoSuchElementException();
        return currentIterator.next();
    }

    @Override
    public void recycle() {
        sourceIterator.recycle();
        if (currentIterator != null) currentIterator.recycle();
    }
}

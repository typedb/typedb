/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

class LoopIterator<T> extends AbstractFunctionalIterator<T> {

    private final Predicate<T> predicate;
    private final UnaryOperator<T> function;
    private T next;
    private State state;

    private enum State {EMPTY, FETCHED, COMPLETED}

    LoopIterator(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        this.next = seed;
        this.predicate = predicate;
        this.function = function;
        if (predicate.test(seed)) state = State.FETCHED; // because first result is 'seed'
        else state = State.COMPLETED;
    }

    private boolean fetchAndCheck() {
        next = function.apply(next);
        if (!predicate.test(next)) {
            state = State.COMPLETED;
            return false;
        }
        state = State.FETCHED;
        return true;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case EMPTY:
                return fetchAndCheck();
            default: // This should never be reached
                return false;
        }
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return next;
    }

    @Override
    public void recycle() {}
}

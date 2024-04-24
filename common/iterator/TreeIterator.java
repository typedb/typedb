/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.function.Function;

class TreeIterator<T> extends AbstractFunctionalIterator<T> {

    private final Function<T, FunctionalIterator<T>> childrenFn;
    private final LinkedList<FunctionalIterator<T>> families;
    // a 'family' is [an iterator for] a collection of children from the same parent

    private T next;
    private State state;

    private enum State {EMPTY, FETCHED, COMPLETED}

    TreeIterator(T root, Function<T, FunctionalIterator<T>> childrenFn) {
        this.next = root;
        this.childrenFn = childrenFn;

        state = State.FETCHED; // because first result is 'root'
        families = new LinkedList<>();
        families.add(childrenFn.apply(next));
    }

    private boolean fetchAndCheck() {
        while (!families.isEmpty() && !families.getFirst().hasNext()) families.removeFirst();
        if (families.isEmpty()) {
            state = State.COMPLETED;
            return false;
        }
        next = families.getFirst().next();
        families.addLast(childrenFn.apply(next));
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
    public void recycle() {
        families.forEach(FunctionalIterator::recycle);
    }
}

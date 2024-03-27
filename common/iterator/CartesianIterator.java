/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.vaticle.typedb.common.collection.Collections.list;

class CartesianIterator<T> extends AbstractFunctionalIterator<List<T>> {

    private final ArrayList<ArrayList<T>> copies;
    private final ArrayList<Boolean> iterated;
    private final ArrayList<FunctionalIterator<T>> iterators;
    private final ArrayList<T> result;
    private State state;

    CartesianIterator(List<FunctionalIterator<T>> iterators) {
        this.iterators = new ArrayList<>(iterators);
        this.result = new ArrayList<>(iterators.size());
        this.copies = new ArrayList<>(iterators.size());
        this.iterated = new ArrayList<>(iterators.size());
        this.state = State.INIT;
    }

    private enum State {INIT, EMPTY, FETCHED, COMPLETED}

    private boolean initialiseAndCheck() {
        if (iterators.isEmpty()) {
            state = State.COMPLETED;
            return false;
        }

        for (FunctionalIterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                T next = iterator.next();
                ArrayList<T> copy = new ArrayList<>();
                copy.add(next);
                copies.add(copy);
                iterated.add(false);
                result.add(next);
            } else {
                result.clear();
                recycle();
                state = State.COMPLETED;
                return false;
            }
        }

        state = State.FETCHED;
        return true;
    }

    private boolean fetchAndCheck() {
        if (tryIncrement(iterators.size() - 1)) state = State.FETCHED;
        else state = State.COMPLETED;

        return state == State.FETCHED;
    }

    private boolean tryIncrement(int i) {
        if (iterators.get(i).hasNext()) {
            T next = iterators.get(i).next();
            if (!iterated.get(i)) copies.get(i).add(next);
            result.set(i, next);
            return true;
        } else if (i == 0) {
            return false;
        } else if (tryIncrement(i - 1)) {
            FunctionalIterator<T> iterator = Iterators.iterate(copies.get(i).iterator());
            iterated.set(i, true);
            iterators.set(i, iterator);
            result.set(i, iterator.next());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void recycle() {
        iterators.forEach(FunctionalIterator::recycle);
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INIT:
                return initialiseAndCheck();
            case EMPTY:
                return fetchAndCheck();
            case FETCHED:
                return true;
            case COMPLETED:
                return false;
            default: // This should never be reached
                return false;
        }
    }

    @Override
    public List<T> next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.EMPTY;
        return list(result);
    }
}

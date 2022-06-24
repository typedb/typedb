/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
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

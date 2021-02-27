/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.iterator;

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

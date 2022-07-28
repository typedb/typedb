/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.NoSuchElementException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class IteratorSortedIterator<T extends Comparable<? super T>, ORDER extends SortedIterator.Order> extends AbstractSortedIterator<T, ORDER> {

    private final FunctionalIterator<T> iterator;
    private State state;
    private T next;
    private T last;

    private enum State {EMPTY, FETCHED, COMPLETED};

    IteratorSortedIterator(ORDER order, FunctionalIterator<T> iterator) {
        super(order);
        this.iterator = iterator;
        this.state = State.EMPTY;
    }

    @Override
    public boolean hasNext() {
        switch(state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case EMPTY:
                return fetchAndCheck();
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private boolean fetchAndCheck() {
        if (!iterator.hasNext()) {
            state = State.COMPLETED;
        } else {
            next = iterator.next();
            assert last == null || order.isValidNext(last, next) : "Iterator values are not ordered.";
            state = State.FETCHED;
        }
        return state == State.FETCHED;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        last = next;
        state = State.EMPTY;
        return next;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

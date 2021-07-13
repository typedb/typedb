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

package com.vaticle.typedb.core.common.iterator;

import java.util.NoSuchElementException;
import java.util.function.Function;

class FlatMappedIterator<T, U> extends AbstractFunctionalIterator<U> {

    private final FunctionalIterator<T> sourceIterator;
    private FunctionalIterator<U> currentIterator;
    private final Function<T, FunctionalIterator<U>> mappingFn;
    private State state;

    private enum State {INIT, ACTIVE, COMPLETED}

    FlatMappedIterator(FunctionalIterator<T> iterator, Function<T, FunctionalIterator<U>> mappingFn) {
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
    }
}

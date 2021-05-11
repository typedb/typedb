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

import org.omg.CORBA.INITIALIZE;

import java.util.function.Function;

public class FlatMergeSortedIterator<T, K extends Comparable<K>> extends AbstractFunctionalIterator.Sorted<T, K> {

    private final FunctionalIterator<T> source;
    private final Function<T, FunctionalIterator.Sorted<T, K>> flatMappingFn;
    private final State state;

    public FlatMergeSortedIterator(FunctionalIterator<T> source, Function<T, FunctionalIterator.Sorted<T, K>> flatMappingFn, Function<T, K> keyExtractor) {
        super(keyExtractor);
        this.source = source;
        this.flatMappingFn = flatMappingFn;
        this.state = State.INIT;
    }

    private enum State {
        INIT, EMPTY, FETCHED, COMPLETED;
    }

    @Override
    public T peek() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    @Override
    public void recycle() {
        source.recycle();
    }
}

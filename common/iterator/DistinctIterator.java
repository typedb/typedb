/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.collection.Either;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

// TODO: verify (and potentially fix) this class to be able to hand null objects
public class DistinctIterator<T> implements ResourceIterator<T> {

    private final Either<Iterators.Recyclable<T>, Iterator<T>> iterator;
    private final Iterator<T> genericIterator;
    private final Set<T> consumed;
    private T next;

    DistinctIterator(Either<Iterators.Recyclable<T>, Iterator<T>> iterator) {
        this.iterator = iterator;
        this.genericIterator = iterator.apply(r -> r, i -> i);
        this.consumed = new HashSet<>();
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (genericIterator.hasNext() && consumed.contains(next = genericIterator.next())) next = null;
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T result = next;
        consumed.add(next);
        next = null;
        return result;
    }

    @Override
    public void recycle() {
        iterator.ifFirst(Iterators.Recyclable::recycle);
    }
}

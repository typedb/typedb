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

import java.util.Iterator;
import java.util.function.Function;

public class AppliedIterator<T, U> implements ResourceIterator<U> {

    private final Either<Iterators.Recyclable<T>, Iterator<T>> iterator;
    private final Iterator<T> genericIterator;
    private final Function<T, U> function;

    AppliedIterator(final Either<Iterators.Recyclable<T>, Iterator<T>> iterator, final Function<T, U> function) {
        this.iterator = iterator;
        this.genericIterator = iterator.apply(r -> r, i -> i);
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return genericIterator.hasNext();
    }

    @Override
    public U next() {
        return function.apply(genericIterator.next());
    }

    @Override
    public void recycle() {
        iterator.ifFirst(Iterators.Recyclable::recycle);
    }
}

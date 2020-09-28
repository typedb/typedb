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
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class LinkedIterators<T> implements ResourceIterator<T> {

    private final List<Either<Iterators.Recyclable<T>, Iterator<T>>> iterators;

    LinkedIterators(LinkedList<Either<Iterators.Recyclable<T>, Iterator<T>>> iterators) {
        this.iterators = iterators;
    }

    private Iterator<T> headIterator() {
        return iterators.get(0).apply(r -> r, i -> i);
    }

    @Override
    public final LinkedIterators<T> link(Iterators.Recyclable<T> iterator) {
        iterators.add(Either.first(iterator));
        return this;
    }

    @Override
    public final LinkedIterators<T> link(Iterator<T> iterator) {
        if (iterator instanceof Iterators.Recyclable<?>) return link((Iterators.Recyclable<T>) iterator);
        iterators.add(Either.second(iterator));
        return this;
    }

    @Override
    public boolean hasNext() {
        while (iterators.size() > 1 && !headIterator().hasNext()) iterators.remove(0);
        return !iterators.isEmpty() && headIterator().hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return headIterator().next();
    }

    @Override
    public void recycle() {
        iterators.forEach(iterator -> iterator.ifFirst(Iterators.Recyclable::recycle));
    }
}

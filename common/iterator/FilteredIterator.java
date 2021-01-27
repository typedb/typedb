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

import java.util.NoSuchElementException;
import java.util.function.Predicate;

class FilteredIterator<T> extends AbstractResourceIterator<T> {

    private final ResourceIterator<T> iterator;
    private final Predicate<T> predicate;
    private T next;

    FilteredIterator(ResourceIterator<T> iterator, Predicate<T> predicate) {
        this.iterator = iterator;
        this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (iterator.hasNext() && !predicate.test(next = iterator.next())) next = null;
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T result = next;
        next = null;
        return result;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

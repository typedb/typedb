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

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.synchronised;

public class SplitIterator<T>
        extends AbstractResourceIterator<ResourceIterator<T>>
        implements ResourceIterator<ResourceIterator<T>> {

    private final SynchronisedIterator<T> sourceIterator;
    private final ResourceIterator<ResourceIterator<T>> splitIterators;

    SplitIterator(ResourceIterator<T> iterator, int count) {
        this.sourceIterator = synchronised(iterator);
        ArrayList<ResourceIterator<T>> iterators = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            iterators.add(new AbstractResourceIterator<T>() {
                T next = null;

                @Override
                public boolean hasNext() {
                    return next != null || (next = sourceIterator.atomicNext()) != null;
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
                    sourceIterator.recycle();
                }
            });
        }
        this.splitIterators = iterate(iterators);
    }

    @Override
    public boolean hasNext() {
        return splitIterators.hasNext();
    }

    @Override
    public ResourceIterator<T> next() {
        return splitIterators.next();
    }

    @Override
    public void recycle() {
        sourceIterator.recycle();
    }
}

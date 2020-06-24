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

package hypergraph.common.iterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class LinkedIterators<T> implements Iterators.Composable<T> {

    private final List<Iterator<T>> iterators;

    LinkedIterators(LinkedList<Iterator<T>> iterators) {
        this.iterators = iterators;
    }

    @Override
    public final LinkedIterators<T> link(Iterator<T> iterator) {
        iterators.add(iterator);
        return this;
    }

    @Override
    public boolean hasNext() {
        while (!iterators.isEmpty() && !iterators.get(0).hasNext() && iterators.size() > 1) {
            iterators.remove(0);
        }
        return !iterators.isEmpty() && iterators.get(0).hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterators.get(0).next();
    }
}

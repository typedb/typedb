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

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

class LinkedIterators<T> extends AbstractFunctionalIterator<T> {

    private final List<FunctionalIterator<T>> iterators;

    LinkedIterators(List<FunctionalIterator<T>> iterators) {
        this.iterators = new LinkedList<>(iterators);
    }

    @Override
    public final LinkedIterators<T> link(FunctionalIterator<T> iterator) {
        iterators.add(iterator);
        return this;
    }

    @Override
    public boolean hasNext() {
        while (iterators.size() > 1 && !iterators.get(0).hasNext()) iterators.remove(0);
        return !iterators.isEmpty() && iterators.get(0).hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterators.get(0).next();
    }

    @Override
    public void recycle() {
        iterators.forEach(FunctionalIterator::recycle);
    }
}

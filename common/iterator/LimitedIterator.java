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

class LimitedIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final long limit;
    private long counter;

    LimitedIterator(FunctionalIterator<T> iterator, long limit) {
        this.iterator = iterator;
        this.limit = limit;
        this.counter = 0L;
    }

    @Override
    public boolean hasNext() {
        if (counter < limit && iterator.hasNext()) {
            return true;
        } else {
            recycle();
            return false;
        }
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        counter++;
        return iterator.next();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

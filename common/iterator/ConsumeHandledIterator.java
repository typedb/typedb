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

class ConsumeHandledIterator<T> extends AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Runnable function;
    private boolean isConsumed;

    ConsumeHandledIterator(FunctionalIterator<T> iterator, Runnable function) {
        this.iterator = iterator;
        this.function = function;
        this.isConsumed = false;
    }

    private void mayHandleConsume(boolean hasNext) {
        if (!hasNext && !isConsumed) {
            isConsumed = true;
            function.run();
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext;
        hasNext = iterator.hasNext();
        mayHandleConsume(hasNext);
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        T next = iterator.next();
        mayHandleConsume(iterator.hasNext());
        return next;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

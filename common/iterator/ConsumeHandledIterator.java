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

public class ConsumeHandledIterator<T> extends AbstractResourceIterator<T> implements ResourceIterator<T> {

    private final ResourceIterator<T> iterator;
    private final Runnable function;
    private boolean isConsumed;

    public ConsumeHandledIterator(ResourceIterator<T> iterator, Runnable function) {
        this.iterator = iterator;
        this.function = function;
        this.isConsumed = false;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext;
        if (!(hasNext = iterator.hasNext()) && !isConsumed) {
            isConsumed = true;
            function.run();
        }
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        return iterator.next();
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

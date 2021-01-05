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

import grakn.core.common.exception.GraknException;

import java.util.function.Function;

public class ErrorHandledIterator<T> implements ResourceIterator<T> {

    private final ResourceIterator<T> iterator;
    private final Function<Exception, GraknException> exceptionFn;

    public ErrorHandledIterator(ResourceIterator<T> iterator, Function<Exception, GraknException> exceptionFn) {
        this.iterator = iterator;
        this.exceptionFn = exceptionFn;
    }

    @Override
    public void recycle() {
        this.iterator.recycle();
    }

    @Override
    public boolean hasNext() {
        try {
            return iterator.hasNext();
        } catch (Exception e) {
            throw this.exceptionFn.apply(e);
        }
    }

    @Override
    public T next() {
        try {
            return this.iterator.next();
        } catch (Exception e) {
            throw this.exceptionFn.apply(e);
        }
    }
}

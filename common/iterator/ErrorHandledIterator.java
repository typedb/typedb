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

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.function.Function;

class ErrorHandledIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Function<Exception, TypeDBException> exceptionFn;

    ErrorHandledIterator(FunctionalIterator<T> iterator, Function<Exception, TypeDBException> exceptionFn) {
        this.iterator = iterator;
        this.exceptionFn = exceptionFn;
    }

    @Override
    public boolean hasNext() {
        try {
            return iterator.hasNext();
        } catch (Exception e) {
            recycle();
            throw exceptionFn.apply(e);
        }
    }

    @Override
    public T next() {
        try {
            return iterator.next();
        } catch (Exception e) {
            recycle();
            throw exceptionFn.apply(e);
        }
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

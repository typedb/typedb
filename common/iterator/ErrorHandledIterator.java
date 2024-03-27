/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.function.Function;

public class ErrorHandledIterator<T> extends AbstractFunctionalIterator<T> {

    private final FunctionalIterator<T> iterator;
    private final Function<Exception, TypeDBException> exceptionFn;

    public ErrorHandledIterator(FunctionalIterator<T> iterator, Function<Exception, TypeDBException> exceptionFn) {
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

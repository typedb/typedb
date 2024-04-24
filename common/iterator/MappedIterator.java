/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import java.util.function.Function;

public class MappedIterator<T, U> extends AbstractFunctionalIterator<U> {

    private final FunctionalIterator<T> iterator;
    private final Function<T, U> function;

    public MappedIterator(FunctionalIterator<T> iterator, Function<T, U> function) {
        this.iterator = iterator;
        this.function = function;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public U next() {
        return function.apply(iterator.next());
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }
}

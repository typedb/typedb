/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.poller;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import java.util.Optional;

public class BasePoller<T> extends AbstractPoller<T> {

    private final FunctionalIterator<T> source;

    public BasePoller(FunctionalIterator<T> iterator) {
        source = iterator;
    }

    @Override
    public Optional<T> poll() {
        if (source.hasNext()) {
            return Optional.of(source.next());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void recycle() {
        source.recycle();
    }
}

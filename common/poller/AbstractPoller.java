/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.poller;

import java.util.Optional;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.list;

public abstract class AbstractPoller<T> implements Poller<T> {

    @Override
    public Optional<T> poll() {
        return Optional.empty();
    }

    @Override
    public <U> Poller<U> flatMap(Function<T, Poller<U>> mappingFn) {
        return new FlatMappedPoller<>(this, mappingFn);
    }

    @Override
    public Poller<T> link(Poller<T> poller) {
        return new LinkedPollers<>(list(this, poller));
    }

}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.poller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class FlatMappedPoller<T, U> extends AbstractPoller<U> {

    private final Poller<T> source;
    private final Function<T, Poller<U>> mappingFn;
    private final List<Poller<U>> mappedPollers;

    FlatMappedPoller(Poller<T> poller, Function<T, Poller<U>> mappingFn) {
        this.source = poller;
        this.mappingFn = mappingFn;
        this.mappedPollers = new ArrayList<>();
    }

    @Override
    public Optional<U> poll() {
        for (Poller<U> poller : mappedPollers) {
            Optional<U> next = poller.poll();
            if (next.isPresent()) return next;
        }
        Optional<T> fromSource;
        while ((fromSource = source.poll()).isPresent()) {
            Poller<U> newPoller = mappingFn.apply(fromSource.get());
            mappedPollers.add(newPoller);
            Optional<U> next = newPoller.poll();
            if (next.isPresent()) return next;
        }
        return Optional.empty();
    }

    @Override
    public void recycle() {
        mappedPollers.forEach(Poller::recycle);
        source.recycle();
    }

}
